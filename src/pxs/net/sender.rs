use super::{
    BLOCK_SIZE, BLOCK_SIZE_USIZE, LargeFileParallelOptions, RemoteSyncOptions,
    codec::PxsCodec,
    path::{
        display_protocol_path, ensure_expected_protocol_path, relative_protocol_path,
        resolve_requested_root,
    },
    protocol::{
        Block, FileMetadata, Message, deserialize_message, serialize_block_batch, serialize_message,
    },
    shared::{
        BlockCompression, TransportFeatures, block_bytes, compress_block_batch_payload,
        connect_with_retry, local_handshake_version, negotiate_transport_features,
        recv_with_timeout, skipped_bytes,
    },
    ssh::{
        ChildSession, build_ssh_chunk_writer_command, build_ssh_command, build_ssh_push_command,
    },
    tasks::{SyncTask, collect_sync_tasks, source_path_for},
};
use crate::pxs::tools;
use anyhow::Result;
use futures_util::SinkExt;
use indicatif::ProgressBar;
use std::{
    collections::HashMap,
    os::unix::ffi::OsStrExt,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
};
use tokio_util::codec::Framed;

const MIN_COMPRESSED_BATCH_BYTES: u64 = 64 * 1024;

#[derive(Clone)]
struct SessionOptionFlags {
    fsync: bool,
    delete: bool,
    path: Option<Vec<u8>>,
}

#[derive(Clone)]
struct SenderLoopOptions {
    threshold: f32,
    checksum: bool,
    session: SessionOptionFlags,
    allow_block_compression: bool,
    large_file_parallel: Option<ParallelSenderOptions>,
    network_file_concurrency: usize,
}

#[derive(Clone)]
enum ParallelWorkerTransport {
    Tcp { addr: String },
    Ssh { addr: String, dst_path: String },
}

#[derive(Clone)]
struct ParallelSenderOptions {
    threshold_bytes: u64,
    worker_count: usize,
    transport: ParallelWorkerTransport,
}

#[derive(Clone)]
struct ServeRequestDefaults {
    threshold: f32,
    checksum: bool,
    ignores: Arc<[String]>,
}

struct PullRequestOptions {
    path: Option<Vec<u8>>,
    threshold: f32,
    checksum: bool,
    delete: bool,
    ignores: Arc<[String]>,
}

/// Run the listener in sender mode (serves files to clients).
///
/// # Errors
///
/// Returns an error if the listener fails to bind or synchronization fails.
pub async fn run_sender_listener(
    addr: &str,
    src_root: &Path,
    threshold: f32,
    checksum: bool,
    ignores: &[String],
) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("Sender listener listening on {addr}");
    let defaults = ServeRequestDefaults {
        threshold,
        checksum,
        ignores: Arc::<[String]>::from(ignores.to_vec()),
    };
    let control_session_gate = Arc::new(Semaphore::new(1));

    while let Ok((stream, peer_addr)) = listener.accept().await {
        tracing::debug!("Accepted connection from {peer_addr}");
        let src_root = src_root.to_path_buf();
        let defaults = defaults.clone();
        let control_session_gate = Arc::clone(&control_session_gate);

        tokio::spawn(async move {
            let mut framed = Framed::new(stream, PxsCodec);
            if let Err(error) =
                serve_client_session(&mut framed, &src_root, defaults, control_session_gate).await
            {
                tracing::error!("Error serving client {peer_addr}: {error}");
            }
            tracing::debug!("Served client {peer_addr}");
        });
    }

    Ok(())
}

fn default_pull_request(defaults: &ServeRequestDefaults) -> PullRequestOptions {
    PullRequestOptions {
        path: None,
        threshold: defaults.threshold,
        checksum: defaults.checksum,
        delete: false,
        ignores: Arc::clone(&defaults.ignores),
    }
}

async fn receive_pull_request<T>(
    framed: &mut Framed<T, PxsCodec>,
    defaults: &ServeRequestDefaults,
) -> Result<PullRequestOptions>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let Some(bytes) = recv_with_timeout(framed).await? else {
        return Ok(default_pull_request(defaults));
    };
    match deserialize_message(&bytes)? {
        Message::PullRequest {
            path,
            threshold,
            checksum,
            delete,
            ignores,
        } => Ok(PullRequestOptions {
            path,
            threshold,
            checksum,
            delete,
            ignores: Arc::<[String]>::from(ignores),
        }),
        Message::Error(message) => anyhow::bail!("pull client error: {message}"),
        other => anyhow::bail!("expected pull request after handshake, got {other:?}"),
    }
}

fn acquire_control_session(control_session_gate: &Arc<Semaphore>) -> Result<OwnedSemaphorePermit> {
    Arc::clone(control_session_gate)
        .try_acquire_owned()
        .map_err(|_| anyhow::anyhow!("another sync session is already active for this root"))
}

async fn serve_client_session<T>(
    framed: &mut Framed<T, PxsCodec>,
    src_root: &Path,
    defaults: ServeRequestDefaults,
    control_session_gate: Arc<Semaphore>,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let features = sender_handshake(framed, true, false).await?;
    let request = receive_pull_request(framed, &defaults).await?;
    let src_root = if let Some(path) = &request.path {
        resolve_requested_root(src_root, path)?
    } else {
        src_root.to_path_buf()
    };
    anyhow::ensure!(
        !request.delete || src_root.is_dir(),
        "--delete is only supported when syncing directories"
    );
    let _control_session = acquire_control_session(&control_session_gate)?;
    let (tasks, total_size) = collect_sync_tasks(&src_root, request.ignores.as_ref()).await?;
    let progress = Arc::new(tools::create_progress_bar(total_size));

    sender_transfer_loop(
        framed,
        &src_root,
        SenderLoopOptions {
            threshold: request.threshold,
            checksum: request.checksum,
            session: SessionOptionFlags {
                fsync: false,
                delete: request.delete,
                path: None,
            },
            allow_block_compression: true,
            large_file_parallel: None,
            network_file_concurrency: 1,
        },
        &tasks,
        progress,
        features,
    )
    .await
}

/// Run the sender to coordinate the client-side sync.
///
/// # Errors
///
/// Returns an error if the connection fails or synchronization fails.
pub async fn run_sender(
    addr: &str,
    src_root: &Path,
    threshold: f32,
    checksum: bool,
    fsync: bool,
    ignores: &[String],
) -> Result<()> {
    run_sender_with_options(addr, src_root, threshold, checksum, fsync, ignores, None).await
}

/// Run the sender to coordinate the client-side sync with optional large-file parallelism.
///
/// # Errors
///
/// Returns an error if the connection fails or synchronization fails.
pub async fn run_sender_with_options(
    addr: &str,
    src_root: &Path,
    threshold: f32,
    checksum: bool,
    fsync: bool,
    ignores: &[String],
    large_file_parallel: Option<LargeFileParallelOptions>,
) -> Result<()> {
    run_sender_with_features(
        addr,
        src_root,
        RemoteSyncOptions {
            path: None,
            threshold,
            features: super::RemoteFeatureOptions {
                checksum,
                delete: false,
                fsync,
            },
            large_file_parallel,
            network_file_concurrency: 1,
            ignores,
        },
    )
    .await
}

/// Run the sender with explicit remote session options such as `delete`.
///
/// # Errors
///
/// Returns an error if the connection fails or synchronization fails.
pub async fn run_sender_with_features(
    addr: &str,
    src_root: &Path,
    options: RemoteSyncOptions<'_>,
) -> Result<()> {
    anyhow::ensure!(
        !options.features.delete || src_root.is_dir(),
        "--delete is only supported when syncing directories"
    );
    let (tasks, total_size) = collect_sync_tasks(src_root, options.ignores).await?;
    let progress = Arc::new(tools::create_progress_bar(total_size));
    let large_file_parallel = options.large_file_parallel.map(
        |LargeFileParallelOptions {
             threshold_bytes,
             worker_count,
         }| ParallelSenderOptions {
            threshold_bytes,
            worker_count,
            transport: ParallelWorkerTransport::Tcp {
                addr: addr.to_string(),
            },
        },
    );
    let stream = connect_with_retry(addr).await?;
    let mut framed = Framed::new(stream, PxsCodec);
    sender_loop(
        &mut framed,
        src_root,
        SenderLoopOptions {
            threshold: options.threshold,
            checksum: options.features.checksum,
            session: SessionOptionFlags {
                fsync: options.features.fsync,
                delete: options.features.delete,
                path: options.path.map(|path| path.as_bytes().to_vec()),
            },
            allow_block_compression: true,
            large_file_parallel,
            network_file_concurrency: options.network_file_concurrency,
        },
        &tasks,
        Arc::clone(&progress),
    )
    .await?;
    progress.finish_with_message("Done");
    Ok(())
}

/// Run the sender using stdin/stdout (for manual piping).
///
/// # Errors
///
/// Returns an error if synchronization fails.
pub async fn run_stdio_sender(
    src_root: &Path,
    threshold: f32,
    checksum: bool,
    delete: bool,
    ignores: &[String],
    quiet: bool,
) -> Result<()> {
    anyhow::ensure!(
        !delete || src_root.is_dir(),
        "--delete is only supported when syncing directories"
    );
    let (tasks, total_size) = collect_sync_tasks(src_root, ignores).await?;
    let progress = Arc::new(if quiet {
        ProgressBar::hidden()
    } else {
        tools::create_progress_bar(total_size)
    });
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    let combined = tokio::io::join(stdin, stdout);
    let mut framed = Framed::new(combined, PxsCodec);
    sender_loop(
        &mut framed,
        src_root,
        SenderLoopOptions {
            threshold,
            checksum,
            session: SessionOptionFlags {
                fsync: false,
                delete,
                path: None,
            },
            allow_block_compression: true,
            large_file_parallel: None,
            network_file_concurrency: 1,
        },
        &tasks,
        progress,
    )
    .await
}

/// Run the sender over an SSH tunnel.
///
/// # Errors
///
/// Returns an error if the SSH command fails or synchronization fails.
pub async fn run_ssh_sender(
    addr: &str,
    src_root: &Path,
    dst_path: &str,
    options: RemoteSyncOptions<'_>,
) -> Result<()> {
    anyhow::ensure!(
        !options.features.delete || src_root.is_dir(),
        "--delete is only supported when syncing directories"
    );
    let (tasks, total_size) = collect_sync_tasks(src_root, options.ignores).await?;
    let progress = Arc::new(tools::create_progress_bar(total_size));
    let remote_cmd = build_ssh_push_command(dst_path, options.features.fsync, options.ignores);
    let mut session = ChildSession::spawn(build_ssh_command(addr, &remote_cmd))?;
    let result = sender_loop(
        session.framed_mut()?,
        src_root,
        SenderLoopOptions {
            threshold: options.threshold,
            checksum: options.features.checksum,
            session: SessionOptionFlags {
                fsync: false,
                delete: options.features.delete,
                path: None,
            },
            allow_block_compression: true,
            large_file_parallel: options.large_file_parallel.map(
                |LargeFileParallelOptions {
                     threshold_bytes,
                     worker_count,
                 }| ParallelSenderOptions {
                    threshold_bytes,
                    worker_count,
                    transport: ParallelWorkerTransport::Ssh {
                        addr: addr.to_string(),
                        dst_path: dst_path.to_string(),
                    },
                },
            ),
            network_file_concurrency: options.network_file_concurrency,
        },
        &tasks,
        Arc::clone(&progress),
    )
    .await;
    session.finish(result, "SSH process").await?;
    progress.finish_with_message("Done");
    Ok(())
}

async fn sender_handshake<T>(
    framed: &mut Framed<T, PxsCodec>,
    allow_block_compression: bool,
    allow_large_file_parallel: bool,
) -> Result<TransportFeatures>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let handshake = Message::Handshake {
        version: local_handshake_version(allow_block_compression, allow_large_file_parallel),
    };
    framed.send(serialize_message(&handshake)?).await?;
    let resp_bytes = recv_with_timeout(framed)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Connection closed during handshake"))?;
    match deserialize_message(&resp_bytes)? {
        Message::Handshake { version } => negotiate_transport_features(
            &version,
            allow_block_compression,
            allow_large_file_parallel,
        ),
        Message::Error(message) => anyhow::bail!("Handshake rejected: {message}"),
        other => anyhow::bail!("Unexpected handshake response: {other:?}"),
    }
}

async fn send_push_session_options<T>(
    framed: &mut Framed<T, PxsCodec>,
    fsync: bool,
    delete: bool,
    path: Option<&[u8]>,
    single_file_name: Option<Vec<u8>>,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    if !fsync && !delete && path.is_none() && single_file_name.is_none() {
        return Ok(());
    }

    framed
        .send(serialize_message(&Message::SessionOptions {
            fsync,
            delete,
            path: path.map(ToOwned::to_owned),
            single_file_name,
        })?)
        .await?;
    Ok(())
}

fn session_single_file_name(src_root: &Path) -> Result<Option<Vec<u8>>> {
    if !src_root.is_file() {
        return Ok(None);
    }

    src_root
        .file_name()
        .map(|name| Some(name.as_bytes().to_vec()))
        .ok_or_else(|| anyhow::anyhow!("source file has no name: {}", src_root.display()))
}

async fn send_parallel_transfer_config<T>(
    framed: &mut Framed<T, PxsCodec>,
    config: Option<ParallelSenderOptions>,
    features: TransportFeatures,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let Some(config) = config else {
        return Ok(());
    };
    if !features.large_file_parallel || config.worker_count <= 1 || config.threshold_bytes == 0 {
        return Ok(());
    }

    framed
        .send(serialize_message(&Message::ParallelTransferConfig {
            threshold_bytes: config.threshold_bytes,
            worker_count: u16::try_from(config.worker_count).map_err(|e| anyhow::anyhow!(e))?,
        })?)
        .await?;
    Ok(())
}

async fn finish_control_connection<T>(framed: &mut Framed<T, PxsCodec>) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    framed
        .send(serialize_message(&Message::SyncComplete)?)
        .await?;

    let response = recv_with_timeout(framed).await?.ok_or_else(|| {
        anyhow::anyhow!("connection closed before sync completion acknowledgment")
    })?;
    match deserialize_message(&response)? {
        Message::SyncCompleteAck => Ok(()),
        Message::Error(message) => anyhow::bail!("Remote error on control connection: {message}"),
        other => anyhow::bail!("Unexpected control response: {other:?}"),
    }
}

async fn sender_loop<T>(
    framed: &mut Framed<T, PxsCodec>,
    src_root: &Path,
    options: SenderLoopOptions,
    tasks: &[SyncTask],
    progress: Arc<ProgressBar>,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let features = sender_handshake(
        framed,
        options.allow_block_compression,
        options.large_file_parallel.is_some(),
    )
    .await?;
    sender_transfer_loop(framed, src_root, options, tasks, progress, features).await
}

async fn sender_transfer_loop<T>(
    framed: &mut Framed<T, PxsCodec>,
    src_root: &Path,
    options: SenderLoopOptions,
    tasks: &[SyncTask],
    progress: Arc<ProgressBar>,
    features: TransportFeatures,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let single_file_name = session_single_file_name(src_root)?;
    send_push_session_options(
        framed,
        options.session.fsync,
        options.session.delete,
        options.session.path.as_deref(),
        single_file_name,
    )
    .await?;
    send_parallel_transfer_config(framed, options.large_file_parallel.clone(), features).await?;

    framed
        .send(serialize_message(&Message::SyncStart {
            total_size: progress.length().unwrap_or(0),
        })?)
        .await?;

    let file_options = RemoteFileSyncOptions {
        threshold: options.threshold,
        checksum: options.checksum,
        progress: Arc::clone(&progress),
        features,
        parallel: options.large_file_parallel.clone(),
    };
    if options.network_file_concurrency <= 1 {
        sender_transfer_tasks_serial(framed, src_root, tasks, &file_options).await?;
    } else {
        sender_transfer_tasks_with_concurrency(
            framed,
            src_root,
            tasks,
            &file_options,
            options.network_file_concurrency,
        )
        .await?;
    }

    finish_control_connection(framed).await
}

async fn send_non_file_sync_task<T>(framed: &mut Framed<T, PxsCodec>, task: &SyncTask) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    match task {
        SyncTask::Dir { path, metadata } => {
            framed
                .send(serialize_message(&Message::SyncDir {
                    path: path.clone(),
                    metadata: *metadata,
                })?)
                .await?;
        }
        SyncTask::Symlink {
            path,
            target,
            metadata,
        } => {
            framed
                .send(serialize_message(&Message::SyncSymlink {
                    path: path.clone(),
                    target: target.clone(),
                    metadata: *metadata,
                })?)
                .await?;
        }
        SyncTask::File { .. } => {}
    }
    Ok(())
}

async fn sender_transfer_tasks_serial<T>(
    framed: &mut Framed<T, PxsCodec>,
    src_root: &Path,
    tasks: &[SyncTask],
    file_options: &RemoteFileSyncOptions,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    for task in tasks {
        match task {
            SyncTask::File { path } => {
                let src_path = source_path_for(src_root, path)?;
                sync_remote_file_with_features(framed, src_root, &src_path, file_options.clone())
                    .await?;
            }
            _ => send_non_file_sync_task(framed, task).await?,
        }
    }
    Ok(())
}

fn remote_file_message_path(message: &Message) -> Option<&[u8]> {
    match message {
        Message::RequestFullCopy { path }
        | Message::RequestHashes { path }
        | Message::MetadataApplied { path }
        | Message::ChecksumVerified { path }
        | Message::ChecksumMismatch { path }
        | Message::EndOfFile { path }
        | Message::RequestParallelFullCopy { path, .. }
        | Message::RequestBlocks { path, .. }
        | Message::RequestParallelBlocks { path, .. } => Some(path.as_slice()),
        _ => None,
    }
}

async fn process_active_remote_file_message<T>(
    framed: &mut Framed<T, PxsCodec>,
    active_files: &mut HashMap<Vec<u8>, ActiveRemoteFile>,
    file_options: &RemoteFileSyncOptions,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let msg_bytes = recv_with_timeout(framed)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Connection closed unexpectedly"))?;
    let msg = deserialize_message(&msg_bytes)?;
    if let Message::Error(error_message) = &msg {
        anyhow::bail!("Remote error: {error_message}");
    }

    let path = remote_file_message_path(&msg)
        .ok_or_else(|| anyhow::anyhow!("Unexpected message: {msg:?}"))?
        .to_vec();
    let mut completed = false;
    {
        let active_file = active_files.get_mut(&path).ok_or_else(|| {
            anyhow::anyhow!(
                "received response for inactive path: {}",
                display_protocol_path(&path)
            )
        })?;
        if let Some(outcome) = handle_remote_file_request_message(
            framed,
            &active_file.context,
            &mut active_file.source_reader,
            &msg,
            file_options,
        )
        .await?
        {
            completed = matches!(outcome, RemoteFileMessageOutcome::Complete);
        } else {
            match handle_remote_file_status_message(
                framed,
                &active_file.context,
                &msg,
                file_options,
            )
            .await?
            {
                Some(RemoteFileMessageOutcome::Complete) => completed = true,
                Some(RemoteFileMessageOutcome::Continue) => {}
                None => anyhow::bail!("Unexpected message: {msg:?}"),
            }
        }
    }
    if completed {
        active_files.remove(&path);
    }
    Ok(())
}

async fn drain_active_remote_files<T>(
    framed: &mut Framed<T, PxsCodec>,
    active_files: &mut HashMap<Vec<u8>, ActiveRemoteFile>,
    file_options: &RemoteFileSyncOptions,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    while !active_files.is_empty() {
        process_active_remote_file_message(framed, active_files, file_options).await?;
    }
    Ok(())
}

async fn sender_transfer_tasks_with_concurrency<T>(
    framed: &mut Framed<T, PxsCodec>,
    src_root: &Path,
    tasks: &[SyncTask],
    file_options: &RemoteFileSyncOptions,
    network_file_concurrency: usize,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut active_files: HashMap<Vec<u8>, ActiveRemoteFile> = HashMap::new();
    let network_file_concurrency = network_file_concurrency.max(1);

    for task in tasks {
        match task {
            SyncTask::File { path } => {
                let src_path = source_path_for(src_root, path)?;
                let context = build_remote_file_context(src_root, &src_path).await?;
                if is_large_file_transfer(&context, file_options.parallel.as_ref()) {
                    drain_active_remote_files(framed, &mut active_files, file_options).await?;
                    sync_remote_file_context_with_features(framed, context, file_options.clone())
                        .await?;
                    continue;
                }

                while active_files.len() >= network_file_concurrency {
                    process_active_remote_file_message(framed, &mut active_files, file_options)
                        .await?;
                }
                file_options
                    .progress
                    .set_message(display_protocol_path(&context.rel_path));
                send_sync_file_message(
                    framed,
                    &context.rel_path,
                    context.metadata,
                    file_options.threshold,
                    file_options.checksum,
                )
                .await?;
                let previous =
                    active_files.insert(context.rel_path.clone(), ActiveRemoteFile::new(context));
                anyhow::ensure!(previous.is_none(), "duplicate in-flight file path");
            }
            _ => send_non_file_sync_task(framed, task).await?,
        }
    }

    drain_active_remote_files(framed, &mut active_files, file_options).await
}

async fn send_sync_file_message<T>(
    framed: &mut Framed<T, PxsCodec>,
    rel_path: &[u8],
    metadata: FileMetadata,
    threshold: f32,
    checksum: bool,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    framed
        .send(serialize_message(&Message::SyncFile {
            path: rel_path.to_vec(),
            metadata,
            threshold,
            checksum,
        })?)
        .await?;
    Ok(())
}

#[derive(Clone)]
struct SourceReadContext {
    file: Arc<std::fs::File>,
    mmap: Option<Arc<memmap2::Mmap>>,
    len: usize,
}

fn mapped_source_chunk(source_reader: &SourceReadContext, offset: u64) -> Result<Option<&[u8]>> {
    let Some(mmap) = source_reader.mmap.as_ref() else {
        return Ok(None);
    };

    let start = usize::try_from(offset).map_err(|e| anyhow::anyhow!(e))?;
    if start >= source_reader.len {
        return Ok(None);
    }

    let end = start
        .saturating_add(BLOCK_SIZE_USIZE)
        .min(source_reader.len);
    let chunk = mmap.get(start..end);
    if chunk.is_some() {
        tools::record_sender_mmap_read_hit();
    }
    Ok(chunk)
}

async fn open_source_read_context(path: &Path) -> Result<Arc<SourceReadContext>> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&path)?;
        let len = usize::try_from(file.metadata()?.len()).map_err(|e| anyhow::anyhow!(e))?;
        let file = Arc::new(file);
        let mmap = tools::safe_mmap(file.as_ref()).ok().map(Arc::new);
        Ok(Arc::new(SourceReadContext { file, mmap, len }))
    })
    .await?
}

async fn ensure_source_read_context(
    source_reader: &mut Option<Arc<SourceReadContext>>,
    path: &Path,
) -> Result<Arc<SourceReadContext>> {
    if let Some(source_reader) = source_reader {
        return Ok(Arc::clone(source_reader));
    }

    let reader = open_source_read_context(path).await?;
    *source_reader = Some(Arc::clone(&reader));
    Ok(reader)
}

async fn read_block_range(
    source_reader: Arc<SourceReadContext>,
    start_block: u64,
    end_block: u64,
) -> Result<Vec<Block>> {
    tokio::task::spawn_blocking(move || {
        let mut blocks = Vec::with_capacity(
            usize::try_from(end_block - start_block).map_err(|e| anyhow::anyhow!(e))?,
        );
        let mut buffer = vec![0_u8; BLOCK_SIZE_USIZE];

        for block_idx in start_block..end_block {
            let offset = block_idx * BLOCK_SIZE;
            let data = if let Some(chunk) = mapped_source_chunk(&source_reader, offset)? {
                chunk.to_vec()
            } else {
                let bytes_read = source_reader.file.read_at(&mut buffer, offset)?;
                let chunk = buffer
                    .get(..bytes_read)
                    .ok_or_else(|| anyhow::anyhow!("chunk exceeds buffer"))?;
                chunk.to_vec()
            };
            blocks.push(Block { offset, data });
        }

        Ok::<Vec<Block>, anyhow::Error>(blocks)
    })
    .await?
}

async fn read_requested_blocks(
    source_reader: Arc<SourceReadContext>,
    indices: Vec<u32>,
) -> Result<Vec<Block>> {
    tokio::task::spawn_blocking(move || {
        let mut blocks = Vec::with_capacity(indices.len());
        let mut buffer = vec![0_u8; BLOCK_SIZE_USIZE];

        for idx in indices {
            let offset = u64::from(idx) * BLOCK_SIZE;
            let data = if let Some(chunk) = mapped_source_chunk(&source_reader, offset)? {
                chunk.to_vec()
            } else {
                let bytes_read = source_reader.file.read_at(&mut buffer, offset)?;
                let chunk = buffer
                    .get(..bytes_read)
                    .ok_or_else(|| anyhow::anyhow!("chunk exceeds buffer"))?;
                chunk.to_vec()
            };
            blocks.push(Block { offset, data });
        }

        Ok::<Vec<Block>, anyhow::Error>(blocks)
    })
    .await?
}

async fn send_block_batch<T>(
    framed: &mut Framed<T, PxsCodec>,
    rel_path: &[u8],
    blocks: Vec<Block>,
    progress: &Arc<ProgressBar>,
    features: TransportFeatures,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let bytes_sent = block_bytes(&blocks)?;
    if features.block_compression != BlockCompression::None
        && bytes_sent >= MIN_COMPRESSED_BATCH_BYTES
    {
        let serialized = serialize_block_batch(&blocks)?;
        if let Some(compressed) =
            compress_block_batch_payload(serialized.as_slice(), features.block_compression)?
        {
            framed
                .send(serialize_message(&Message::ApplyBlocksCompressed {
                    path: rel_path.to_vec(),
                    compressed,
                })?)
                .await?;
            progress.inc(bytes_sent);
            return Ok(());
        }
    }

    framed
        .send(serialize_message(&Message::ApplyBlocks {
            path: rel_path.to_vec(),
            blocks,
        })?)
        .await?;
    progress.inc(bytes_sent);
    Ok(())
}

async fn send_apply_metadata<T>(
    framed: &mut Framed<T, PxsCodec>,
    rel_path: &[u8],
    metadata: FileMetadata,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    framed
        .send(serialize_message(&Message::ApplyMetadata {
            path: rel_path.to_vec(),
            metadata,
        })?)
        .await?;
    Ok(())
}

fn build_parallel_block_ranges(total_blocks: u64, worker_count: usize) -> Vec<(u64, u64)> {
    if total_blocks == 0 || worker_count == 0 {
        return Vec::new();
    }

    let effective_workers = worker_count.min(usize::try_from(total_blocks).unwrap_or(worker_count));
    let base = total_blocks / u64::try_from(effective_workers).unwrap_or(1);
    let remainder = total_blocks % u64::try_from(effective_workers).unwrap_or(1);
    let mut start = 0_u64;
    let mut ranges = Vec::with_capacity(effective_workers);

    for worker_index in 0..effective_workers {
        let extra = u64::from(u64::try_from(worker_index).unwrap_or(u64::MAX) < remainder);
        let end = start + base + extra;
        if start < end {
            ranges.push((start, end));
        }
        start = end;
    }

    ranges
}

fn build_parallel_index_batches(indices: Vec<u32>, worker_count: usize) -> Vec<Vec<u32>> {
    if indices.is_empty() || worker_count == 0 {
        return Vec::new();
    }

    let effective_workers = worker_count.min(indices.len());
    let mut batches = vec![Vec::new(); effective_workers];
    for (index, block_index) in indices.into_iter().enumerate() {
        let batch_index = index % effective_workers;
        if let Some(batch) = batches.get_mut(batch_index) {
            batch.push(block_index);
        }
    }
    batches
        .into_iter()
        .filter(|batch| !batch.is_empty())
        .collect()
}

async fn finish_worker_connection<T>(framed: &mut Framed<T, PxsCodec>) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    framed
        .send(serialize_message(&Message::SyncComplete)?)
        .await?;
    let response = recv_with_timeout(framed)
        .await?
        .ok_or_else(|| anyhow::anyhow!("chunk writer closed before completion acknowledgment"))?;
    match deserialize_message(&response)? {
        Message::SyncCompleteAck => Ok(()),
        Message::Error(message) => anyhow::bail!("chunk writer remote error: {message}"),
        other => anyhow::bail!("unexpected chunk writer response: {other:?}"),
    }
}

async fn send_full_copy_range<T>(
    framed: &mut Framed<T, PxsCodec>,
    rel_path: &[u8],
    source_reader: Arc<SourceReadContext>,
    start_block: u64,
    end_block: u64,
    progress: &Arc<ProgressBar>,
    features: TransportFeatures,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let batch_size = 128_u64;
    let mut start = start_block;
    let mut pending_blocks: Option<Vec<Block>> = None;

    while start < end_block || pending_blocks.is_some() {
        let read_future = if start < end_block {
            let next_end = std::cmp::min(start + batch_size, end_block);
            let next_start = next_end;
            Some((
                read_block_range(Arc::clone(&source_reader), start, next_end),
                next_start,
            ))
        } else {
            None
        };

        if let Some(blocks) = pending_blocks.take() {
            if let Some((read_fut, next_start)) = read_future {
                let (send_result, read_result) = tokio::join!(
                    send_block_batch(framed, rel_path, blocks, progress, features),
                    read_fut
                );
                send_result?;
                pending_blocks = Some(read_result?);
                start = next_start;
            } else {
                send_block_batch(framed, rel_path, blocks, progress, features).await?;
            }
        } else if let Some((read_fut, next_start)) = read_future {
            pending_blocks = Some(read_fut.await?);
            start = next_start;
        }
    }

    Ok(())
}

async fn send_requested_block_batches<T>(
    framed: &mut Framed<T, PxsCodec>,
    rel_path: &[u8],
    source_reader: Arc<SourceReadContext>,
    indices: Vec<u32>,
    progress: &Arc<ProgressBar>,
    features: TransportFeatures,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    for chunk_indices in indices.chunks(128) {
        let blocks =
            read_requested_blocks(Arc::clone(&source_reader), chunk_indices.to_vec()).await?;
        send_block_batch(framed, rel_path, blocks, progress, features).await?;
    }
    Ok(())
}

async fn open_tcp_chunk_writer_connection(
    addr: &str,
    transfer_id: &str,
    rel_path: &[u8],
) -> Result<(Framed<TcpStream, PxsCodec>, TransportFeatures)> {
    let stream = connect_with_retry(addr).await?;
    let mut framed = Framed::new(stream, PxsCodec);
    let features = sender_handshake(&mut framed, true, false).await?;
    framed
        .send(serialize_message(&Message::ChunkWriterStart {
            transfer_id: transfer_id.to_string(),
            path: rel_path.to_vec(),
        })?)
        .await?;
    Ok((framed, features))
}

async fn run_tcp_parallel_full_copy_workers(
    addr: &str,
    rel_path: &[u8],
    transfer_id: &str,
    metadata: FileMetadata,
    progress: &Arc<ProgressBar>,
    source_reader: Arc<SourceReadContext>,
    worker_count: usize,
) -> Result<()> {
    let total_blocks = metadata.size.div_ceil(BLOCK_SIZE);
    let ranges = build_parallel_block_ranges(total_blocks, worker_count);
    let mut workers = Vec::with_capacity(ranges.len());

    for (start_block, end_block) in ranges {
        let addr = addr.to_string();
        let transfer_id = transfer_id.to_string();
        let rel_path = rel_path.to_vec();
        let progress = Arc::clone(progress);
        let source_reader = Arc::clone(&source_reader);

        workers.push(tokio::spawn(async move {
            let (mut framed, features) =
                open_tcp_chunk_writer_connection(&addr, &transfer_id, &rel_path).await?;
            send_full_copy_range(
                &mut framed,
                &rel_path,
                source_reader,
                start_block,
                end_block,
                &progress,
                features,
            )
            .await?;
            finish_worker_connection(&mut framed).await
        }));
    }

    for worker in workers {
        worker.await??;
    }
    Ok(())
}

async fn run_tcp_parallel_block_workers(
    addr: &str,
    rel_path: &[u8],
    transfer_id: &str,
    indices: Vec<u32>,
    progress: &Arc<ProgressBar>,
    source_reader: Arc<SourceReadContext>,
    worker_count: usize,
) -> Result<()> {
    let batches = build_parallel_index_batches(indices, worker_count);
    let mut workers = Vec::with_capacity(batches.len());

    for batch in batches {
        let addr = addr.to_string();
        let transfer_id = transfer_id.to_string();
        let rel_path = rel_path.to_vec();
        let progress = Arc::clone(progress);
        let source_reader = Arc::clone(&source_reader);

        workers.push(tokio::spawn(async move {
            let (mut framed, features) =
                open_tcp_chunk_writer_connection(&addr, &transfer_id, &rel_path).await?;
            send_requested_block_batches(
                &mut framed,
                &rel_path,
                source_reader,
                batch,
                &progress,
                features,
            )
            .await?;
            finish_worker_connection(&mut framed).await
        }));
    }

    for worker in workers {
        worker.await??;
    }
    Ok(())
}

async fn run_ssh_parallel_full_copy_workers(
    options: &ParallelSenderOptions,
    rel_path: &[u8],
    transfer_id: &str,
    metadata: FileMetadata,
    progress: &Arc<ProgressBar>,
    source_reader: Arc<SourceReadContext>,
) -> Result<()> {
    let total_blocks = metadata.size.div_ceil(BLOCK_SIZE);
    let ParallelWorkerTransport::Ssh { addr, dst_path } = &options.transport else {
        anyhow::bail!("parallel SSH worker requested for non-SSH transport");
    };
    let ranges = build_parallel_block_ranges(total_blocks, options.worker_count);
    let mut workers = Vec::with_capacity(ranges.len());

    for (start_block, end_block) in ranges {
        let addr = addr.clone();
        let dst_path = dst_path.clone();
        let transfer_id = transfer_id.to_string();
        let rel_path = rel_path.to_vec();
        let progress = Arc::clone(progress);
        let source_reader = Arc::clone(&source_reader);

        workers.push(tokio::spawn(async move {
            let remote_cmd = build_ssh_chunk_writer_command(&dst_path, &transfer_id);
            let mut session = ChildSession::spawn(build_ssh_command(&addr, &remote_cmd))?;
            let result = async {
                let features = sender_handshake(session.framed_mut()?, true, false).await?;
                send_full_copy_range(
                    session.framed_mut()?,
                    &rel_path,
                    source_reader,
                    start_block,
                    end_block,
                    &progress,
                    features,
                )
                .await?;
                finish_worker_connection(session.framed_mut()?).await
            }
            .await;
            session.finish(result, "SSH chunk writer").await
        }));
    }

    for worker in workers {
        worker.await??;
    }
    Ok(())
}

async fn run_ssh_parallel_block_workers(
    options: &ParallelSenderOptions,
    rel_path: &[u8],
    transfer_id: &str,
    indices: Vec<u32>,
    progress: &Arc<ProgressBar>,
    source_reader: Arc<SourceReadContext>,
) -> Result<()> {
    let ParallelWorkerTransport::Ssh { addr, dst_path } = &options.transport else {
        anyhow::bail!("parallel SSH worker requested for non-SSH transport");
    };
    let batches = build_parallel_index_batches(indices, options.worker_count);
    let mut workers = Vec::with_capacity(batches.len());

    for batch in batches {
        let addr = addr.clone();
        let dst_path = dst_path.clone();
        let transfer_id = transfer_id.to_string();
        let rel_path = rel_path.to_vec();
        let progress = Arc::clone(progress);
        let source_reader = Arc::clone(&source_reader);

        workers.push(tokio::spawn(async move {
            let remote_cmd = build_ssh_chunk_writer_command(&dst_path, &transfer_id);
            let mut session = ChildSession::spawn(build_ssh_command(&addr, &remote_cmd))?;
            let result = async {
                let features = sender_handshake(session.framed_mut()?, true, false).await?;
                send_requested_block_batches(
                    session.framed_mut()?,
                    &rel_path,
                    source_reader,
                    batch,
                    &progress,
                    features,
                )
                .await?;
                finish_worker_connection(session.framed_mut()?).await
            }
            .await;
            session.finish(result, "SSH chunk writer").await
        }));
    }

    for worker in workers {
        worker.await??;
    }
    Ok(())
}

async fn handle_request_full_copy_message<T>(
    framed: &mut Framed<T, PxsCodec>,
    rel_path: &[u8],
    source_reader: Arc<SourceReadContext>,
    metadata: FileMetadata,
    progress: &Arc<ProgressBar>,
    features: TransportFeatures,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let num_blocks = metadata.size.div_ceil(BLOCK_SIZE);
    let batch_size = 128_u64;
    let mut start_block = 0_u64;

    // Pipeline: read next batch while sending current batch
    let mut pending_blocks: Option<Vec<Block>> = None;

    while start_block < num_blocks || pending_blocks.is_some() {
        let read_future = if start_block < num_blocks {
            let end_block = std::cmp::min(start_block + batch_size, num_blocks);
            let next_start = end_block;
            Some((
                read_block_range(Arc::clone(&source_reader), start_block, end_block),
                next_start,
            ))
        } else {
            None
        };

        if let Some(blocks) = pending_blocks.take() {
            if let Some((read_fut, next_start)) = read_future {
                // Pipeline: send current batch while reading next
                let (send_result, read_result) = tokio::join!(
                    send_block_batch(framed, rel_path, blocks, progress, features),
                    read_fut
                );
                send_result?;
                pending_blocks = Some(read_result?);
                start_block = next_start;
            } else {
                // Last batch, just send
                send_block_batch(framed, rel_path, blocks, progress, features).await?;
            }
        } else if let Some((read_fut, next_start)) = read_future {
            // First iteration, just read
            pending_blocks = Some(read_fut.await?);
            start_block = next_start;
        }
    }

    send_apply_metadata(framed, rel_path, metadata).await
}

async fn handle_request_hashes_message<T>(
    framed: &mut Framed<T, PxsCodec>,
    rel_path: &[u8],
    source_reader: Arc<SourceReadContext>,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let hashes = tokio::task::spawn_blocking(move || {
        tools::calculate_file_hashes_for_open_file(
            source_reader.file.as_ref(),
            BLOCK_SIZE,
            source_reader.mmap.as_deref(),
        )
    })
    .await??;
    framed
        .send(serialize_message(&Message::BlockHashes {
            path: rel_path.to_vec(),
            hashes,
        })?)
        .await?;
    Ok(())
}

async fn handle_request_blocks_message<T>(
    framed: &mut Framed<T, PxsCodec>,
    rel_path: &[u8],
    source_reader: Arc<SourceReadContext>,
    metadata: FileMetadata,
    indices: Vec<u32>,
    progress: &Arc<ProgressBar>,
    features: TransportFeatures,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let requested_count = u64::try_from(indices.len()).map_err(|e| anyhow::anyhow!(e))?;
    let total_blocks = metadata.size.div_ceil(BLOCK_SIZE);
    if total_blocks > requested_count {
        progress.inc(skipped_bytes(metadata, &indices));
    }

    for chunk_indices in indices.chunks(128) {
        let blocks =
            read_requested_blocks(Arc::clone(&source_reader), chunk_indices.to_vec()).await?;
        send_block_batch(framed, rel_path, blocks, progress, features).await?;
    }

    send_apply_metadata(framed, rel_path, metadata).await
}

#[derive(Clone)]
struct RemoteFileSyncOptions {
    threshold: f32,
    checksum: bool,
    progress: Arc<ProgressBar>,
    features: TransportFeatures,
    parallel: Option<ParallelSenderOptions>,
}

struct RemoteFileContext {
    rel_path: Vec<u8>,
    path: PathBuf,
    metadata: FileMetadata,
}

impl RemoteFileContext {
    fn ensure_expected_path(&self, received_path: &[u8]) -> Result<()> {
        ensure_expected_protocol_path(&self.rel_path, received_path)
    }
}

struct ActiveRemoteFile {
    context: RemoteFileContext,
    source_reader: Option<Arc<SourceReadContext>>,
}

impl ActiveRemoteFile {
    fn new(context: RemoteFileContext) -> Self {
        Self {
            context,
            source_reader: None,
        }
    }
}

async fn build_remote_file_context(src_root: &Path, path: &Path) -> Result<RemoteFileContext> {
    Ok(RemoteFileContext {
        rel_path: relative_protocol_path(src_root, path)?,
        path: path.to_path_buf(),
        metadata: FileMetadata::from(tokio::fs::metadata(path).await?),
    })
}

fn is_large_file_transfer(
    context: &RemoteFileContext,
    parallel_options: Option<&ParallelSenderOptions>,
) -> bool {
    parallel_options.is_some_and(|parallel| {
        parallel.threshold_bytes > 0 && context.metadata.size >= parallel.threshold_bytes
    })
}

fn ensure_parallel_options(
    parallel_options: Option<ParallelSenderOptions>,
    transfer_kind: &str,
) -> Result<ParallelSenderOptions> {
    parallel_options.ok_or_else(|| {
        anyhow::anyhow!(
            "remote requested parallel {transfer_kind} transfer but large-file parallelism is not configured"
        )
    })
}

async fn handle_parallel_full_copy_request<T>(
    framed: &mut Framed<T, PxsCodec>,
    context: &RemoteFileContext,
    source_reader: &mut Option<Arc<SourceReadContext>>,
    progress: &Arc<ProgressBar>,
    parallel_options: Option<ParallelSenderOptions>,
    transfer_id: String,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let source_reader = ensure_source_read_context(source_reader, &context.path).await?;
    let parallel_options = ensure_parallel_options(parallel_options, "full-copy")?;
    match &parallel_options.transport {
        ParallelWorkerTransport::Tcp { addr } => {
            run_tcp_parallel_full_copy_workers(
                addr,
                &context.rel_path,
                &transfer_id,
                context.metadata,
                progress,
                source_reader,
                parallel_options.worker_count,
            )
            .await?;
        }
        ParallelWorkerTransport::Ssh { .. } => {
            run_ssh_parallel_full_copy_workers(
                &parallel_options,
                &context.rel_path,
                &transfer_id,
                context.metadata,
                progress,
                source_reader,
            )
            .await?;
        }
    }
    send_apply_metadata(framed, &context.rel_path, context.metadata).await
}

async fn handle_parallel_block_request<T>(
    framed: &mut Framed<T, PxsCodec>,
    context: &RemoteFileContext,
    source_reader: &mut Option<Arc<SourceReadContext>>,
    progress: &Arc<ProgressBar>,
    parallel_options: Option<ParallelSenderOptions>,
    transfer_id: String,
    indices: Vec<u32>,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let source_reader = ensure_source_read_context(source_reader, &context.path).await?;
    let parallel_options = ensure_parallel_options(parallel_options, "block")?;
    match &parallel_options.transport {
        ParallelWorkerTransport::Tcp { addr } => {
            run_tcp_parallel_block_workers(
                addr,
                &context.rel_path,
                &transfer_id,
                indices,
                progress,
                source_reader,
                parallel_options.worker_count,
            )
            .await?;
        }
        ParallelWorkerTransport::Ssh { .. } => {
            run_ssh_parallel_block_workers(
                &parallel_options,
                &context.rel_path,
                &transfer_id,
                indices,
                progress,
                source_reader,
            )
            .await?;
        }
    }
    send_apply_metadata(framed, &context.rel_path, context.metadata).await
}

async fn handle_request_full_copy_with_context<T>(
    framed: &mut Framed<T, PxsCodec>,
    context: &RemoteFileContext,
    source_reader: &mut Option<Arc<SourceReadContext>>,
    progress: &Arc<ProgressBar>,
    features: TransportFeatures,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let source_reader = ensure_source_read_context(source_reader, &context.path).await?;
    handle_request_full_copy_message(
        framed,
        &context.rel_path,
        source_reader,
        context.metadata,
        progress,
        features,
    )
    .await
}

async fn handle_request_hashes_with_context<T>(
    framed: &mut Framed<T, PxsCodec>,
    context: &RemoteFileContext,
    source_reader: &mut Option<Arc<SourceReadContext>>,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let source_reader = ensure_source_read_context(source_reader, &context.path).await?;
    handle_request_hashes_message(framed, &context.rel_path, source_reader).await
}

async fn handle_request_blocks_with_context<T>(
    framed: &mut Framed<T, PxsCodec>,
    context: &RemoteFileContext,
    source_reader: &mut Option<Arc<SourceReadContext>>,
    indices: Vec<u32>,
    progress: &Arc<ProgressBar>,
    features: TransportFeatures,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let source_reader = ensure_source_read_context(source_reader, &context.path).await?;
    handle_request_blocks_message(
        framed,
        &context.rel_path,
        source_reader,
        context.metadata,
        indices,
        progress,
        features,
    )
    .await
}

async fn handle_metadata_applied_with_context<T>(
    framed: &mut Framed<T, PxsCodec>,
    context: &RemoteFileContext,
    checksum: bool,
) -> Result<bool>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    if !checksum {
        return Ok(true);
    }

    let hash = tools::blake3_file_hash(&context.path).await?;
    framed
        .send(serialize_message(&Message::VerifyChecksum {
            path: context.rel_path.clone(),
            hash,
        })?)
        .await?;
    Ok(false)
}

async fn handle_end_of_file_with_context<T>(
    framed: &mut Framed<T, PxsCodec>,
    context: &RemoteFileContext,
    progress: &Arc<ProgressBar>,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    progress.inc(context.metadata.size);
    send_apply_metadata(framed, &context.rel_path, context.metadata).await
}

enum RemoteFileMessageOutcome {
    Continue,
    Complete,
}

async fn handle_remote_file_request_message<T>(
    framed: &mut Framed<T, PxsCodec>,
    context: &RemoteFileContext,
    source_reader: &mut Option<Arc<SourceReadContext>>,
    message: &Message,
    options: &RemoteFileSyncOptions,
) -> Result<Option<RemoteFileMessageOutcome>>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    match message {
        Message::RequestFullCopy {
            path: received_path,
        } => {
            context.ensure_expected_path(received_path)?;
            handle_request_full_copy_with_context(
                framed,
                context,
                source_reader,
                &options.progress,
                options.features,
            )
            .await?;
        }
        Message::RequestHashes {
            path: received_path,
        } => {
            context.ensure_expected_path(received_path)?;
            handle_request_hashes_with_context(framed, context, source_reader).await?;
        }
        Message::RequestParallelFullCopy {
            path: received_path,
            transfer_id,
        } => {
            context.ensure_expected_path(received_path)?;
            handle_parallel_full_copy_request(
                framed,
                context,
                source_reader,
                &options.progress,
                options.parallel.clone(),
                transfer_id.clone(),
            )
            .await?;
        }
        Message::RequestBlocks {
            path: received_path,
            indices,
        } => {
            context.ensure_expected_path(received_path)?;
            handle_request_blocks_with_context(
                framed,
                context,
                source_reader,
                indices.clone(),
                &options.progress,
                options.features,
            )
            .await?;
        }
        Message::RequestParallelBlocks {
            path: received_path,
            transfer_id,
            indices,
        } => {
            context.ensure_expected_path(received_path)?;
            handle_parallel_block_request(
                framed,
                context,
                source_reader,
                &options.progress,
                options.parallel.clone(),
                transfer_id.clone(),
                indices.clone(),
            )
            .await?;
        }
        _ => return Ok(None),
    }

    Ok(Some(RemoteFileMessageOutcome::Continue))
}

async fn handle_remote_file_status_message<T>(
    framed: &mut Framed<T, PxsCodec>,
    context: &RemoteFileContext,
    message: &Message,
    options: &RemoteFileSyncOptions,
) -> Result<Option<RemoteFileMessageOutcome>>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    match message {
        Message::MetadataApplied {
            path: received_path,
        } => {
            context.ensure_expected_path(received_path)?;
            let done =
                handle_metadata_applied_with_context(framed, context, options.checksum).await?;
            Ok(Some(if done {
                RemoteFileMessageOutcome::Complete
            } else {
                RemoteFileMessageOutcome::Continue
            }))
        }
        Message::ChecksumVerified {
            path: received_path,
        } => {
            context.ensure_expected_path(received_path)?;
            Ok(Some(RemoteFileMessageOutcome::Complete))
        }
        Message::ChecksumMismatch {
            path: received_path,
        } => {
            context.ensure_expected_path(received_path)?;
            anyhow::bail!(
                "Checksum mismatch for {}: destination file differs from source after transfer",
                display_protocol_path(&context.rel_path)
            );
        }
        Message::EndOfFile {
            path: received_path,
        } => {
            context.ensure_expected_path(received_path)?;
            handle_end_of_file_with_context(framed, context, &options.progress).await?;
            Ok(Some(RemoteFileMessageOutcome::Continue))
        }
        Message::Error(error_message) => {
            anyhow::bail!(
                "Remote error for {}: {error_message}",
                display_protocol_path(&context.rel_path)
            );
        }
        _ => Ok(None),
    }
}

/// Sync a remote file.
///
/// # Errors
///
/// Returns an error if synchronization fails.
pub async fn sync_remote_file<T>(
    framed: &mut Framed<T, PxsCodec>,
    src_root: &Path,
    path: &Path,
    threshold: f32,
    checksum: bool,
    progress: Arc<ProgressBar>,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    sync_remote_file_with_features(
        framed,
        src_root,
        path,
        RemoteFileSyncOptions {
            threshold,
            checksum,
            progress,
            features: TransportFeatures::default(),
            parallel: None,
        },
    )
    .await
}

async fn sync_remote_file_with_features<T>(
    framed: &mut Framed<T, PxsCodec>,
    src_root: &Path,
    path: &Path,
    options: RemoteFileSyncOptions,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let context = build_remote_file_context(src_root, path).await?;
    sync_remote_file_context_with_features(framed, context, options).await
}

async fn sync_remote_file_context_with_features<T>(
    framed: &mut Framed<T, PxsCodec>,
    context: RemoteFileContext,
    options: RemoteFileSyncOptions,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut source_reader: Option<Arc<SourceReadContext>> = None;
    options
        .progress
        .set_message(display_protocol_path(&context.rel_path));
    send_sync_file_message(
        framed,
        &context.rel_path,
        context.metadata,
        options.threshold,
        options.checksum,
    )
    .await?;

    while let Some(msg_bytes) = recv_with_timeout(framed).await? {
        let msg = deserialize_message(&msg_bytes)?;
        if let Some(outcome) =
            handle_remote_file_request_message(framed, &context, &mut source_reader, &msg, &options)
                .await?
        {
            if matches!(outcome, RemoteFileMessageOutcome::Complete) {
                return Ok(());
            }
            continue;
        }

        match handle_remote_file_status_message(framed, &context, &msg, &options).await? {
            Some(RemoteFileMessageOutcome::Complete) => return Ok(()),
            Some(RemoteFileMessageOutcome::Continue) => {}
            None => anyhow::bail!("Unexpected message: {msg:?}"),
        }
    }

    anyhow::bail!("Connection closed unexpectedly")
}

#[cfg(test)]
mod tests {
    use super::{MIN_COMPRESSED_BATCH_BYTES, send_block_batch, sender_handshake};
    use crate::pxs::net::protocol::deserialize_block_batch;
    use crate::pxs::net::shared::{
        BlockCompression, TransportFeatures, decompress_block_batch_payload,
    };
    use crate::pxs::net::{Block, Message, PxsCodec, deserialize_message, serialize_message};
    use anyhow::Result;
    use futures_util::{SinkExt, StreamExt};
    use indicatif::ProgressBar;
    use std::sync::Arc;
    use tokio_util::codec::Framed;

    #[tokio::test]
    async fn test_sender_handshake_prefers_zstd_with_capable_peer() -> Result<()> {
        let (client, server) = tokio::io::duplex(4096);
        let mut sender_framed = Framed::new(client, PxsCodec);
        let mut peer_framed = Framed::new(server, PxsCodec);

        let sender_task =
            tokio::spawn(async move { sender_handshake(&mut sender_framed, true, false).await });

        let handshake = peer_framed
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("missing sender handshake"))??;
        match deserialize_message(&handshake)? {
            Message::Handshake { version } => {
                assert!(version.contains("lz4-blocks"));
                assert!(version.contains("zstd-blocks"));
            }
            other => anyhow::bail!("expected handshake, got {other:?}"),
        }

        peer_framed
            .send(serialize_message(&Message::Handshake {
                version: format!("{}+caps=zstd-blocks,lz4-blocks", env!("CARGO_PKG_VERSION")),
            })?)
            .await?;

        let features = sender_task.await??;
        assert_eq!(features.block_compression, BlockCompression::Zstd);
        Ok(())
    }

    #[tokio::test]
    async fn test_send_block_batch_uses_zstd_message_when_beneficial() -> Result<()> {
        let (client, server) = tokio::io::duplex(2 * 1024 * 1024);
        let mut sender_framed = Framed::new(client, PxsCodec);
        let mut receiver_framed = Framed::new(server, PxsCodec);
        let progress = Arc::new(ProgressBar::hidden());
        let blocks = vec![Block {
            offset: 0,
            data: vec![
                b'A';
                usize::try_from(MIN_COMPRESSED_BATCH_BYTES)
                    .map_err(|e| anyhow::anyhow!(e))?
            ],
        }];

        send_block_batch(
            &mut sender_framed,
            b"file.bin",
            blocks,
            &progress,
            TransportFeatures {
                block_compression: BlockCompression::Zstd,
                large_file_parallel: false,
            },
        )
        .await?;

        let msg = receiver_framed
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("missing block batch"))??;
        match deserialize_message(&msg)? {
            Message::ApplyBlocksCompressed { path, compressed } => {
                assert_eq!(path, b"file.bin");
                let payload = decompress_block_batch_payload(&compressed, BlockCompression::Zstd)?;
                let blocks = deserialize_block_batch(&payload)?;
                assert_eq!(blocks.len(), 1);
                assert_eq!(
                    blocks
                        .first()
                        .ok_or_else(|| anyhow::anyhow!("missing decoded block"))?
                        .offset,
                    0
                );
            }
            other => anyhow::bail!("expected compressed blocks, got {other:?}"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_send_block_batch_falls_back_without_compression_negotiation() -> Result<()> {
        let (client, server) = tokio::io::duplex(2 * 1024 * 1024);
        let mut sender_framed = Framed::new(client, PxsCodec);
        let mut receiver_framed = Framed::new(server, PxsCodec);
        let progress = Arc::new(ProgressBar::hidden());
        let blocks = vec![Block {
            offset: 0,
            data: vec![
                b'A';
                usize::try_from(MIN_COMPRESSED_BATCH_BYTES)
                    .map_err(|e| anyhow::anyhow!(e))?
            ],
        }];

        send_block_batch(
            &mut sender_framed,
            b"file.bin",
            blocks,
            &progress,
            TransportFeatures::default(),
        )
        .await?;

        let msg = receiver_framed
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("missing block batch"))??;
        match deserialize_message(&msg)? {
            Message::ApplyBlocks { path, blocks } => {
                assert_eq!(path, b"file.bin");
                assert_eq!(blocks.len(), 1);
            }
            other => anyhow::bail!("expected uncompressed blocks, got {other:?}"),
        }

        Ok(())
    }
}
