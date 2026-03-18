use super::{
    BLOCK_SIZE, BLOCK_SIZE_USIZE,
    codec::PxsCodec,
    path::{ensure_expected_protocol_path, relative_protocol_path},
    protocol::{Block, FileMetadata, Message, deserialize_message, serialize_message},
    shared::{
        block_bytes, connect_with_retry, recv_with_timeout, skipped_bytes, validate_peer_version,
    },
    tasks::{SyncTask, collect_sync_tasks, source_path_for},
};
use crate::pxs::tools;
use futures_util::SinkExt;
use indicatif::ProgressBar;
use std::{fmt::Write as _, os::unix::fs::FileExt, path::Path, process::Stdio, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    process::Command,
};
use tokio_util::codec::Framed;

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
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    eprintln!("Sender listener listening on {addr}");
    let ignores = Arc::<[String]>::from(ignores.to_vec());

    while let Ok((stream, peer_addr)) = listener.accept().await {
        eprintln!("Accepted connection from {peer_addr}");
        let src_root = src_root.to_path_buf();
        let ignores = Arc::clone(&ignores);

        tokio::spawn(async move {
            let (tasks, total_size) = match collect_sync_tasks(&src_root, ignores.as_ref()).await {
                Ok(sync_plan) => sync_plan,
                Err(error) => {
                    eprintln!("Error collecting sync tasks for client {peer_addr}: {error}");
                    return;
                }
            };
            let pb = Arc::new(tools::create_progress_bar(total_size));
            let mut framed = Framed::new(stream, PxsCodec);
            if let Err(error) =
                sender_loop(&mut framed, &src_root, threshold, checksum, &tasks, pb).await
            {
                eprintln!("Error serving client {peer_addr}: {error}");
            }
            eprintln!("Served client {peer_addr}");
        });
    }

    Ok(())
}

async fn send_non_file_tasks<T>(
    framed: &mut Framed<T, PxsCodec>,
    tasks: &[SyncTask],
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    for task in tasks {
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
    }

    framed.flush().await?;
    Ok(())
}

fn collect_file_paths(tasks: &[SyncTask]) -> Vec<String> {
    tasks
        .iter()
        .filter_map(|task| match task {
            SyncTask::File { path } => Some(path.clone()),
            _ => None,
        })
        .collect()
}

fn build_worker_batches(
    file_paths: Vec<String>,
    worker_count: usize,
) -> anyhow::Result<Vec<Vec<String>>> {
    let mut batches = vec![Vec::new(); worker_count];
    for (index, rel_path) in file_paths.into_iter().enumerate() {
        let batch_index = index % worker_count;
        let batch = batches
            .get_mut(batch_index)
            .ok_or_else(|| anyhow::anyhow!("missing worker batch {batch_index}"))?;
        batch.push(rel_path);
    }
    Ok(batches)
}

async fn run_sender_workers(
    addr: &str,
    src_root: &Path,
    threshold: f32,
    checksum: bool,
    progress: &Arc<ProgressBar>,
    batches: Vec<Vec<String>>,
) -> anyhow::Result<()> {
    let mut workers = Vec::with_capacity(batches.len());
    for batch in batches {
        let addr_owned = addr.to_string();
        let src_root_worker = src_root.to_path_buf();
        let progress_worker = Arc::clone(progress);

        workers.push(tokio::spawn(async move {
            let stream = connect_with_retry(&addr_owned).await?;
            let mut framed = Framed::new(stream, PxsCodec);
            sender_handshake(&mut framed).await?;

            for rel_path in batch {
                let src_path = source_path_for(&src_root_worker, &rel_path);
                sync_remote_file(
                    &mut framed,
                    &src_root_worker,
                    &src_path,
                    threshold,
                    checksum,
                    Arc::clone(&progress_worker),
                )
                .await?;
            }

            Ok::<(), anyhow::Error>(())
        }));
    }

    for worker in workers {
        worker.await??;
    }

    Ok(())
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
    ignores: &[String],
) -> anyhow::Result<()> {
    let (tasks, total_size) = collect_sync_tasks(src_root, ignores).await?;
    let progress = Arc::new(tools::create_progress_bar(total_size));

    let has_non_file_tasks = tasks
        .iter()
        .any(|task| !matches!(task, SyncTask::File { .. }));

    let main_framed = if has_non_file_tasks {
        let stream = connect_with_retry(addr).await?;
        let mut framed = Framed::new(stream, PxsCodec);
        sender_handshake(&mut framed).await?;
        send_non_file_tasks(&mut framed, &tasks).await?;
        Some(framed)
    } else {
        None::<Framed<TcpStream, PxsCodec>>
    };

    let file_paths = collect_file_paths(&tasks);

    if file_paths.is_empty() {
        if let Some(mut framed) = main_framed {
            let _ = framed.flush().await;
        }
        progress.finish_with_message("Done");
        return Ok(());
    }

    let worker_count = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(8)
        .min(64)
        .min(file_paths.len());
    let batches = build_worker_batches(file_paths, worker_count)?;
    run_sender_workers(addr, src_root, threshold, checksum, &progress, batches).await?;

    if let Some(mut framed) = main_framed {
        let _ = framed.flush().await;
    }

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
    ignores: &[String],
) -> anyhow::Result<()> {
    let (tasks, total_size) = collect_sync_tasks(src_root, ignores).await?;
    let progress = Arc::new(tools::create_progress_bar(total_size));
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    let combined = tokio::io::join(stdin, stdout);
    let mut framed = Framed::new(combined, PxsCodec);
    sender_loop(&mut framed, src_root, threshold, checksum, &tasks, progress).await
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
    threshold: f32,
    checksum: bool,
    ignores: &[String],
) -> anyhow::Result<()> {
    let (tasks, total_size) = collect_sync_tasks(src_root, ignores).await?;
    let progress = Arc::new(tools::create_progress_bar(total_size));
    let mut cmd = Command::new("ssh");
    cmd.arg("-q")
        .arg("-o")
        .arg("Compression=no")
        .arg("-o")
        .arg("Ciphers=aes128-gcm@openssh.com,chacha20-poly1305@openssh.com,aes128-ctr")
        .arg("-o")
        .arg("IPQoS=throughput")
        .arg(addr);
    let mut remote_cmd = format!("pxs --stdio --destination {dst_path}");
    for pattern in ignores {
        let _ = write!(remote_cmd, " --ignore '{pattern}'");
    }
    let mut child = cmd
        .arg(remote_cmd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow::anyhow!("stdin failed"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("stdout failed"))?;
    {
        let combined = tokio::io::join(stdout, stdin);
        let mut framed = Framed::new(combined, PxsCodec);
        sender_loop(
            &mut framed,
            src_root,
            threshold,
            checksum,
            &tasks,
            Arc::clone(&progress),
        )
        .await?;
    }
    let status = child.wait().await?;
    if !status.success() {
        anyhow::bail!("SSH process exited with error: {status}");
    }
    progress.finish_with_message("Done");
    Ok(())
}

async fn sender_handshake<T>(framed: &mut Framed<T, PxsCodec>) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let handshake = Message::Handshake {
        version: env!("CARGO_PKG_VERSION").to_string(),
    };
    framed.send(serialize_message(&handshake)?).await?;
    let resp_bytes = recv_with_timeout(framed)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Connection closed during handshake"))?;
    match deserialize_message(&resp_bytes)? {
        Message::Handshake { version } => validate_peer_version(&version)?,
        Message::Error(message) => anyhow::bail!("Handshake rejected: {message}"),
        other => anyhow::bail!("Unexpected handshake response: {other:?}"),
    }
    Ok(())
}

async fn sender_loop<T>(
    framed: &mut Framed<T, PxsCodec>,
    src_root: &Path,
    threshold: f32,
    checksum: bool,
    tasks: &[SyncTask],
    progress: Arc<ProgressBar>,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    sender_handshake(framed).await?;

    framed
        .send(serialize_message(&Message::SyncStart {
            total_size: progress.length().unwrap_or(0),
        })?)
        .await?;

    for task in tasks {
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
            SyncTask::File { path } => {
                let src_path = source_path_for(src_root, path);
                sync_remote_file(
                    framed,
                    src_root,
                    &src_path,
                    threshold,
                    checksum,
                    Arc::clone(&progress),
                )
                .await?;
            }
        }
    }

    Ok(())
}

async fn send_sync_file_message<T>(
    framed: &mut Framed<T, PxsCodec>,
    rel_path: &str,
    metadata: FileMetadata,
    threshold: f32,
    checksum: bool,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    framed
        .send(serialize_message(&Message::SyncFile {
            path: rel_path.to_string(),
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

fn mapped_source_chunk(
    source_reader: &SourceReadContext,
    offset: u64,
) -> anyhow::Result<Option<&[u8]>> {
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
    Ok(mmap.get(start..end))
}

async fn open_source_read_context(path: &Path) -> anyhow::Result<Arc<SourceReadContext>> {
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
) -> anyhow::Result<Arc<SourceReadContext>> {
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
) -> anyhow::Result<Vec<Block>> {
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
) -> anyhow::Result<Vec<Block>> {
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
    rel_path: &str,
    blocks: Vec<Block>,
    progress: &Arc<ProgressBar>,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let bytes_sent = block_bytes(&blocks)?;
    framed
        .send(serialize_message(&Message::ApplyBlocks {
            path: rel_path.to_string(),
            blocks,
        })?)
        .await?;
    progress.inc(bytes_sent);
    Ok(())
}

async fn send_apply_metadata<T>(
    framed: &mut Framed<T, PxsCodec>,
    rel_path: &str,
    metadata: FileMetadata,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    framed
        .send(serialize_message(&Message::ApplyMetadata {
            path: rel_path.to_string(),
            metadata,
        })?)
        .await?;
    Ok(())
}

async fn handle_request_full_copy_message<T>(
    framed: &mut Framed<T, PxsCodec>,
    rel_path: &str,
    source_reader: Arc<SourceReadContext>,
    metadata: FileMetadata,
    progress: &Arc<ProgressBar>,
) -> anyhow::Result<()>
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
                    send_block_batch(framed, rel_path, blocks, progress),
                    read_fut
                );
                send_result?;
                pending_blocks = Some(read_result?);
                start_block = next_start;
            } else {
                // Last batch, just send
                send_block_batch(framed, rel_path, blocks, progress).await?;
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
    rel_path: &str,
    source_reader: Arc<SourceReadContext>,
) -> anyhow::Result<()>
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
            path: rel_path.to_string(),
            hashes,
        })?)
        .await?;
    Ok(())
}

async fn handle_request_blocks_message<T>(
    framed: &mut Framed<T, PxsCodec>,
    rel_path: &str,
    source_reader: Arc<SourceReadContext>,
    metadata: FileMetadata,
    indices: Vec<u32>,
    progress: &Arc<ProgressBar>,
) -> anyhow::Result<()>
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
        send_block_batch(framed, rel_path, blocks, progress).await?;
    }

    send_apply_metadata(framed, rel_path, metadata).await
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
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let rel_path = relative_protocol_path(src_root, path)?;
    let metadata = FileMetadata::from(tokio::fs::metadata(path).await?);
    let mut source_reader: Option<Arc<SourceReadContext>> = None;
    progress.set_message(rel_path.clone());
    send_sync_file_message(framed, &rel_path, metadata, threshold, checksum).await?;

    while let Some(msg_bytes) = recv_with_timeout(framed).await? {
        let msg = deserialize_message(&msg_bytes)?;
        match msg {
            Message::RequestFullCopy {
                path: received_path,
            } => {
                ensure_expected_protocol_path(&rel_path, &received_path)?;
                let source_reader = ensure_source_read_context(&mut source_reader, path).await?;
                handle_request_full_copy_message(
                    framed,
                    &rel_path,
                    source_reader,
                    metadata,
                    &progress,
                )
                .await?;
            }
            Message::RequestHashes {
                path: received_path,
            } => {
                ensure_expected_protocol_path(&rel_path, &received_path)?;
                let source_reader = ensure_source_read_context(&mut source_reader, path).await?;
                handle_request_hashes_message(framed, &rel_path, source_reader).await?;
            }
            Message::RequestBlocks {
                path: received_path,
                indices,
            } => {
                ensure_expected_protocol_path(&rel_path, &received_path)?;
                let source_reader = ensure_source_read_context(&mut source_reader, path).await?;
                handle_request_blocks_message(
                    framed,
                    &rel_path,
                    source_reader,
                    metadata,
                    indices,
                    &progress,
                )
                .await?;
            }
            Message::MetadataApplied {
                path: received_path,
            } => {
                ensure_expected_protocol_path(&rel_path, &received_path)?;
                if checksum {
                    let hash = tools::blake3_file_hash(path).await?;
                    framed
                        .send(serialize_message(&Message::VerifyChecksum {
                            path: rel_path.clone(),
                            hash,
                        })?)
                        .await?;
                } else {
                    return Ok(());
                }
            }
            Message::ChecksumVerified {
                path: received_path,
            } => {
                ensure_expected_protocol_path(&rel_path, &received_path)?;
                return Ok(());
            }
            Message::ChecksumMismatch {
                path: received_path,
            } => {
                ensure_expected_protocol_path(&rel_path, &received_path)?;
                anyhow::bail!(
                    "Checksum mismatch for {rel_path}: destination file differs from source after transfer"
                );
            }
            Message::EndOfFile {
                path: received_path,
            } => {
                ensure_expected_protocol_path(&rel_path, &received_path)?;
                progress.inc(metadata.size);
                send_apply_metadata(framed, &rel_path, metadata).await?;
            }
            _ => anyhow::bail!("Unexpected message: {msg:?}"),
        }
    }

    anyhow::bail!("Connection closed unexpectedly")
}
