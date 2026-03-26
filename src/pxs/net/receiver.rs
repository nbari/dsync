use super::{
    BLOCK_SIZE, RemoteSyncOptions,
    codec::PxsCodec,
    path::{ensure_expected_protocol_path, resolve_protocol_path, validate_protocol_path},
    protocol::{
        Block, FileMetadata, Message, apply_file_metadata, deserialize_block_batch,
        deserialize_message, serialize_message,
    },
    shared::{
        TransportFeatures, block_bytes, connect_with_retry, local_handshake_version,
        negotiate_transport_features, recv_with_timeout, skipped_bytes,
    },
    ssh::{ChildSession, build_ssh_command, build_ssh_pull_command},
};
use crate::pxs::tools;
use futures_util::SinkExt;
use indicatif::ProgressBar;
use std::{
    collections::{HashMap, HashSet},
    os::unix::fs::{FileExt, MetadataExt},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
};
use tokio_util::codec::Framed;

struct PendingFile {
    staged_file: tools::StagedFile,
    file: Option<std::fs::File>,
    checksum: bool,
    parallel_transfer: Option<ParallelTransferRecord>,
}

#[derive(Debug)]
struct ParallelTransferRecord {
    id: String,
    dir: PathBuf,
}

#[derive(Clone, Copy, Debug)]
struct ParallelTransferConfig {
    threshold_bytes: u64,
    worker_count: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransferPhase {
    Hashes,
    Content,
    Metadata,
    Checksum,
}

#[derive(Clone, Copy, Debug)]
struct FileTransferState {
    metadata: FileMetadata,
    checksum: bool,
    phase: TransferPhase,
}

#[derive(Clone, Copy)]
enum RemoteFsyncOverride {
    Allow,
    Deny,
}

#[derive(Clone, Copy)]
enum RemoteDeleteOverride {
    Allow,
    Deny,
}

#[derive(Clone, Copy)]
enum Lz4Allowance {
    Allow,
    Deny,
}

#[derive(Clone)]
struct ClientHandlingOptions {
    show_progress: bool,
    fsync: bool,
    remote_fsync_override: RemoteFsyncOverride,
    remote_delete_override: RemoteDeleteOverride,
    lz4: Lz4Allowance,
    large_file_parallel: bool,
    ignores: Arc<[String]>,
}

struct ClientState {
    pending_files: HashMap<String, PendingFile>,
    file_transfers: HashMap<String, FileTransferState>,
    progress: Option<Arc<ProgressBar>>,
    dir_metadata: Vec<(String, FileMetadata)>,
    dst_root: PathBuf,
    transport: TransportFeatures,
    protocol_state: ProtocolState,
    session_fsync_override: Option<bool>,
    delete_mode: DeleteMode,
    source_paths: HashSet<String>,
    parallel_config: Option<ParallelTransferConfig>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProtocolState {
    AwaitingHandshake,
    AwaitingTransfer,
    InTransfer,
    Finalized,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeleteMode {
    Disabled,
    Enabled,
}

const TRANSFER_STATE_DIR_NAME: &str = ".pxs-transfers";
const TRANSFER_PATH_FILENAME: &str = "path";
const TRANSFER_STAGED_LINK_NAME: &str = "staged";
static PARALLEL_TRANSFER_COUNTER: AtomicU64 = AtomicU64::new(1);

impl ClientState {
    fn new(dst_root: &Path) -> Self {
        Self {
            pending_files: HashMap::new(),
            file_transfers: HashMap::new(),
            progress: None,
            dir_metadata: Vec::new(),
            dst_root: dst_root.to_path_buf(),
            transport: TransportFeatures::default(),
            protocol_state: ProtocolState::AwaitingHandshake,
            session_fsync_override: None,
            delete_mode: DeleteMode::Disabled,
            source_paths: HashSet::new(),
            parallel_config: None,
        }
    }

    /// Prepare a staged file for an incoming transfer.
    ///
    /// # Errors
    ///
    /// Returns an error if the staging file cannot be created.
    fn prepare_pending_file(
        &mut self,
        path: &str,
        final_path: &Path,
        size: u64,
        seed_from_existing: bool,
        checksum: bool,
    ) -> anyhow::Result<()> {
        if let Some(existing) = self.pending_files.remove(path) {
            let _ = cleanup_pending_file(existing);
        }

        let staged_file = tools::StagedFile::new(final_path)?;
        staged_file.prepare(size, seed_from_existing)?;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(staged_file.path())?;
        self.pending_files.insert(
            path.to_string(),
            PendingFile {
                staged_file,
                file: Some(file),
                checksum,
                parallel_transfer: None,
            },
        );
        Ok(())
    }

    /// Clean up partial files on transfer failure.
    fn cleanup_partial_files(&mut self) {
        for (path, pending_file) in self.pending_files.drain() {
            if let Err(e) = cleanup_pending_file(pending_file)
                && let Ok(full_path) = resolve_protocol_path(&self.dst_root, &path)
            {
                eprintln!(
                    "Warning: failed to remove partial file {}: {e}",
                    full_path.display(),
                );
            }
        }
    }

    fn effective_fsync(&self, default_fsync: bool) -> bool {
        self.session_fsync_override.unwrap_or(default_fsync)
    }

    fn delete_enabled(&self) -> bool {
        self.delete_mode == DeleteMode::Enabled
    }

    fn set_transfer_state(
        &mut self,
        path: &str,
        metadata: FileMetadata,
        checksum: bool,
        phase: TransferPhase,
    ) {
        self.file_transfers.insert(
            path.to_string(),
            FileTransferState {
                metadata,
                checksum,
                phase,
            },
        );
    }

    fn transfer_state(&self, path: &str) -> anyhow::Result<FileTransferState> {
        self.file_transfers
            .get(path)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("missing active transfer state for path: {path}"))
    }

    fn update_transfer_phase(&mut self, path: &str, phase: TransferPhase) -> anyhow::Result<()> {
        let transfer = self
            .file_transfers
            .get_mut(path)
            .ok_or_else(|| anyhow::anyhow!("missing active transfer state for path: {path}"))?;
        transfer.phase = phase;
        Ok(())
    }

    fn clear_transfer_state(&mut self, path: &str) {
        self.file_transfers.remove(path);
    }

    fn parallel_config(&self) -> Option<ParallelTransferConfig> {
        self.parallel_config
    }

    fn ensure_parallel_transfer_record(&mut self, path: &str) -> anyhow::Result<String> {
        let pending_file = self
            .pending_files
            .get_mut(path)
            .ok_or_else(|| anyhow::anyhow!("missing pending file for path: {path}"))?;
        if let Some(record) = &pending_file.parallel_transfer {
            return Ok(record.id.clone());
        }

        let record =
            create_parallel_transfer_record(&self.dst_root, path, pending_file.staged_file.path())?;
        let transfer_id = record.id.clone();
        pending_file.parallel_transfer = Some(record);
        Ok(transfer_id)
    }
}

fn validate_transfer_id(transfer_id: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !transfer_id.is_empty()
            && transfer_id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() || byte == b'-'),
        "invalid transfer id"
    );
    Ok(())
}

fn transfer_state_root(dst_root: &Path) -> PathBuf {
    dst_root.join(TRANSFER_STATE_DIR_NAME)
}

fn transfer_record_dir(dst_root: &Path, transfer_id: &str) -> anyhow::Result<PathBuf> {
    validate_transfer_id(transfer_id)?;
    Ok(transfer_state_root(dst_root).join(transfer_id))
}

fn next_parallel_transfer_id() -> String {
    let counter = PARALLEL_TRANSFER_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u128, |duration| duration.as_nanos());
    format!("{:x}-{:x}-{:x}", std::process::id(), counter, nanos)
}

fn remove_parallel_transfer_record(record_dir: &Path) -> anyhow::Result<()> {
    match std::fs::remove_dir_all(record_dir) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn cleanup_pending_file(mut pending_file: PendingFile) -> anyhow::Result<()> {
    drop(pending_file.file.take());
    let staged_cleanup = pending_file.staged_file.cleanup();
    let transfer_cleanup = pending_file
        .parallel_transfer
        .take()
        .map_or(Ok(()), |record| {
            remove_parallel_transfer_record(&record.dir)
        });
    staged_cleanup?;
    transfer_cleanup?;
    Ok(())
}

fn create_parallel_transfer_record(
    dst_root: &Path,
    expected_path: &str,
    staged_path: &Path,
) -> anyhow::Result<ParallelTransferRecord> {
    let transfer_root = transfer_state_root(dst_root);
    tools::ensure_no_symlink_ancestors_under_root(dst_root, &transfer_root)?;
    std::fs::create_dir_all(&transfer_root)?;

    loop {
        let transfer_id = next_parallel_transfer_id();
        let record_dir = transfer_record_dir(dst_root, &transfer_id)?;
        match std::fs::create_dir(&record_dir) {
            Ok(()) => {
                let staged_target = if staged_path.is_absolute() {
                    staged_path.to_path_buf()
                } else {
                    std::env::current_dir()?.join(staged_path)
                };
                let path_file = record_dir.join(TRANSFER_PATH_FILENAME);
                let staged_link = record_dir.join(TRANSFER_STAGED_LINK_NAME);
                if let Err(error) = std::fs::write(&path_file, expected_path.as_bytes()) {
                    let _ = remove_parallel_transfer_record(&record_dir);
                    return Err(error.into());
                }
                if let Err(error) = std::os::unix::fs::symlink(&staged_target, &staged_link) {
                    let _ = remove_parallel_transfer_record(&record_dir);
                    return Err(error.into());
                }
                return Ok(ParallelTransferRecord {
                    id: transfer_id,
                    dir: record_dir,
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
}

fn resolve_parallel_transfer_record(
    dst_root: &Path,
    transfer_id: &str,
    expected_path: &str,
) -> anyhow::Result<PathBuf> {
    let record_dir = transfer_record_dir(dst_root, transfer_id)?;
    tools::ensure_no_symlink_ancestors_under_root(dst_root, &record_dir)?;
    let recorded_path =
        std::fs::read_to_string(record_dir.join(TRANSFER_PATH_FILENAME)).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                anyhow::anyhow!("unknown transfer id: {transfer_id}")
            } else {
                anyhow::Error::from(error)
            }
        })?;
    ensure_expected_protocol_path(recorded_path.trim_end_matches('\n'), expected_path)?;
    let staged_path =
        std::fs::read_link(record_dir.join(TRANSFER_STAGED_LINK_NAME)).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                anyhow::anyhow!("unknown transfer id: {transfer_id}")
            } else {
                anyhow::Error::from(error)
            }
        })?;
    Ok(staged_path)
}

/// Run the client in pull mode (connects to a server to receive files).
///
/// # Errors
///
/// Returns an error if the connection fails or synchronization fails.
pub async fn run_pull_client(addr: &str, dst_root: &Path, fsync: bool) -> anyhow::Result<()> {
    let stream = connect_with_retry(addr).await?;
    let mut framed = Framed::new(stream, PxsCodec);
    handle_client_with_transport(
        &mut framed,
        dst_root,
        ClientHandlingOptions {
            show_progress: true,
            fsync,
            remote_fsync_override: RemoteFsyncOverride::Deny,
            remote_delete_override: RemoteDeleteOverride::Deny,
            lz4: Lz4Allowance::Allow,
            large_file_parallel: false,
            ignores: Arc::from(Vec::<String>::new()),
        },
    )
    .await
}

/// Run the receiver to handle incoming sync connections.
///
/// # Errors
///
/// Returns an error if the listener fails to bind or synchronization fails.
pub async fn run_receiver(addr: &str, dst_root: &Path, fsync: bool) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    eprintln!("Receiver listening on {addr}");
    while let Ok((stream, peer_addr)) = listener.accept().await {
        eprintln!("Accepted connection from {peer_addr}");
        let dst_root = dst_root.to_path_buf();
        tokio::spawn(async move {
            let mut framed = Framed::new(stream, PxsCodec);
            if let Err(error) = handle_client_with_transport(
                &mut framed,
                &dst_root,
                ClientHandlingOptions {
                    show_progress: false,
                    fsync,
                    remote_fsync_override: RemoteFsyncOverride::Allow,
                    remote_delete_override: RemoteDeleteOverride::Deny,
                    lz4: Lz4Allowance::Allow,
                    large_file_parallel: false,
                    ignores: Arc::from(Vec::<String>::new()),
                },
            )
            .await
            {
                eprintln!("Error handling client {peer_addr}: {error}");
            }
            eprintln!("Connection with {peer_addr} closed");
        });
    }
    Ok(())
}

/// Run the receiver using stdin/stdout (for SSH tunnels).
///
/// # Errors
///
/// Returns an error if synchronization fails.
pub async fn run_stdio_receiver(
    dst_root: &Path,
    fsync: bool,
    ignores: &[String],
    _quiet: bool,
) -> anyhow::Result<()> {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    let combined = tokio::io::join(stdin, stdout);
    let mut framed = Framed::new(combined, PxsCodec);
    handle_client_with_transport(
        &mut framed,
        dst_root,
        ClientHandlingOptions {
            show_progress: false,
            fsync,
            remote_fsync_override: RemoteFsyncOverride::Deny,
            remote_delete_override: RemoteDeleteOverride::Allow,
            lz4: Lz4Allowance::Deny,
            large_file_parallel: true,
            ignores: Arc::<[String]>::from(ignores.to_vec()),
        },
    )
    .await
}

/// Run the receiver in SSH chunk-writer mode using stdin/stdout.
///
/// # Errors
///
/// Returns an error if the staged file cannot be updated or protocol handling fails.
pub async fn run_stdio_chunk_writer(
    dst_root: &Path,
    transfer_id: &str,
    expected_path: &str,
    _quiet: bool,
) -> anyhow::Result<()> {
    tools::ensure_no_symlink_ancestors_under_root(dst_root, dst_root)?;
    validate_protocol_path(expected_path)?;
    let staged_path = resolve_parallel_transfer_record(dst_root, transfer_id, expected_path)?;

    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    let combined = tokio::io::join(stdin, stdout);
    let mut framed = Framed::new(combined, PxsCodec);
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(staged_path)?;

    let mut saw_handshake = false;
    while let Some(bytes) = recv_with_timeout(&mut framed).await? {
        let msg = deserialize_message(&bytes)?;
        match msg {
            Message::Handshake { version } => {
                anyhow::ensure!(!saw_handshake, "duplicate handshake message");
                let _features = negotiate_transport_features(&version, false, false)?;
                saw_handshake = true;
                framed
                    .send(serialize_message(&Message::Handshake {
                        version: local_handshake_version(false, false),
                    })?)
                    .await?;
            }
            Message::ApplyBlocks { path, blocks } => {
                anyhow::ensure!(saw_handshake, "expected handshake before chunk data");
                ensure_expected_protocol_path(expected_path, &path)?;
                for block in blocks {
                    file.write_all_at(&block.data, block.offset)?;
                }
            }
            Message::SyncComplete => {
                anyhow::ensure!(saw_handshake, "expected handshake before sync completion");
                framed
                    .send(serialize_message(&Message::SyncCompleteAck)?)
                    .await?;
                return Ok(());
            }
            Message::Error(message) => anyhow::bail!("chunk writer sender error: {message}"),
            Message::ApplyBlocksCompressed { .. } => {
                anyhow::bail!("chunk writer does not support compressed block batches")
            }
            other => anyhow::bail!("unexpected chunk writer protocol message: {other:?}"),
        }
    }

    anyhow::bail!("chunk writer connection closed unexpectedly")
}

/// Run the receiver over an SSH tunnel (for pull mode).
///
/// # Errors
///
/// Returns an error if the SSH command fails or synchronization fails.
pub async fn run_ssh_receiver(
    addr: &str,
    dst_root: &Path,
    src_path: &str,
    options: RemoteSyncOptions<'_>,
) -> anyhow::Result<()> {
    let remote_cmd = build_ssh_pull_command(
        src_path,
        options.threshold,
        options.features.checksum,
        options.features.delete,
        options.ignores,
    );
    let mut session = ChildSession::spawn(build_ssh_command(addr, &remote_cmd))?;
    let result = handle_client_with_transport(
        session.framed_mut()?,
        dst_root,
        ClientHandlingOptions {
            show_progress: true,
            fsync: options.features.fsync,
            remote_fsync_override: RemoteFsyncOverride::Deny,
            remote_delete_override: RemoteDeleteOverride::Allow,
            lz4: Lz4Allowance::Deny,
            large_file_parallel: false,
            ignores: Arc::<[String]>::from(options.ignores.to_vec()),
        },
    )
    .await;
    session.finish(result, "SSH process").await?;
    Ok(())
}

async fn handle_handshake<T>(
    framed: &mut Framed<T, PxsCodec>,
    version: String,
    allow_lz4: bool,
    allow_large_file_parallel: bool,
) -> anyhow::Result<TransportFeatures>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let features =
        match negotiate_transport_features(&version, allow_lz4, allow_large_file_parallel) {
            Ok(features) => features,
            Err(error) => {
                framed
                    .send(serialize_message(&Message::Error(error.to_string()))?)
                    .await?;
                return Err(error);
            }
        };
    let response = Message::Handshake {
        version: local_handshake_version(allow_lz4, allow_large_file_parallel),
    };
    framed.send(serialize_message(&response)?).await?;
    Ok(features)
}

async fn handle_sync_symlink(
    path: String,
    target: String,
    metadata: FileMetadata,
    dst_root: &Path,
    fsync: bool,
) -> anyhow::Result<()> {
    let full_path = resolve_protocol_path(dst_root, &path)?;
    if let Some(parent) = full_path.parent()
        && !parent.exists()
    {
        tokio::fs::create_dir_all(parent).await?;
    }
    let prepared_path = tools::create_prepared_symlink(Path::new(&target), &full_path)?;
    if let Err(error) = apply_file_metadata(&prepared_path, &metadata) {
        let _ = std::fs::remove_file(&prepared_path);
        return Err(error);
    }
    tools::install_prepared_path(&prepared_path, &full_path)?;
    if fsync {
        tools::sync_parent_directory(&full_path)?;
    }
    Ok(())
}

fn parallel_transfer_config_for(
    state: &ClientState,
    metadata: FileMetadata,
) -> Option<ParallelTransferConfig> {
    let config = state.parallel_config()?;
    if !state.transport.large_file_parallel || config.worker_count <= 1 {
        return None;
    }
    if metadata.size < config.threshold_bytes {
        return None;
    }
    Some(config)
}

fn pending_transfer_id(state: &mut ClientState, path: &str) -> anyhow::Result<String> {
    state.ensure_parallel_transfer_record(path)
}

async fn handle_sync_file<T>(
    framed: &mut Framed<T, PxsCodec>,
    state: &mut ClientState,
    path: String,
    metadata: FileMetadata,
    threshold: f32,
    checksum: bool,
    dst_root: &Path,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    tracing::debug!("Syncing file: {path} ({} bytes)", metadata.size);
    let full_path = resolve_protocol_path(dst_root, &path)?;
    let progress = state.progress.clone();
    state.clear_transfer_state(&path);

    if let Ok(existing_meta) = tokio::fs::symlink_metadata(&full_path).await {
        if existing_meta.file_type().is_file() {
            let meta = tokio::fs::metadata(&full_path).await?;
            if meta.len() == metadata.size
                && !checksum
                && meta.mtime() == metadata.mtime
                && u32::try_from(meta.mtime_nsec()).ok() == Some(metadata.mtime_nsec)
            {
                if let Some(progress) = &progress {
                    progress.inc(metadata.size);
                }
                framed
                    .send(serialize_message(&Message::EndOfFile {
                        path: path.clone(),
                    })?)
                    .await?;
                state.set_transfer_state(&path, metadata, checksum, TransferPhase::Metadata);
                return Ok(());
            }

            if metadata.size > 0
                && tools::should_use_full_copy_meta(metadata.size, &full_path, threshold).await?
            {
                state.prepare_pending_file(&path, &full_path, metadata.size, false, checksum)?;
                state.set_transfer_state(&path, metadata, checksum, TransferPhase::Content);
                if let Some(progress) = &progress {
                    progress.set_message(format!("Full copy: {path}"));
                }
                if parallel_transfer_config_for(state, metadata).is_some() {
                    let transfer_id = pending_transfer_id(state, &path)?;
                    framed
                        .send(serialize_message(&Message::RequestParallelFullCopy {
                            path,
                            transfer_id,
                        })?)
                        .await?;
                } else {
                    framed
                        .send(serialize_message(&Message::RequestFullCopy { path })?)
                        .await?;
                }
                return Ok(());
            }

            if let Some(progress) = &progress {
                progress.set_message(format!("Hashing: {path}"));
            }
            state.set_transfer_state(&path, metadata, checksum, TransferPhase::Hashes);
            framed
                .send(serialize_message(&Message::RequestHashes { path })?)
                .await?;
            return Ok(());
        }

        state.prepare_pending_file(&path, &full_path, metadata.size, false, checksum)?;
        state.set_transfer_state(&path, metadata, checksum, TransferPhase::Content);
        if let Some(progress) = &progress {
            progress.set_message(format!("Full copy: {path}"));
        }
        if parallel_transfer_config_for(state, metadata).is_some() {
            let transfer_id = pending_transfer_id(state, &path)?;
            framed
                .send(serialize_message(&Message::RequestParallelFullCopy {
                    path,
                    transfer_id,
                })?)
                .await?;
        } else {
            framed
                .send(serialize_message(&Message::RequestFullCopy { path })?)
                .await?;
        }
        return Ok(());
    }

    state.prepare_pending_file(&path, &full_path, metadata.size, false, checksum)?;
    state.set_transfer_state(&path, metadata, checksum, TransferPhase::Content);
    if let Some(progress) = &progress {
        progress.set_message(format!("Full copy: {path}"));
    }
    if parallel_transfer_config_for(state, metadata).is_some() {
        let transfer_id = pending_transfer_id(state, &path)?;
        framed
            .send(serialize_message(&Message::RequestParallelFullCopy {
                path,
                transfer_id,
            })?)
            .await?;
    } else {
        framed
            .send(serialize_message(&Message::RequestFullCopy { path })?)
            .await?;
    }
    Ok(())
}

fn handle_sync_dir_message(
    state: &mut ClientState,
    dst_root: &Path,
    path: String,
    metadata: FileMetadata,
    fsync: bool,
) -> anyhow::Result<()> {
    let full_path = resolve_protocol_path(dst_root, &path)?;
    tools::ensure_directory_path(&full_path)?;
    if fsync {
        tools::sync_directory_and_parent(&full_path)?;
    }
    state.dir_metadata.push((path, metadata));
    Ok(())
}

async fn handle_sync_file_message<T>(
    framed: &mut Framed<T, PxsCodec>,
    state: &mut ClientState,
    path: String,
    metadata: FileMetadata,
    threshold: f32,
    checksum: bool,
    dst_root: &Path,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let progress = state.progress.clone();
    if let Some(progress) = &progress {
        progress.set_message(path.clone());
    }

    handle_sync_file(framed, state, path, metadata, threshold, checksum, dst_root).await
}

async fn request_full_copy_for_path<T>(
    framed: &mut Framed<T, PxsCodec>,
    state: &mut ClientState,
    path: String,
    full_path: &Path,
    metadata: FileMetadata,
    checksum: bool,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    state.prepare_pending_file(&path, full_path, metadata.size, false, checksum)?;
    state.update_transfer_phase(&path, TransferPhase::Content)?;
    framed
        .send(serialize_message(&Message::RequestFullCopy { path })?)
        .await?;
    Ok(())
}

async fn request_missing_blocks_for_path<T>(
    framed: &mut Framed<T, PxsCodec>,
    state: &mut ClientState,
    path: String,
    transfer: FileTransferState,
    full_path: &Path,
    requested: Vec<u32>,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    if !state.pending_files.contains_key(&path) {
        state.prepare_pending_file(
            &path,
            full_path,
            transfer.metadata.size,
            true,
            transfer.checksum,
        )?;
    }
    state.update_transfer_phase(&path, TransferPhase::Content)?;
    if parallel_transfer_config_for(state, transfer.metadata).is_some() {
        let transfer_id = pending_transfer_id(state, &path)?;
        framed
            .send(serialize_message(&Message::RequestParallelBlocks {
                path,
                transfer_id,
                indices: requested,
            })?)
            .await?;
    } else {
        framed
            .send(serialize_message(&Message::RequestBlocks {
                path,
                indices: requested,
            })?)
            .await?;
    }
    Ok(())
}

async fn handle_block_hashes_message<T>(
    framed: &mut Framed<T, PxsCodec>,
    state: &mut ClientState,
    path: String,
    hashes: Vec<u64>,
    dst_root: &Path,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let full_path = resolve_protocol_path(dst_root, &path)?;
    let transfer = state.transfer_state(&path)?;
    anyhow::ensure!(
        transfer.phase == TransferPhase::Hashes,
        "received block hashes out of sequence for path: {path}"
    );

    let requested = if let Some(pending_file) = state.pending_files.get(&path) {
        tools::compute_requested_blocks(pending_file.staged_file.path(), &hashes, BLOCK_SIZE)?
    } else {
        match tokio::fs::symlink_metadata(&full_path).await {
            Ok(meta) if meta.is_file() => {
                match tools::compute_requested_blocks(&full_path, &hashes, BLOCK_SIZE) {
                    Ok(requested) => requested,
                    Err(error) if is_not_found_error(&error) => {
                        request_full_copy_for_path(
                            framed,
                            state,
                            path,
                            &full_path,
                            transfer.metadata,
                            transfer.checksum,
                        )
                        .await?;
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                }
            }
            Ok(_) => {
                request_full_copy_for_path(
                    framed,
                    state,
                    path,
                    &full_path,
                    transfer.metadata,
                    transfer.checksum,
                )
                .await?;
                return Ok(());
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                request_full_copy_for_path(
                    framed,
                    state,
                    path,
                    &full_path,
                    transfer.metadata,
                    transfer.checksum,
                )
                .await?;
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        }
    };

    if let Some(progress) = &state.progress {
        progress.inc(skipped_bytes(transfer.metadata, &requested));
    }

    if requested.is_empty() {
        state.update_transfer_phase(&path, TransferPhase::Metadata)?;
        framed
            .send(serialize_message(&Message::EndOfFile { path })?)
            .await?;
    } else {
        request_missing_blocks_for_path(framed, state, path, transfer, &full_path, requested)
            .await?;
    }

    Ok(())
}

fn handle_apply_blocks_message(
    state: &mut ClientState,
    path: &str,
    blocks: Vec<Block>,
) -> anyhow::Result<()> {
    if let Some(progress) = &state.progress {
        progress.inc(block_bytes(&blocks)?);
    }

    handle_apply_blocks(&mut state.pending_files, path, blocks)
}

fn handle_end_of_file_message(state: &mut ClientState, path: &str) -> anyhow::Result<()> {
    validate_protocol_path(path)?;
    if let Some(pending_file) = state.pending_files.remove(path) {
        cleanup_pending_file(pending_file)?;
    }
    state.clear_transfer_state(path);
    Ok(())
}

fn handle_session_options(
    state: &mut ClientState,
    remote_fsync_override: RemoteFsyncOverride,
    remote_delete_override: RemoteDeleteOverride,
    fsync: bool,
    delete: bool,
) -> anyhow::Result<()> {
    if fsync {
        anyhow::ensure!(
            matches!(remote_fsync_override, RemoteFsyncOverride::Allow),
            "remote fsync session options are not supported for this receiver"
        );
    }
    if delete {
        anyhow::ensure!(
            matches!(remote_delete_override, RemoteDeleteOverride::Allow),
            "remote delete session options are not supported for this receiver"
        );
    }
    anyhow::ensure!(
        matches!(state.protocol_state, ProtocolState::AwaitingTransfer),
        "session options must be sent before transfer messages"
    );
    if fsync {
        state.session_fsync_override = Some(fsync);
    }
    state.delete_mode = if delete {
        DeleteMode::Enabled
    } else {
        DeleteMode::Disabled
    };
    Ok(())
}

fn handle_parallel_transfer_config(
    state: &mut ClientState,
    worker_count: u16,
    threshold_bytes: u64,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        matches!(state.protocol_state, ProtocolState::AwaitingTransfer),
        "parallel transfer config must be sent before transfer messages"
    );
    let worker_count = usize::from(worker_count);
    anyhow::ensure!(
        worker_count > 0,
        "parallel transfer worker count must be greater than zero"
    );
    state.parallel_config = Some(ParallelTransferConfig {
        threshold_bytes,
        worker_count,
    });
    Ok(())
}

async fn handle_sync_complete_message<T>(
    framed: &mut Framed<T, PxsCodec>,
    state: &mut ClientState,
    dst_root: &Path,
    ignores: &[String],
    fsync: bool,
) -> anyhow::Result<bool>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    anyhow::ensure!(
        state.pending_files.is_empty(),
        "received sync completion with {} pending staged file(s)",
        state.pending_files.len()
    );
    anyhow::ensure!(
        state.file_transfers.is_empty(),
        "received sync completion with {} active transfer state(s)",
        state.file_transfers.len()
    );
    finalize_client_state(state, dst_root, ignores, fsync)?;
    framed
        .send(serialize_message(&Message::SyncCompleteAck)?)
        .await?;
    Ok(false)
}

async fn process_handshake_or_session_message<T>(
    framed: &mut Framed<T, PxsCodec>,
    state: &mut ClientState,
    options: &ClientHandlingOptions,
    msg: &Message,
) -> anyhow::Result<Option<bool>>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    if state.protocol_state == ProtocolState::AwaitingHandshake {
        return match msg {
            Message::Handshake { version } => {
                tracing::debug!("Client connected (version: {version})");
                state.transport = handle_handshake(
                    framed,
                    version.clone(),
                    matches!(options.lz4, Lz4Allowance::Allow),
                    options.large_file_parallel,
                )
                .await?;
                state.protocol_state = ProtocolState::AwaitingTransfer;
                Ok(Some(true))
            }
            other => {
                anyhow::bail!("expected handshake before transfer messages, got {other:?}")
            }
        };
    }

    if let Message::SessionOptions { fsync, delete } = msg {
        handle_session_options(
            state,
            options.remote_fsync_override,
            options.remote_delete_override,
            *fsync,
            *delete,
        )?;
        return Ok(Some(true));
    }

    if let Message::ParallelTransferConfig {
        threshold_bytes,
        worker_count,
    } = msg
    {
        anyhow::ensure!(
            state.transport.large_file_parallel,
            "parallel transfer config is not supported by this receiver"
        );
        handle_parallel_transfer_config(state, *worker_count, *threshold_bytes)?;
        return Ok(Some(true));
    }

    Ok(None)
}

async fn process_client_message<T>(
    framed: &mut Framed<T, PxsCodec>,
    state: &mut ClientState,
    dst_root: &Path,
    options: &ClientHandlingOptions,
    msg: Message,
) -> anyhow::Result<bool>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    if let Some(result) = process_handshake_or_session_message(framed, state, options, &msg).await?
    {
        return Ok(result);
    }

    if state.protocol_state == ProtocolState::AwaitingTransfer {
        state.protocol_state = ProtocolState::InTransfer;
    }

    match msg {
        Message::Handshake { .. } => anyhow::bail!("duplicate handshake message"),
        Message::SyncStart { total_size } => {
            tracing::debug!("Starting sync (total size: {total_size} bytes)");
            if options.show_progress && total_size > 0 {
                state.progress = Some(Arc::new(tools::create_progress_bar(total_size)));
            }
        }
        Message::SyncDir { path, metadata } => {
            state.source_paths.insert(path.clone());
            let effective_fsync = state.effective_fsync(options.fsync);
            handle_sync_dir_message(state, dst_root, path, metadata, effective_fsync)?;
        }
        Message::SyncSymlink {
            path,
            target,
            metadata,
        } => {
            state.source_paths.insert(path.clone());
            let effective_fsync = state.effective_fsync(options.fsync);
            handle_sync_symlink(path, target, metadata, dst_root, effective_fsync).await?;
        }
        Message::SyncFile {
            path,
            metadata,
            threshold,
            checksum,
        } => {
            state.source_paths.insert(path.clone());
            handle_sync_file_message(framed, state, path, metadata, threshold, checksum, dst_root)
                .await?;
        }
        Message::BlockHashes { path, hashes } => {
            handle_block_hashes_message(framed, state, path, hashes, dst_root).await?;
        }
        Message::ApplyBlocks { path, blocks } => {
            handle_apply_blocks_message(state, &path, blocks)?;
        }
        Message::ApplyBlocksCompressed { path, compressed } => {
            if !state.transport.lz4_block_messages {
                anyhow::bail!("received compressed block batch without negotiated LZ4 support");
            }
            let payload = lz4_flex::decompress_size_prepended(&compressed)
                .map_err(|error| anyhow::anyhow!("failed to decompress block batch: {error}"))?;
            let blocks = deserialize_block_batch(&payload)?;
            handle_apply_blocks_message(state, &path, blocks)?;
        }
        Message::ApplyMetadata { path, metadata } => {
            let effective_fsync = state.effective_fsync(options.fsync);
            handle_apply_metadata(framed, state, path, metadata, dst_root, effective_fsync).await?;
        }
        Message::VerifyChecksum { path, hash } => {
            let effective_fsync = state.effective_fsync(options.fsync);
            handle_verify_checksum(framed, state, &path, &hash, dst_root, effective_fsync).await?;
        }
        Message::EndOfFile { path } => {
            handle_end_of_file_message(state, &path)?;
        }
        Message::SyncComplete => {
            let effective_fsync = state.effective_fsync(options.fsync);
            return handle_sync_complete_message(
                framed,
                state,
                dst_root,
                options.ignores.as_ref(),
                effective_fsync,
            )
            .await;
        }
        Message::RequestFullCopy { .. }
        | Message::RequestParallelFullCopy { .. }
        | Message::RequestHashes { .. }
        | Message::RequestBlocks { .. }
        | Message::RequestParallelBlocks { .. }
        | Message::MetadataApplied { .. }
        | Message::ChecksumVerified { .. }
        | Message::ChecksumMismatch { .. }
        | Message::SessionOptions { .. }
        | Message::ParallelTransferConfig { .. }
        | Message::SyncCompleteAck
        | Message::Error(_) => {
            anyhow::bail!("unexpected receiver-side protocol message: {msg:?}");
        }
    }

    Ok(true)
}

fn finalize_directory_metadata(
    dst_root: &Path,
    mut dir_metadata: Vec<(String, FileMetadata)>,
    fsync: bool,
) -> anyhow::Result<()> {
    dir_metadata.sort_by_key(|(path, _)| std::cmp::Reverse(path.split('/').count()));
    for (path, metadata) in dir_metadata {
        let full_path = resolve_protocol_path(dst_root, &path)?;
        apply_file_metadata(&full_path, &metadata)?;
        if fsync {
            tools::sync_directory_and_parent(&full_path)?;
        }
    }
    Ok(())
}

fn protocol_relative_path(root: &Path, path: &Path) -> anyhow::Result<String> {
    let rel_path = path
        .strip_prefix(root)
        .map_err(|_| {
            anyhow::anyhow!(
                "path {} is not under root {}",
                path.display(),
                root.display()
            )
        })?
        .components()
        .map(|component| component.as_os_str().to_string_lossy().to_string())
        .collect::<Vec<_>>()
        .join("/");
    validate_protocol_path(&rel_path)?;
    Ok(rel_path)
}

fn delete_extraneous_destination_entries(
    dst_root: &Path,
    source_paths: &HashSet<String>,
    ignores: &[String],
    fsync: bool,
) -> anyhow::Result<()> {
    use ignore::{WalkBuilder, overrides::OverrideBuilder};

    tools::ensure_no_symlink_ancestors_under_root(dst_root, dst_root)?;

    match std::fs::symlink_metadata(dst_root) {
        Ok(meta) => {
            anyhow::ensure!(
                meta.is_dir(),
                "--delete requires a directory destination root: {}",
                dst_root.display()
            );
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    }

    let mut override_builder = OverrideBuilder::new(dst_root);
    for pattern in ignores {
        override_builder.add(&format!("!{pattern}"))?;
    }
    let overrides = override_builder.build()?;

    let walker = WalkBuilder::new(dst_root)
        .hidden(false)
        .git_ignore(false)
        .git_global(false)
        .git_exclude(false)
        .ignore(false)
        .parents(false)
        .overrides(overrides)
        .build();

    let mut to_delete = Vec::new();
    for entry in walker {
        let entry = entry?;
        let path = entry.path();
        if path == dst_root {
            continue;
        }

        let rel_path = protocol_relative_path(dst_root, path)?;
        if !source_paths.contains(&rel_path) {
            to_delete.push(path.to_path_buf());
        }
    }

    to_delete.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for path in to_delete {
        let meta = match std::fs::symlink_metadata(&path) {
            Ok(meta) => meta,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        if meta.is_dir() {
            std::fs::remove_dir_all(&path)?;
        } else {
            std::fs::remove_file(&path)?;
        }
        if fsync {
            tools::sync_parent_directory(&path)?;
        }
    }

    Ok(())
}

fn finalize_client_state(
    state: &mut ClientState,
    dst_root: &Path,
    ignores: &[String],
    fsync: bool,
) -> anyhow::Result<()> {
    if state.protocol_state == ProtocolState::Finalized {
        return Ok(());
    }

    if state.delete_enabled() {
        delete_extraneous_destination_entries(dst_root, &state.source_paths, ignores, fsync)?;
    }
    finalize_directory_metadata(dst_root, std::mem::take(&mut state.dir_metadata), fsync)?;
    if let Some(progress) = state.progress.take() {
        progress.finish_with_message("Done");
    }
    state.protocol_state = ProtocolState::Finalized;
    Ok(())
}

fn is_not_found_error(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<std::io::Error>()
        .is_some_and(|io_error| io_error.kind() == std::io::ErrorKind::NotFound)
}

/// Handle a client connection.
///
/// # Errors
///
/// Returns an error if synchronization or network I/O fails.
pub async fn handle_client<T>(
    framed: &mut Framed<T, PxsCodec>,
    dst_root: &Path,
    show_progress: bool,
    fsync: bool,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    handle_client_with_transport(
        framed,
        dst_root,
        ClientHandlingOptions {
            show_progress,
            fsync,
            remote_fsync_override: RemoteFsyncOverride::Deny,
            remote_delete_override: RemoteDeleteOverride::Deny,
            lz4: Lz4Allowance::Deny,
            large_file_parallel: false,
            ignores: Arc::from(Vec::<String>::new()),
        },
    )
    .await
}

async fn handle_client_with_transport<T>(
    framed: &mut Framed<T, PxsCodec>,
    dst_root: &Path,
    options: ClientHandlingOptions,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut state = ClientState::new(dst_root);

    let result = handle_client_inner(framed, &mut state, dst_root, options).await;

    if let Err(error) = &result {
        state.cleanup_partial_files();
        // Best effort error reporting to the sender
        let _ = framed
            .send(serialize_message(&Message::Error(format!("{error:#}")))?)
            .await;
    }

    result
}

async fn handle_client_inner<T>(
    framed: &mut Framed<T, PxsCodec>,
    state: &mut ClientState,
    dst_root: &Path,
    options: ClientHandlingOptions,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    while let Some(bytes) = recv_with_timeout(framed).await? {
        let msg = deserialize_message(&bytes)?;
        let should_continue =
            process_client_message(framed, state, dst_root, &options, msg).await?;
        if !should_continue {
            return Ok(());
        }
    }

    // If connection closed while files were still being written, treat as error
    if !state.pending_files.is_empty() {
        anyhow::bail!(
            "Connection closed with {} incomplete file(s)",
            state.pending_files.len()
        );
    }
    if !state.file_transfers.is_empty() {
        anyhow::bail!(
            "Connection closed with {} incomplete transfer state(s)",
            state.file_transfers.len()
        );
    }
    if state.delete_enabled() {
        anyhow::bail!("connection closed before sync completion for delete-enabled session");
    }

    finalize_client_state(
        state,
        dst_root,
        options.ignores.as_ref(),
        state.effective_fsync(options.fsync),
    )?;

    Ok(())
}

fn handle_apply_blocks(
    pending_files: &mut HashMap<String, PendingFile>,
    path: &str,
    blocks: Vec<Block>,
) -> anyhow::Result<()> {
    let pending_file = pending_files
        .get_mut(path)
        .ok_or_else(|| anyhow::anyhow!("missing pending file for path: {path}"))?;
    let file = pending_file
        .file
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("pending file already finalized for path: {path}"))?;
    for block in blocks {
        if let Err(e) = file.write_all_at(&block.data, block.offset) {
            if e.raw_os_error() == Some(nix::libc::ENOSPC) {
                anyhow::bail!("Disk full: not enough space to write to destination");
            }
            return Err(e.into());
        }
    }
    Ok(())
}

async fn handle_apply_metadata<T>(
    framed: &mut Framed<T, PxsCodec>,
    state: &mut ClientState,
    path: String,
    metadata: FileMetadata,
    dst_root: &Path,
    fsync: bool,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let full_path = resolve_protocol_path(dst_root, &path)?;
    let transfer = state.transfer_state(&path)?;
    if let Some(pending_file) = state.pending_files.get_mut(&path) {
        anyhow::ensure!(
            matches!(
                transfer.phase,
                TransferPhase::Content | TransferPhase::Metadata
            ),
            "received metadata out of sequence for path: {path}"
        );
        if let Some(file) = pending_file.file.take() {
            file.set_len(metadata.size)?;
            drop(file);
        }

        apply_file_metadata(pending_file.staged_file.path(), &metadata)?;
        if fsync {
            let staged_handle = std::fs::OpenOptions::new()
                .read(true)
                .open(pending_file.staged_file.path())?;
            staged_handle.sync_all()?;
        }

        if pending_file.checksum {
            state.update_transfer_phase(&path, TransferPhase::Checksum)?;
            framed
                .send(serialize_message(&Message::MetadataApplied { path })?)
                .await?;
            return Ok(());
        }

        let mut pending_file = state
            .pending_files
            .remove(&path)
            .ok_or_else(|| anyhow::anyhow!("missing pending file for path: {path}"))?;
        if let Err(error) = pending_file.staged_file.commit() {
            let _ = cleanup_pending_file(pending_file);
            return Err(error);
        }
        if fsync {
            tools::sync_parent_directory(&full_path)?;
        }
        if let Some(record) = pending_file.parallel_transfer.take() {
            remove_parallel_transfer_record(&record.dir)?;
        }
        state.clear_transfer_state(&path);
        framed
            .send(serialize_message(&Message::MetadataApplied { path })?)
            .await?;
        return Ok(());
    }

    anyhow::ensure!(
        transfer.phase == TransferPhase::Metadata,
        "received metadata without staged file for path: {path}"
    );
    if let Ok(meta) = tokio::fs::symlink_metadata(&full_path).await
        && meta.is_file()
        && meta.len() > metadata.size
    {
        let file = std::fs::OpenOptions::new().write(true).open(&full_path)?;
        file.set_len(metadata.size)?;
        if fsync {
            file.sync_all()?;
        }
    }
    apply_file_metadata(&full_path, &metadata)?;
    if fsync {
        tools::sync_parent_directory(&full_path)?;
    }
    if transfer.checksum {
        state.update_transfer_phase(&path, TransferPhase::Checksum)?;
    } else {
        state.clear_transfer_state(&path);
    }
    framed
        .send(serialize_message(&Message::MetadataApplied { path })?)
        .await?;
    Ok(())
}

async fn handle_verify_checksum<T>(
    framed: &mut Framed<T, PxsCodec>,
    state: &mut ClientState,
    path: &str,
    expected_hash: &[u8; 32],
    dst_root: &Path,
    fsync: bool,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let full_path = resolve_protocol_path(dst_root, path)?;
    let transfer = state.transfer_state(path)?;
    anyhow::ensure!(
        transfer.phase == TransferPhase::Checksum,
        "received checksum verification out of sequence for path: {path}"
    );
    let actual_hash = if let Some(mut pending_file) = state.pending_files.remove(path) {
        let hash = match tools::blake3_file_hash(pending_file.staged_file.path()).await {
            Ok(h) => h,
            Err(error) => {
                let _ = cleanup_pending_file(pending_file);
                return Err(error);
            }
        };

        if &hash == expected_hash {
            if let Err(error) = pending_file.staged_file.commit() {
                let _ = cleanup_pending_file(pending_file);
                return Err(error);
            }
            if fsync {
                tools::sync_parent_directory(&full_path)?;
            }
            if let Some(record) = pending_file.parallel_transfer.take() {
                remove_parallel_transfer_record(&record.dir)?;
            }
        } else {
            cleanup_pending_file(pending_file)?;
            state.clear_transfer_state(path);
            framed
                .send(serialize_message(&Message::ChecksumMismatch {
                    path: path.to_string(),
                })?)
                .await?;
            return Ok(());
        }
        hash
    } else {
        tools::blake3_file_hash(&full_path).await?
    };

    if &actual_hash == expected_hash {
        framed
            .send(serialize_message(&Message::ChecksumVerified {
                path: path.to_string(),
            })?)
            .await?;
    } else {
        framed
            .send(serialize_message(&Message::ChecksumMismatch {
                path: path.to_string(),
            })?)
            .await?;
    }
    state.clear_transfer_state(path);
    Ok(())
}
