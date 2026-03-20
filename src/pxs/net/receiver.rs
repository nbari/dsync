use super::{
    BLOCK_SIZE,
    codec::PxsCodec,
    path::{resolve_protocol_path, validate_protocol_path},
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
    collections::HashMap,
    os::unix::fs::{FileExt, MetadataExt},
    path::{Path, PathBuf},
    sync::Arc,
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
}

struct ClientState {
    pending_files: HashMap<String, PendingFile>,
    progress: Option<Arc<ProgressBar>>,
    current_metadata: Option<FileMetadata>,
    dir_metadata: Vec<(String, FileMetadata)>,
    dst_root: PathBuf,
    transport: TransportFeatures,
    handshake_complete: bool,
}

impl ClientState {
    fn new(dst_root: &Path) -> Self {
        Self {
            pending_files: HashMap::new(),
            progress: None,
            current_metadata: None,
            dir_metadata: Vec::new(),
            dst_root: dst_root.to_path_buf(),
            transport: TransportFeatures::default(),
            handshake_complete: false,
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
            let _ = existing.staged_file.cleanup();
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
            },
        );
        Ok(())
    }

    /// Clean up partial files on transfer failure.
    fn cleanup_partial_files(&mut self) {
        for (path, pending_file) in self.pending_files.drain() {
            drop(pending_file.file);
            if let Err(e) = pending_file.staged_file.cleanup()
                && let Ok(full_path) = resolve_protocol_path(&self.dst_root, &path)
            {
                eprintln!(
                    "Warning: failed to remove partial file {}: {e}",
                    full_path.display(),
                );
            }
        }
    }
}

/// Run the client in pull mode (connects to a server to receive files).
///
/// # Errors
///
/// Returns an error if the connection fails or synchronization fails.
pub async fn run_pull_client(addr: &str, dst_root: &Path, fsync: bool) -> anyhow::Result<()> {
    let stream = connect_with_retry(addr).await?;
    let mut framed = Framed::new(stream, PxsCodec);
    handle_client_with_transport(&mut framed, dst_root, true, fsync, true).await
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
            if let Err(error) =
                handle_client_with_transport(&mut framed, &dst_root, false, fsync, true).await
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
pub async fn run_stdio_receiver(dst_root: &Path, fsync: bool, _quiet: bool) -> anyhow::Result<()> {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    let combined = tokio::io::join(stdin, stdout);
    let mut framed = Framed::new(combined, PxsCodec);
    handle_client_with_transport(&mut framed, dst_root, false, fsync, false).await
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
    threshold: f32,
    checksum: bool,
    fsync: bool,
    ignores: &[String],
) -> anyhow::Result<()> {
    let remote_cmd = build_ssh_pull_command(src_path, threshold, checksum, ignores);
    let mut session = ChildSession::spawn(build_ssh_command(addr, &remote_cmd))?;
    let result =
        handle_client_with_transport(session.framed_mut()?, dst_root, true, fsync, false).await;
    session.finish(result, "SSH process").await?;
    Ok(())
}

async fn handle_handshake<T>(
    framed: &mut Framed<T, PxsCodec>,
    version: String,
    allow_lz4: bool,
) -> anyhow::Result<TransportFeatures>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let features = match negotiate_transport_features(&version, allow_lz4) {
        Ok(features) => features,
        Err(error) => {
            framed
                .send(serialize_message(&Message::Error(error.to_string()))?)
                .await?;
            return Err(error);
        }
    };
    let response = Message::Handshake {
        version: local_handshake_version(allow_lz4),
    };
    framed.send(serialize_message(&response)?).await?;
    Ok(features)
}

async fn handle_sync_symlink(
    path: String,
    target: String,
    metadata: FileMetadata,
    dst_root: &Path,
) -> anyhow::Result<()> {
    let full_path = resolve_protocol_path(dst_root, &path)?;
    if full_path.exists() {
        if full_path.is_dir() {
            tokio::fs::remove_dir_all(&full_path).await?;
        } else {
            tokio::fs::remove_file(&full_path).await?;
        }
    }
    if let Some(parent) = full_path.parent()
        && !parent.exists()
    {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::symlink(target, &full_path).await?;
    apply_file_metadata(&full_path, &metadata)?;
    Ok(())
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
                    .send(serialize_message(&Message::EndOfFile { path })?)
                    .await?;
                return Ok(());
            }

            if metadata.size > 0
                && tools::should_use_full_copy_meta(metadata.size, &full_path, threshold).await?
            {
                state.prepare_pending_file(&path, &full_path, metadata.size, false, checksum)?;
                if let Some(progress) = &progress {
                    progress.set_message(format!("Full copy: {path}"));
                }
                framed
                    .send(serialize_message(&Message::RequestFullCopy { path })?)
                    .await?;
                return Ok(());
            }

            state.prepare_pending_file(&path, &full_path, metadata.size, true, checksum)?;
            if let Some(progress) = &progress {
                progress.set_message(format!("Hashing: {path}"));
            }
            framed
                .send(serialize_message(&Message::RequestHashes { path })?)
                .await?;
            return Ok(());
        }

        state.prepare_pending_file(&path, &full_path, metadata.size, false, checksum)?;
        if let Some(progress) = &progress {
            progress.set_message(format!("Full copy: {path}"));
        }
        framed
            .send(serialize_message(&Message::RequestFullCopy { path })?)
            .await?;
        return Ok(());
    }

    state.prepare_pending_file(&path, &full_path, metadata.size, false, checksum)?;
    if let Some(progress) = &progress {
        progress.set_message(format!("Full copy: {path}"));
    }
    framed
        .send(serialize_message(&Message::RequestFullCopy { path })?)
        .await?;
    Ok(())
}

async fn handle_sync_dir_message(
    state: &mut ClientState,
    dst_root: &Path,
    path: String,
    metadata: FileMetadata,
) -> anyhow::Result<()> {
    let full_path = resolve_protocol_path(dst_root, &path)?;
    match tokio::fs::symlink_metadata(&full_path).await {
        Ok(existing) if existing.is_dir() => {}
        Ok(_) => {
            tokio::fs::remove_file(&full_path).await?;
            tokio::fs::create_dir_all(&full_path).await?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            tokio::fs::create_dir_all(&full_path).await?;
        }
        Err(error) => return Err(error.into()),
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
    state.current_metadata = Some(metadata);
    let progress = state.progress.clone();
    if let Some(progress) = &progress {
        progress.set_message(path.clone());
    }

    handle_sync_file(framed, state, path, metadata, threshold, checksum, dst_root).await
}

async fn handle_block_hashes_message<T>(
    framed: &mut Framed<T, PxsCodec>,
    state: &mut ClientState,
    path: String,
    hashes: Vec<u64>,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let pending_file = state
        .pending_files
        .get(&path)
        .ok_or_else(|| anyhow::anyhow!("missing pending file for path: {path}"))?;
    let requested =
        tools::compute_requested_blocks(pending_file.staged_file.path(), &hashes, BLOCK_SIZE)?;

    if let Some(progress) = &state.progress
        && let Some(metadata) = state.current_metadata
    {
        progress.inc(skipped_bytes(metadata, &requested));
    }

    if requested.is_empty() {
        framed
            .send(serialize_message(&Message::EndOfFile { path })?)
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
    if let Some(mut pending_file) = state.pending_files.remove(path) {
        drop(pending_file.file.take());
        pending_file.staged_file.cleanup()?;
    }
    Ok(())
}

async fn process_client_message<T>(
    framed: &mut Framed<T, PxsCodec>,
    state: &mut ClientState,
    dst_root: &Path,
    show_progress: bool,
    fsync: bool,
    allow_lz4: bool,
    msg: Message,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    if !state.handshake_complete {
        return match msg {
            Message::Handshake { version } => {
                tracing::debug!("Client connected (version: {version})");
                state.transport = handle_handshake(framed, version, allow_lz4).await?;
                state.handshake_complete = true;
                Ok(())
            }
            other => anyhow::bail!("expected handshake before transfer messages, got {other:?}"),
        };
    }

    match msg {
        Message::Handshake { .. } => anyhow::bail!("duplicate handshake message"),
        Message::SyncStart { total_size } => {
            tracing::debug!("Starting sync (total size: {total_size} bytes)");
            if show_progress && total_size > 0 {
                state.progress = Some(Arc::new(tools::create_progress_bar(total_size)));
            }
        }
        Message::SyncDir { path, metadata } => {
            handle_sync_dir_message(state, dst_root, path, metadata).await?;
        }
        Message::SyncSymlink {
            path,
            target,
            metadata,
        } => handle_sync_symlink(path, target, metadata, dst_root).await?,
        Message::SyncFile {
            path,
            metadata,
            threshold,
            checksum,
        } => {
            handle_sync_file_message(framed, state, path, metadata, threshold, checksum, dst_root)
                .await?;
        }
        Message::BlockHashes { path, hashes } => {
            handle_block_hashes_message(framed, state, path, hashes).await?;
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
            handle_apply_metadata(framed, state, path, metadata, dst_root, fsync).await?;
        }
        Message::VerifyChecksum { path, hash } => {
            handle_verify_checksum(framed, state, &path, &hash, dst_root, fsync).await?;
        }
        Message::EndOfFile { path } => {
            handle_end_of_file_message(state, &path)?;
        }
        Message::RequestFullCopy { .. }
        | Message::RequestHashes { .. }
        | Message::RequestBlocks { .. }
        | Message::MetadataApplied { .. }
        | Message::ChecksumVerified { .. }
        | Message::ChecksumMismatch { .. }
        | Message::Error(_) => {
            anyhow::bail!("unexpected receiver-side protocol message: {msg:?}");
        }
    }

    Ok(())
}

fn finalize_directory_metadata(dst_root: &Path, mut dir_metadata: Vec<(String, FileMetadata)>) {
    dir_metadata.sort_by_key(|(path, _)| std::cmp::Reverse(path.split('/').count()));
    for (path, metadata) in dir_metadata {
        let full_path = match resolve_protocol_path(dst_root, &path) {
            Ok(full_path) => full_path,
            Err(error) => {
                eprintln!("Warning: failed to resolve protocol path {path}: {error}");
                continue;
            }
        };

        if let Err(error) = apply_file_metadata(&full_path, &metadata) {
            eprintln!(
                "Warning: failed to apply metadata to {}: {}",
                full_path.display(),
                error
            );
        }
    }
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
    handle_client_with_transport(framed, dst_root, show_progress, fsync, false).await
}

async fn handle_client_with_transport<T>(
    framed: &mut Framed<T, PxsCodec>,
    dst_root: &Path,
    show_progress: bool,
    fsync: bool,
    allow_lz4: bool,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut state = ClientState::new(dst_root);

    let result = handle_client_inner(
        framed,
        &mut state,
        dst_root,
        show_progress,
        fsync,
        allow_lz4,
    )
    .await;

    if result.is_err() {
        state.cleanup_partial_files();
    }

    result
}

async fn handle_client_inner<T>(
    framed: &mut Framed<T, PxsCodec>,
    state: &mut ClientState,
    dst_root: &Path,
    show_progress: bool,
    fsync: bool,
    allow_lz4: bool,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    while let Some(bytes) = recv_with_timeout(framed).await? {
        let msg = deserialize_message(&bytes)?;
        process_client_message(
            framed,
            state,
            dst_root,
            show_progress,
            fsync,
            allow_lz4,
            msg,
        )
        .await?;
    }

    // If connection closed while files were still being written, treat as error
    if !state.pending_files.is_empty() {
        anyhow::bail!(
            "Connection closed with {} incomplete file(s)",
            state.pending_files.len()
        );
    }

    finalize_directory_metadata(dst_root, std::mem::take(&mut state.dir_metadata));
    if let Some(progress) = state.progress.take() {
        progress.finish_with_message("Done");
    }

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
    if let Some(pending_file) = state.pending_files.get_mut(&path) {
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
            let _ = pending_file.staged_file.cleanup();
            return Err(error);
        }
        if fsync {
            tools::sync_parent_directory(&full_path)?;
        }
        framed
            .send(serialize_message(&Message::MetadataApplied { path })?)
            .await?;
        return Ok(());
    }

    if full_path.exists() {
        let meta = tokio::fs::metadata(&full_path).await?;
        if meta.len() > metadata.size {
            let file = std::fs::OpenOptions::new().write(true).open(&full_path)?;
            file.set_len(metadata.size)?;
            if fsync {
                file.sync_all()?;
            }
        }
    }
    apply_file_metadata(&full_path, &metadata)?;
    if fsync {
        tools::sync_parent_directory(&full_path)?;
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
    let mut pending_file = state
        .pending_files
        .remove(path)
        .ok_or_else(|| anyhow::anyhow!("missing pending file for path: {path}"))?;
    let actual_hash = match tools::blake3_file_hash(pending_file.staged_file.path()).await {
        Ok(hash) => hash,
        Err(error) => {
            let _ = pending_file.staged_file.cleanup();
            return Err(error);
        }
    };

    if &actual_hash == expected_hash {
        if let Err(error) = pending_file.staged_file.commit() {
            let _ = pending_file.staged_file.cleanup();
            return Err(error);
        }
        if fsync {
            tools::sync_parent_directory(&full_path)?;
        }
        framed
            .send(serialize_message(&Message::ChecksumVerified {
                path: path.to_string(),
            })?)
            .await?;
    } else {
        pending_file.staged_file.cleanup()?;
        framed
            .send(serialize_message(&Message::ChecksumMismatch {
                path: path.to_string(),
            })?)
            .await?;
    }
    Ok(())
}
