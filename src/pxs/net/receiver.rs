use super::{
    BLOCK_SIZE,
    codec::PxsCodec,
    path::{resolve_protocol_path, validate_protocol_path},
    protocol::{
        Block, FileMetadata, Message, apply_file_metadata, deserialize_message, serialize_message,
    },
    shared::{block_bytes, skipped_bytes},
};
use crate::pxs::tools;
use futures_util::{SinkExt, StreamExt};
use indicatif::ProgressBar;
use std::{
    collections::HashMap,
    fmt::Write as _,
    os::unix::fs::{FileExt, MetadataExt},
    path::Path,
    process::Stdio,
    sync::Arc,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    process::Command,
};
use tokio_util::codec::Framed;

struct ClientState {
    open_files: HashMap<String, std::fs::File>,
    progress: Option<Arc<ProgressBar>>,
    current_metadata: Option<FileMetadata>,
    dir_metadata: Vec<(String, FileMetadata)>,
}

impl ClientState {
    fn new() -> Self {
        Self {
            open_files: HashMap::new(),
            progress: None,
            current_metadata: None,
            dir_metadata: Vec::new(),
        }
    }
}

/// Run the client in pull mode (connects to a server to receive files).
///
/// # Errors
///
/// Returns an error if the connection fails or synchronization fails.
pub async fn run_pull_client(addr: &str, dst_root: &Path, fsync: bool) -> anyhow::Result<()> {
    let stream = TcpStream::connect(addr).await?;
    let mut framed = Framed::new(stream, PxsCodec);
    handle_client(&mut framed, dst_root, true, fsync).await
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
            if let Err(error) = handle_client(&mut framed, &dst_root, false, fsync).await {
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
pub async fn run_stdio_receiver(dst_root: &Path, fsync: bool) -> anyhow::Result<()> {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    let combined = tokio::io::join(stdin, stdout);
    let mut framed = Framed::new(combined, PxsCodec);
    handle_client(&mut framed, dst_root, false, fsync).await
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
    let mut cmd = Command::new("ssh");
    cmd.arg("-q")
        .arg("-o")
        .arg("Compression=no")
        .arg("-o")
        .arg("Ciphers=aes128-gcm@openssh.com,chacha20-poly1305@openssh.com,aes128-ctr")
        .arg("-o")
        .arg("IPQoS=throughput")
        .arg(addr);

    let mut remote_cmd =
        format!("pxs --stdio --sender --source {src_path} --threshold {threshold}");
    if checksum {
        remote_cmd.push_str(" --checksum");
    }
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
        handle_client(&mut framed, dst_root, true, fsync).await?;
    }

    let status = child.wait().await?;
    if !status.success() {
        anyhow::bail!("SSH process exited with error: {status}");
    }
    Ok(())
}

async fn handle_handshake<T>(
    framed: &mut Framed<T, PxsCodec>,
    _version: String,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let response = Message::Handshake {
        version: env!("CARGO_PKG_VERSION").to_string(),
    };
    framed.send(serialize_message(&response)?).await?;
    Ok(())
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
    path: String,
    metadata: FileMetadata,
    threshold: f32,
    checksum: bool,
    dst_root: &Path,
    progress: Option<&Arc<ProgressBar>>,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    tracing::debug!("Syncing file: {path} ({} bytes)", metadata.size);
    let full_path = resolve_protocol_path(dst_root, &path)?;

    if full_path.exists() {
        if full_path.is_dir() {
            tokio::fs::remove_dir_all(&full_path).await?;
        } else {
            let meta = tokio::fs::metadata(&full_path).await?;
            if meta.len() == metadata.size
                && !checksum
                && meta.mtime() == metadata.mtime
                && u32::try_from(meta.mtime_nsec()).ok() == Some(metadata.mtime_nsec)
            {
                if let Some(progress) = progress {
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
                if let Some(progress) = progress {
                    progress.set_message(format!("Full copy: {path}"));
                }
                framed
                    .send(serialize_message(&Message::RequestFullCopy { path })?)
                    .await?;
                return Ok(());
            }
        }
    }

    if !full_path.exists() {
        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tools::preallocate(&full_path, metadata.size)?;

        if let Some(progress) = progress {
            progress.set_message(format!("Full copy: {path}"));
        }
        framed
            .send(serialize_message(&Message::RequestFullCopy { path })?)
            .await?;
        return Ok(());
    }

    if let Some(progress) = progress {
        progress.set_message(format!("Hashing: {path}"));
    }
    framed
        .send(serialize_message(&Message::RequestHashes { path })?)
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
    if !full_path.exists() {
        tokio::fs::create_dir_all(&full_path).await?;
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
    if let Some(progress) = &state.progress {
        progress.set_message(path.clone());
    }

    handle_sync_file(
        framed,
        path,
        metadata,
        threshold,
        checksum,
        dst_root,
        state.progress.as_ref(),
    )
    .await
}

async fn handle_block_hashes_message<T>(
    framed: &mut Framed<T, PxsCodec>,
    state: &mut ClientState,
    dst_root: &Path,
    path: String,
    hashes: Vec<u64>,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let full_path = resolve_protocol_path(dst_root, &path)?;
    let requested = tools::compute_requested_blocks(&full_path, &hashes, BLOCK_SIZE)?;

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
    dst_root: &Path,
    path: &str,
    blocks: Vec<Block>,
) -> anyhow::Result<()> {
    if let Some(progress) = &state.progress {
        progress.inc(block_bytes(&blocks)?);
    }

    handle_apply_blocks(&mut state.open_files, path, blocks, dst_root)
}

fn handle_end_of_file_message(state: &mut ClientState, path: &str) -> anyhow::Result<()> {
    validate_protocol_path(path)?;
    state.open_files.remove(path);
    Ok(())
}

async fn process_client_message<T>(
    framed: &mut Framed<T, PxsCodec>,
    state: &mut ClientState,
    dst_root: &Path,
    show_progress: bool,
    fsync: bool,
    msg: Message,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    match msg {
        Message::Handshake { version } => {
            tracing::debug!("Client connected (version: {version})");
            handle_handshake(framed, version).await?;
        }
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
            handle_block_hashes_message(framed, state, dst_root, path, hashes).await?;
        }
        Message::ApplyBlocks { path, blocks } => {
            handle_apply_blocks_message(state, dst_root, &path, blocks)?;
        }
        Message::ApplyMetadata { path, metadata } => {
            handle_apply_metadata(
                framed,
                &mut state.open_files,
                path,
                metadata,
                dst_root,
                fsync,
            )
            .await?;
        }
        Message::EndOfFile { path } => {
            handle_end_of_file_message(state, &path)?;
        }
        _ => eprintln!("Unhandled message: {msg:?}"),
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
    let mut state = ClientState::new();

    while let Some(result) = framed.next().await {
        let bytes = result?;
        let msg = deserialize_message(&bytes)?;
        process_client_message(framed, &mut state, dst_root, show_progress, fsync, msg).await?;
    }

    finalize_directory_metadata(dst_root, state.dir_metadata);
    if let Some(progress) = state.progress {
        progress.finish_with_message("Done");
    }

    Ok(())
}

fn handle_apply_blocks(
    open_files: &mut HashMap<String, std::fs::File>,
    path: &str,
    blocks: Vec<Block>,
    dst_root: &Path,
) -> anyhow::Result<()> {
    if !open_files.contains_key(path) {
        let full_path = resolve_protocol_path(dst_root, path)?;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(full_path)?;
        open_files.insert(path.to_string(), file);
    }

    let file = open_files
        .get(path)
        .ok_or_else(|| anyhow::anyhow!("missing open file entry for path: {path}"))?;
    for block in blocks {
        file.write_all_at(&block.data, block.offset)?;
    }
    Ok(())
}

async fn handle_apply_metadata<T>(
    framed: &mut Framed<T, PxsCodec>,
    open_files: &mut HashMap<String, std::fs::File>,
    path: String,
    metadata: FileMetadata,
    dst_root: &Path,
    fsync: bool,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let full_path = resolve_protocol_path(dst_root, &path)?;
    if let Some(file) = open_files.remove(&path) {
        let _ = file.set_len(metadata.size);
        if fsync {
            let _ = file.sync_all();
        }
    } else if full_path.exists() {
        let meta = tokio::fs::metadata(&full_path).await?;
        if meta.len() > metadata.size {
            let file = std::fs::OpenOptions::new().write(true).open(&full_path)?;
            let _ = file.set_len(metadata.size);
            if fsync {
                let _ = file.sync_all();
            }
        }
    }
    apply_file_metadata(&full_path, &metadata)?;
    framed
        .send(serialize_message(&Message::MetadataApplied { path })?)
        .await?;
    Ok(())
}
