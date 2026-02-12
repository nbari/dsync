use crate::dsync::tools;
use bytes::{Buf, BufMut, BytesMut};
use filetime::FileTime;
use futures_util::{SinkExt, StreamExt};
use indicatif::ProgressBar;
use rkyv::{
    api::high::to_bytes_in,
    util::AlignedVec,
    {Archive, Deserialize, Serialize},
};
use std::{
    collections::HashMap,
    fmt::Write as _,
    os::unix::fs::{FileExt, MetadataExt},
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    process::Command,
};
use tokio_util::codec::{Decoder, Encoder, Framed};

const BLOCK_SIZE: u64 = 64 * 1024;

#[derive(Archive, Deserialize, Serialize, Debug, Clone, Copy)]
pub struct FileMetadata {
    pub size: u64,
    pub mtime: i64,
    pub mtime_nsec: u32,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
}

impl From<std::fs::Metadata> for FileMetadata {
    fn from(meta: std::fs::Metadata) -> Self {
        Self {
            size: meta.len(),
            mtime: meta.mtime(),
            mtime_nsec: u32::try_from(meta.mtime_nsec()).unwrap_or(0),
            mode: meta.mode(),
            uid: meta.uid(),
            gid: meta.gid(),
        }
    }
}

#[derive(Archive, Deserialize, Serialize, Debug)]
pub enum Message {
    Handshake {
        version: String,
    },
    SyncDir {
        path: String,
        metadata: FileMetadata,
    },
    SyncSymlink {
        path: String,
        target: String,
        metadata: FileMetadata,
    },
    SyncFile {
        path: String,
        metadata: FileMetadata,
        threshold: f32,
        checksum: bool,
    },
    RequestFullCopy {
        path: String,
    },
    RequestHashes {
        path: String,
    },
    BlockHashes {
        path: String,
        hashes: Vec<u64>,
    },
    ApplyBlocks {
        path: String,
        blocks: Vec<Block>,
    },
    ApplyMetadata {
        path: String,
        metadata: FileMetadata,
    },
    MetadataApplied {
        path: String,
    },
    EndOfFile {
        path: String,
    },
    RequestBlocks {
        path: String,
        indices: Vec<u32>,
    },
    Error(String),
}

#[derive(Archive, Deserialize, Serialize, Debug)]
pub struct Block {
    pub offset: u64,
    pub data: Vec<u8>,
}

/// Serialize a message to a byte vector.
///
/// # Errors
///
/// Returns an error if serialization fails.
pub fn serialize_message(msg: &Message) -> anyhow::Result<Vec<u8>> {
    let mut vec = AlignedVec::<16>::new();
    to_bytes_in::<_, rkyv::rancor::Error>(msg, &mut vec)
        .map_err(|e| anyhow::anyhow!("failed to serialize message: {e}"))?;
    Ok(vec.to_vec())
}

/// Deserialize a message from a byte slice.
///
/// # Errors
///
/// Returns an error if deserialization fails.
pub fn deserialize_message(bytes: &[u8]) -> anyhow::Result<Message> {
    let mut aligned = AlignedVec::<16>::new();
    aligned.extend_from_slice(bytes);
    rkyv::from_bytes::<Message, rkyv::rancor::Error>(&aligned)
        .map_err(|e| anyhow::anyhow!("failed to deserialize message: {e}"))
}

/// Apply metadata to a local path.
///
/// # Errors
///
/// Returns an error if any attribute fails to be applied.
pub fn apply_file_metadata(path: &Path, metadata: &FileMetadata) -> anyhow::Result<()> {
    use std::fs::Permissions;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, Permissions::from_mode(metadata.mode))?;
    let _ = nix::unistd::chown(
        path,
        Some(nix::unistd::Uid::from_raw(metadata.uid)),
        Some(nix::unistd::Gid::from_raw(metadata.gid)),
    );
    let mtime = FileTime::from_unix_time(metadata.mtime, metadata.mtime_nsec);
    filetime::set_file_times(path, mtime, mtime)?;
    Ok(())
}

const MAGIC: &[u8; 4] = b"DSYC";

pub struct DsyncCodec;

impl Decoder for DsyncCodec {
    type Item = Vec<u8>;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            if src.len() < 8 {
                return Ok(None);
            }
            if src.get(0..4) != Some(MAGIC) {
                if let Some(pos) = memchr::memchr(MAGIC[0], src) {
                    src.advance(pos);
                    if src.len() < 4 {
                        return Ok(None);
                    }
                    if src.get(0..4) != Some(MAGIC) {
                        src.advance(1);
                        continue;
                    }
                } else {
                    src.clear();
                    return Ok(None);
                }
            }
            let mut len_bytes = [0u8; 4];
            if let Some(bytes) = src.get(4..8) {
                len_bytes.copy_from_slice(bytes);
            } else {
                return Ok(None);
            }
            let len = u32::from_be_bytes(len_bytes) as usize;
            if len > 64 * 1024 * 1024 {
                return Err(anyhow::anyhow!("Frame size too big: {len}"));
            }
            if src.len() < 8 + len {
                return Ok(None);
            }
            src.advance(8);
            let data = src.split_to(len).to_vec();
            return Ok(Some(data));
        }
    }
}

impl Encoder<Vec<u8>> for DsyncCodec {
    type Error = anyhow::Error;
    fn encode(&mut self, item: Vec<u8>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if item.len() > 64 * 1024 * 1024 {
            return Err(anyhow::anyhow!("Frame size too big: {}", item.len()));
        }
        dst.reserve(8 + item.len());
        dst.put_slice(MAGIC);
        #[allow(clippy::cast_possible_truncation)]
        dst.put_u32(item.len() as u32);
        dst.put_slice(&item);
        Ok(())
    }
}

/// Run the receiver to handle incoming sync connections.
///
/// # Errors
///
/// Returns an error if the listener fails to bind or synchronization fails.
pub async fn run_receiver(addr: &str, dst_root: &Path) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    eprintln!("Receiver listening on {addr}");
    while let Ok((stream, peer_addr)) = listener.accept().await {
        eprintln!("Accepted connection from {peer_addr}");
        let dst_root = dst_root.to_path_buf();
        tokio::spawn(async move {
            let mut framed = Framed::new(stream, DsyncCodec);
            if let Err(e) = handle_client(&mut framed, &dst_root).await {
                eprintln!("Error handling client {peer_addr}: {e}");
            }
            eprintln!("Connection with {peer_addr} closed");
        });
    }
    Ok(())
}

/// Run the receiver using stdin/stdout (for SSH tunnels)
///
/// # Errors
///
/// Returns an error if synchronization fails.
pub async fn run_stdio_receiver(dst_root: &Path) -> anyhow::Result<()> {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    let combined = tokio::io::join(stdin, stdout);
    let mut framed = Framed::new(combined, DsyncCodec);
    handle_client(&mut framed, dst_root).await
}

async fn handle_handshake<T>(
    framed: &mut Framed<T, DsyncCodec>,
    version: String,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    tracing::info!("Client connected (version: {version})");
    let resp = Message::Handshake {
        version: env!("CARGO_PKG_VERSION").to_string(),
    };
    framed.send(serialize_message(&resp)?).await?;
    Ok(())
}

async fn handle_sync_dir(
    path: String,
    metadata: FileMetadata,
    dst_root: &Path,
) -> anyhow::Result<()> {
    let full_path = dst_root.join(&path);
    if !full_path.exists() {
        tokio::fs::create_dir_all(&full_path).await?;
    }
    apply_file_metadata(&full_path, &metadata)?;
    Ok(())
}

async fn handle_sync_symlink(
    path: String,
    target: String,
    metadata: FileMetadata,
    dst_root: &Path,
) -> anyhow::Result<()> {
    let full_path = dst_root.join(&path);
    if full_path.exists() {
        tokio::fs::remove_file(&full_path).await?;
    }
    tokio::fs::symlink(target, &full_path).await?;
    apply_file_metadata(&full_path, &metadata)?;
    Ok(())
}

async fn handle_sync_file<T>(
    framed: &mut Framed<T, DsyncCodec>,
    path: String,
    metadata: FileMetadata,
    threshold: f32,
    checksum: bool,
    dst_root: &Path,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    tracing::info!("Syncing file: {path} ({} bytes)", metadata.size);
    let full_path = dst_root.join(&path);

    if full_path.exists() {
        let meta = tokio::fs::metadata(&full_path).await?;
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        if meta.len() == metadata.size
            && !checksum
            && meta.mtime() == metadata.mtime
            && (meta.mtime_nsec() as u32) == metadata.mtime_nsec
        {
            framed
                .send(serialize_message(&Message::EndOfFile { path })?)
                .await?;
            return Ok(());
        }

        if metadata.size > 0 && tools::is_below_threshold(metadata.size, meta.len(), threshold) {
            framed
                .send(serialize_message(&Message::RequestFullCopy { path })?)
                .await?;
            return Ok(());
        }
    } else if let Some(parent) = full_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
        framed
            .send(serialize_message(&Message::RequestFullCopy { path })?)
            .await?;
        return Ok(());
    }

    framed
        .send(serialize_message(&Message::RequestHashes { path })?)
        .await?;
    Ok(())
}

async fn handle_block_hashes<T>(
    framed: &mut Framed<T, DsyncCodec>,
    path: String,
    hashes: Vec<u64>,
    dst_root: &Path,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let full_path = dst_root.join(&path);
    let requested = tools::compute_requested_blocks(&full_path, &hashes, BLOCK_SIZE)?;

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

fn handle_apply_blocks(
    open_files: &mut HashMap<String, std::fs::File>,
    path: &str,
    blocks: Vec<Block>,
    dst_root: &Path,
) -> anyhow::Result<()> {
    if !open_files.contains_key(path) {
        let full_path = dst_root.join(path);
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
    framed: &mut Framed<T, DsyncCodec>,
    open_files: &mut HashMap<String, std::fs::File>,
    path: String,
    metadata: FileMetadata,
    dst_root: &Path,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let full_path = dst_root.join(&path);
    open_files.remove(&path);
    apply_file_metadata(&full_path, &metadata)?;
    framed
        .send(serialize_message(&Message::MetadataApplied { path })?)
        .await?;
    Ok(())
}

/// Handle a client connection.
///
/// # Errors
///
/// Returns an error if synchronization or network I/O fails.
pub async fn handle_client<T>(
    framed: &mut Framed<T, DsyncCodec>,
    dst_root: &Path,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut open_files: HashMap<String, std::fs::File> = HashMap::new();
    while let Some(result) = framed.next().await {
        let bytes = result?;
        let msg = deserialize_message(&bytes)?;
        match msg {
            Message::Handshake { version } => handle_handshake(framed, version).await?,
            Message::SyncDir { path, metadata } => {
                handle_sync_dir(path, metadata, dst_root).await?;
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
            } => handle_sync_file(framed, path, metadata, threshold, checksum, dst_root).await?,
            Message::BlockHashes { path, hashes } => {
                handle_block_hashes(framed, path, hashes, dst_root).await?;
            }
            Message::ApplyBlocks { path, blocks } => {
                handle_apply_blocks(&mut open_files, &path, blocks, dst_root)?;
            }
            Message::ApplyMetadata { path, metadata } => {
                handle_apply_metadata(framed, &mut open_files, path, metadata, dst_root).await?;
            }
            Message::EndOfFile { path } => {
                open_files.remove(&path);
            }
            _ => eprintln!("Unhandled message: {msg:?}"),
        }
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
    let pb = Arc::new(tools::create_progress_bar(total_size));

    let has_non_file_tasks = tasks
        .iter()
        .any(|task| !matches!(task, SyncTask::File { .. }));
    if has_non_file_tasks {
        let stream = TcpStream::connect(addr).await?;
        let mut framed = Framed::new(stream, DsyncCodec);
        sender_handshake(&mut framed).await?;
        for task in &tasks {
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
    }

    let file_paths: Vec<String> = tasks
        .iter()
        .filter_map(|task| match task {
            SyncTask::File { path } => Some(path.clone()),
            _ => None,
        })
        .collect();

    if file_paths.is_empty() {
        pb.finish_with_message("Done");
        return Ok(());
    }

    let worker_count = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(4)
        .clamp(1, 16);
    let worker_count = std::cmp::min(worker_count, file_paths.len());
    let src_root_owned = src_root.to_path_buf();
    let mut batches = vec![Vec::new(); worker_count];
    for (index, rel_path) in file_paths.into_iter().enumerate() {
        #[allow(clippy::indexing_slicing)]
        batches[index % worker_count].push(rel_path);
    }

    let mut workers = Vec::with_capacity(worker_count);
    for batch in batches {
        let addr_owned = addr.to_string();
        let src_root_worker = src_root_owned.clone();
        let pb_worker = Arc::clone(&pb);

        workers.push(tokio::spawn(async move {
            let stream = TcpStream::connect(&addr_owned).await?;
            let mut framed = Framed::new(stream, DsyncCodec);
            sender_handshake(&mut framed).await?;

            for rel_path in batch {
                let src_path = source_path_for(&src_root_worker, &rel_path);
                sync_remote_file(
                    &mut framed,
                    &src_root_worker,
                    &src_path,
                    threshold,
                    checksum,
                    Arc::clone(&pb_worker),
                )
                .await?;
            }

            Ok::<(), anyhow::Error>(())
        }));
    }

    for worker in workers {
        worker.await??;
    }

    pb.finish_with_message("Done");
    Ok(())
}

/// Run the sender using stdin/stdout (for manual piping)
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
    let pb = Arc::new(tools::create_progress_bar(total_size));
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    let combined = tokio::io::join(stdin, stdout);
    let mut framed = Framed::new(combined, DsyncCodec);
    sender_loop(&mut framed, src_root, threshold, checksum, &tasks, pb).await
}

/// Run the sender over an SSH tunnel
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
    let pb = Arc::new(tools::create_progress_bar(total_size));
    let mut cmd = Command::new("ssh");
    cmd.arg("-q")
        .arg("-o")
        .arg("Compression=no")
        .arg("-o")
        .arg("Ciphers=aes128-gcm@openssh.com,chacha20-poly1305@openssh.com,aes128-ctr")
        .arg("-o")
        .arg("IPQoS=throughput")
        .arg(addr);
    let mut remote_cmd = format!("dsync --stdio --destination {dst_path}");
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
        let mut framed = Framed::new(combined, DsyncCodec);
        sender_loop(
            &mut framed,
            src_root,
            threshold,
            checksum,
            &tasks,
            Arc::clone(&pb),
        )
        .await?;
    }
    let status = child.wait().await?;
    if !status.success() {
        anyhow::bail!("SSH process exited with error: {status}");
    }
    pb.finish_with_message("Done");
    Ok(())
}

async fn sender_handshake<T>(framed: &mut Framed<T, DsyncCodec>) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let handshake = Message::Handshake {
        version: env!("CARGO_PKG_VERSION").to_string(),
    };
    framed.send(serialize_message(&handshake)?).await?;
    let resp_bytes = framed
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("Connection closed during handshake"))??;
    let _resp = deserialize_message(&resp_bytes)?;
    Ok(())
}

async fn sender_loop<T>(
    framed: &mut Framed<T, DsyncCodec>,
    src_root: &Path,
    threshold: f32,
    checksum: bool,
    tasks: &[SyncTask],
    pb: Arc<ProgressBar>,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    sender_handshake(framed).await?;

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
                    Arc::clone(&pb),
                )
                .await?;
            }
        }
    }

    Ok(())
}

#[derive(Clone)]
enum SyncTask {
    Dir {
        path: String,
        metadata: FileMetadata,
    },
    Symlink {
        path: String,
        target: String,
        metadata: FileMetadata,
    },
    File {
        path: String,
    },
}

fn build_overrides(
    src_root: &Path,
    ignores: &[String],
) -> anyhow::Result<ignore::overrides::Override> {
    use ignore::overrides::OverrideBuilder;

    let mut override_builder = OverrideBuilder::new(src_root);
    for pattern in ignores {
        override_builder.add(&format!("!{pattern}"))?;
    }
    Ok(override_builder.build()?)
}

fn source_path_for(src_root: &Path, rel_path: &str) -> PathBuf {
    if src_root.is_file() {
        src_root.to_path_buf()
    } else {
        src_root.join(rel_path)
    }
}

fn collect_sync_tasks_sync(
    src_root: &Path,
    ignores: &[String],
) -> anyhow::Result<(Vec<SyncTask>, u64)> {
    use ignore::WalkBuilder;

    let mut tasks = Vec::new();
    let mut total_size = 0_u64;

    if src_root.is_file() {
        let metadata = FileMetadata::from(std::fs::metadata(src_root)?);
        let path = src_root
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .ok_or_else(|| anyhow::anyhow!("source file has no name: {}", src_root.display()))?;
        total_size = metadata.size;
        tasks.push(SyncTask::File { path });
        return Ok((tasks, total_size));
    }

    let overrides = build_overrides(src_root, ignores)?;
    let walker = WalkBuilder::new(src_root)
        .hidden(false)
        .git_ignore(false)
        .git_global(false)
        .git_exclude(false)
        .ignore(false)
        .parents(false)
        .overrides(overrides)
        .build();

    for entry in walker {
        let entry = entry?;
        let path = entry.path();
        if path == src_root {
            continue;
        }

        let rel_path = path.strip_prefix(src_root)?.to_string_lossy().to_string();
        let metadata = FileMetadata::from(entry.metadata()?);

        if entry.file_type().is_some_and(|ft| ft.is_symlink()) {
            let target = std::fs::read_link(path)?;
            tasks.push(SyncTask::Symlink {
                path: rel_path,
                target: target.to_string_lossy().to_string(),
                metadata,
            });
        } else if entry.file_type().is_some_and(|ft| ft.is_dir()) {
            tasks.push(SyncTask::Dir {
                path: rel_path,
                metadata,
            });
        } else if entry.file_type().is_some_and(|ft| ft.is_file()) {
            total_size += metadata.size;
            tasks.push(SyncTask::File { path: rel_path });
        }
    }

    Ok((tasks, total_size))
}

async fn collect_sync_tasks(
    src_root: &Path,
    ignores: &[String],
) -> anyhow::Result<(Vec<SyncTask>, u64)> {
    let src_root_owned = src_root.to_path_buf();
    let ignores_owned = ignores.to_vec();
    tokio::task::spawn_blocking(move || collect_sync_tasks_sync(&src_root_owned, &ignores_owned))
        .await?
}

/// Sync a remote file.
///
/// # Errors
///
/// Returns an error if synchronization fails.
#[allow(clippy::too_many_lines)]
pub async fn sync_remote_file<T>(
    framed: &mut Framed<T, DsyncCodec>,
    src_root: &Path,
    path: &Path,
    threshold: f32,
    checksum: bool,
    pb: Arc<ProgressBar>,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let rel_path = if src_root.is_file() {
        path.file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default()
    } else {
        path.strip_prefix(src_root)
            .unwrap_or(path)
            .to_string_lossy()
            .to_string()
    };

    let meta = tokio::fs::metadata(path).await?;
    let metadata = FileMetadata::from(meta);
    pb.set_message(rel_path.clone());
    framed
        .send(serialize_message(&Message::SyncFile {
            path: rel_path.clone(),
            metadata,
            threshold,
            checksum,
        })?)
        .await?;

    while let Some(msg_result) = framed.next().await {
        let msg_bytes = msg_result?;
        let msg = deserialize_message(&msg_bytes)?;
        match msg {
            Message::RequestFullCopy { path: p } => {
                let num_blocks = metadata.size.div_ceil(BLOCK_SIZE);
                let batch_size = 128_u64;
                let mut start_block = 0_u64;

                while start_block < num_blocks {
                    let end_block = std::cmp::min(start_block + batch_size, num_blocks);
                    let path_clone = path.to_path_buf();
                    let blocks = tokio::task::spawn_blocking(move || {
                        let file = std::fs::File::open(&path_clone)?;
                        let mut local_blocks = Vec::new();
                        #[allow(clippy::cast_possible_truncation)]
                        let mut buf = vec![0u8; BLOCK_SIZE as usize];

                        for block_idx in start_block..end_block {
                            let offset = block_idx * BLOCK_SIZE;
                            let n = file.read_at(&mut buf, offset)?;
                            let chunk =
                                buf.get(..n).ok_or_else(|| anyhow::anyhow!("chunk fail"))?;
                            local_blocks.push(Block {
                                offset,
                                data: chunk.to_vec(),
                            });
                        }

                        Ok::<Vec<Block>, anyhow::Error>(local_blocks)
                    })
                    .await??;

                    let bytes_sent: u64 = blocks.iter().map(|b| b.data.len() as u64).sum();
                    framed
                        .send(serialize_message(&Message::ApplyBlocks {
                            path: p.clone(),
                            blocks,
                        })?)
                        .await?;
                    pb.inc(bytes_sent);
                    start_block = end_block;
                }

                framed
                    .send(serialize_message(&Message::ApplyMetadata {
                        path: p,
                        metadata,
                    })?)
                    .await?;
            }
            Message::RequestHashes { path: p } => {
                let hashes = tools::calculate_file_hashes(path, BLOCK_SIZE).await?;
                framed
                    .send(serialize_message(&Message::BlockHashes {
                        path: p,
                        hashes,
                    })?)
                    .await?;
            }
            Message::RequestBlocks { path: p, indices } => {
                // Calculate skipped blocks for PB accuracy
                let num_blocks = metadata.size.div_ceil(BLOCK_SIZE);
                let requested_count = indices.len() as u64;
                if num_blocks > requested_count {
                    let mut requested_bytes = 0u64;
                    for &idx in &indices {
                        let offset = u64::from(idx) * BLOCK_SIZE;
                        let n = std::cmp::min(BLOCK_SIZE, metadata.size - offset);
                        requested_bytes += n;
                    }
                    pb.inc(metadata.size - requested_bytes);
                }

                let batch_size = 128;
                for chunk_indices in indices.chunks(batch_size) {
                    let path_clone = path.to_path_buf();
                    let chunk_indices_vec = chunk_indices.to_vec();
                    let blocks = tokio::task::spawn_blocking(move || {
                        let file = std::fs::File::open(&path_clone)?;
                        let mut local_blocks = Vec::with_capacity(chunk_indices_vec.len());
                        #[allow(clippy::cast_possible_truncation)]
                        let mut buf = vec![0u8; BLOCK_SIZE as usize];
                        for &idx in &chunk_indices_vec {
                            let offset = u64::from(idx) * BLOCK_SIZE;
                            let n = file.read_at(&mut buf, offset)?;
                            let chunk =
                                buf.get(..n).ok_or_else(|| anyhow::anyhow!("chunk fail"))?;
                            local_blocks.push(Block {
                                offset,
                                data: chunk.to_vec(),
                            });
                        }
                        Ok::<Vec<Block>, anyhow::Error>(local_blocks)
                    })
                    .await??;
                    let bytes_sent: u64 = blocks.iter().map(|b| b.data.len() as u64).sum();
                    framed
                        .send(serialize_message(&Message::ApplyBlocks {
                            path: p.clone(),
                            blocks,
                        })?)
                        .await?;
                    pb.inc(bytes_sent);
                }
                framed
                    .send(serialize_message(&Message::ApplyMetadata {
                        path: p.clone(),
                        metadata,
                    })?)
                    .await?;
            }
            Message::MetadataApplied { path: _ } => {
                return Ok(());
            }
            Message::EndOfFile { path: p } => {
                pb.inc(metadata.size);
                framed
                    .send(serialize_message(&Message::ApplyMetadata {
                        path: p,
                        metadata,
                    })?)
                    .await?;
                // MUST wait for acknowledgement even when skipping
            }
            _ => anyhow::bail!("Unexpected message: {msg:?}"),
        }
    }
    anyhow::bail!("Connection closed unexpectedly")
}
