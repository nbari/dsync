use crate::dsync::{sync, tools};
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
    path::Path,
    process::Stdio,
    sync::Arc,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    process::Command,
    sync::mpsc,
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
        checksum: bool,
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

/// Handle a client connection.
///
/// # Errors
///
/// Returns an error if synchronization or network I/O fails.
#[allow(clippy::too_many_lines)]
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
            Message::Handshake { version } => {
                tracing::info!("Client connected (version: {version})");
                let resp = Message::Handshake {
                    version: env!("CARGO_PKG_VERSION").to_string(),
                };
                framed.send(serialize_message(&resp)?).await?;
            }
            Message::SyncDir { path, metadata } => {
                let full_path = dst_root.join(&path);
                if !full_path.exists() {
                    tokio::fs::create_dir_all(&full_path).await?;
                }
                apply_file_metadata(&full_path, &metadata)?;
            }
            Message::SyncSymlink {
                path,
                target,
                metadata,
            } => {
                let full_path = dst_root.join(&path);
                if full_path.exists() {
                    tokio::fs::remove_file(&full_path).await?;
                }
                tokio::fs::symlink(target, &full_path).await?;
                apply_file_metadata(&full_path, &metadata)?;
            }
            Message::SyncFile {
                path,
                metadata,
                checksum,
            } => {
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
                        continue;
                    }
                } else if let Some(parent) = full_path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                framed
                    .send(serialize_message(&Message::RequestHashes { path })?)
                    .await?;
            }
            Message::BlockHashes { path, hashes } => {
                let full_path = dst_root.join(&path);
                let requested = if full_path.exists() {
                    let file = std::fs::File::open(&full_path)?;
                    let concurrency = std::thread::available_parallelism()
                        .map(std::num::NonZeroUsize::get)
                        .unwrap_or(8);
                    let num_blocks = hashes.len();
                    let results = Arc::new(std::sync::Mutex::new(Vec::new()));
                    #[allow(clippy::cast_possible_truncation)]
                    let chunk_size = (num_blocks as u64).div_ceil(concurrency as u64);
                    std::thread::scope(|s| {
                        for i in 0..concurrency {
                            let start_idx = (i as u64) * chunk_size;
                            if start_idx >= num_blocks as u64 {
                                break;
                            }
                            let end_idx = std::cmp::min(start_idx + chunk_size, num_blocks as u64);
                            let file_ref = &file;
                            let hashes_ref = &hashes;
                            let results_clone = Arc::clone(&results);
                            s.spawn(move || {
                                let mut local_requested = Vec::new();
                                #[allow(clippy::cast_possible_truncation)]
                                let mut buf = vec![0u8; BLOCK_SIZE as usize];
                                for idx in start_idx..end_idx {
                                    let offset = idx * BLOCK_SIZE;
                                    let mut matched = false;
                                    if let Ok(n) = file_ref.read_at(&mut buf, offset)
                                        && n > 0
                                    {
                                        let chunk = buf.get(..n).unwrap_or_default();
                                        #[allow(clippy::cast_possible_truncation)]
                                        if let Some(&h) = hashes_ref.get(idx as usize)
                                            && sync::fast_hash_block(chunk) == h
                                        {
                                            matched = true;
                                        }
                                    }
                                    if !matched {
                                        #[allow(clippy::cast_possible_truncation)]
                                        local_requested.push(idx as u32);
                                    }
                                }
                                if let Ok(mut r) = results_clone.lock() {
                                    r.extend(local_requested);
                                }
                            });
                        }
                    });
                    let mut r = Arc::try_unwrap(results)
                        .map_err(|_| anyhow::anyhow!("Arc busy"))?
                        .into_inner()
                        .map_err(|_| anyhow::anyhow!("Mutex poisoned"))?;
                    r.sort_unstable();
                    r
                } else {
                    #[allow(clippy::cast_possible_truncation)]
                    (0..hashes.len() as u32).collect()
                };
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
            }
            Message::ApplyBlocks { path, blocks } => {
                let file = if let Some(f) = open_files.get(&path) {
                    f
                } else {
                    let full_path = dst_root.join(&path);
                    let f = std::fs::OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(false)
                        .open(full_path)?;
                    open_files.entry(path.clone()).or_insert(f)
                };
                for block in blocks {
                    file.write_all_at(&block.data, block.offset)?;
                }
            }
            Message::ApplyMetadata { path, metadata } => {
                let full_path = dst_root.join(&path);
                open_files.remove(&path);
                apply_file_metadata(&full_path, &metadata)?;
                framed
                    .send(serialize_message(&Message::MetadataApplied { path })?)
                    .await?;
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
    checksum: bool,
    ignores: &[String],
) -> anyhow::Result<()> {
    let total_size = sync::calculate_total_size(src_root, ignores)?;
    let pb = Arc::new(tools::create_progress_bar(total_size));
    let stream = TcpStream::connect(addr).await?;
    let mut framed = Framed::new(stream, DsyncCodec);
    sender_loop(&mut framed, src_root, checksum, ignores, pb).await
}

/// Run the sender using stdin/stdout (for manual piping)
///
/// # Errors
///
/// Returns an error if synchronization fails.
pub async fn run_stdio_sender(
    src_root: &Path,
    checksum: bool,
    ignores: &[String],
) -> anyhow::Result<()> {
    let total_size = sync::calculate_total_size(src_root, ignores)?;
    let pb = Arc::new(tools::create_progress_bar(total_size));
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    let combined = tokio::io::join(stdin, stdout);
    let mut framed = Framed::new(combined, DsyncCodec);
    sender_loop(&mut framed, src_root, checksum, ignores, pb).await
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
    checksum: bool,
    ignores: &[String],
) -> anyhow::Result<()> {
    let total_size = sync::calculate_total_size(src_root, ignores)?;
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
        sender_loop(&mut framed, src_root, checksum, ignores, Arc::clone(&pb)).await?;
    }
    let status = child.wait().await?;
    if !status.success() {
        anyhow::bail!("SSH process exited with error: {status}");
    }
    pb.finish_with_message("Done");
    Ok(())
}

async fn sender_loop<T>(
    framed: &mut Framed<T, DsyncCodec>,
    src_root: &Path,
    checksum: bool,
    ignores: &[String],
    pb: Arc<ProgressBar>,
) -> anyhow::Result<()>
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
    eprintln!("Handshake successful");
    sync_remote_dir(framed, src_root, src_root, checksum, ignores, pb).await
}

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

async fn sync_remote_dir<T>(
    framed: &mut Framed<T, DsyncCodec>,
    src_root: &Path,
    current_dir: &Path,
    checksum: bool,
    ignores: &[String],
    pb: Arc<ProgressBar>,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    use ignore::WalkBuilder;
    let (tx, mut rx) = mpsc::channel(128);
    let src_root_owned = src_root.to_path_buf();
    let current_dir_owned = current_dir.to_path_buf();
    let ignores_owned = ignores.to_vec();

    tokio::task::spawn_blocking(move || {
        let mut builder = WalkBuilder::new(&current_dir_owned);
        builder.hidden(false).git_ignore(false).ignore(false);
        for pattern in &ignores_owned {
            let _ = builder.add_ignore(pattern);
        }
        let walker = builder.build();
        for entry in walker {
            let Ok(entry) = entry else { continue };
            let path = entry.path();

            // Only skip the exact root if it's a directory
            if path == current_dir_owned && path.is_dir() {
                continue;
            }

            let Ok(rel_path) = path.strip_prefix(&src_root_owned) else {
                // If stripping prefix fails, it's either the root file or something outside
                if path == src_root_owned {
                    let rel_path_str = path
                        .file_name()
                        .map_or_else(|| ".".to_string(), |n| n.to_string_lossy().to_string());
                    let Ok(_meta) = entry.metadata() else {
                        continue;
                    };
                    let _ = tx.blocking_send(SyncTask::File { path: rel_path_str });
                }
                continue;
            };

            let rel_path_str = if rel_path.as_os_str().is_empty() {
                path.file_name()
                    .map_or_else(|| ".".to_string(), |n| n.to_string_lossy().to_string())
            } else {
                rel_path.to_string_lossy().to_string()
            };

            let Ok(meta) = entry.metadata() else { continue };
            let metadata = FileMetadata::from(meta);
            let task = if entry.file_type().is_some_and(|ft| ft.is_symlink()) {
                let target = std::fs::read_link(path).unwrap_or_default();
                SyncTask::Symlink {
                    path: rel_path_str,
                    target: target.to_string_lossy().to_string(),
                    metadata,
                }
            } else if entry.file_type().is_some_and(|ft| ft.is_dir()) {
                SyncTask::Dir {
                    path: rel_path_str,
                    metadata,
                }
            } else {
                SyncTask::File { path: rel_path_str }
            };
            let _ = tx.blocking_send(task);
        }
    });

    while let Some(task) = rx.recv().await {
        match task {
            SyncTask::Dir { path, metadata } => {
                framed
                    .send(serialize_message(&Message::SyncDir { path, metadata })?)
                    .await?;
            }
            SyncTask::Symlink {
                path,
                target,
                metadata,
            } => {
                framed
                    .send(serialize_message(&Message::SyncSymlink {
                        path,
                        target,
                        metadata,
                    })?)
                    .await?;
            }
            SyncTask::File { path } => {
                // Determine the correct source path for reading
                let full_src_path = if src_root.is_file() {
                    src_root.to_path_buf()
                } else {
                    src_root.join(&path)
                };
                sync_remote_file(framed, src_root, &full_src_path, checksum, pb.clone()).await?;
            }
        }
    }
    Ok(())
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
            checksum,
        })?)
        .await?;

    while let Some(msg_result) = framed.next().await {
        let msg_bytes = msg_result?;
        let msg = deserialize_message(&msg_bytes)?;
        match msg {
            Message::RequestHashes { path: p } => {
                let hashes = calculate_file_hashes(path).await?;
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
                let skipped_count = num_blocks - requested_count;
                if skipped_count > 0 {
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

async fn calculate_file_hashes(path: &Path) -> anyhow::Result<Vec<u64>> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&path)?;
        let len = file.metadata()?.len();
        let num_blocks = len.div_ceil(BLOCK_SIZE);
        let concurrency = std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(8);
        let mut hashes = vec![0u64; usize::try_from(num_blocks)?];
        #[allow(clippy::cast_possible_truncation)]
        let chunk_size = num_blocks.div_ceil(concurrency as u64);
        std::thread::scope(|s| {
            let mut remaining_hashes = &mut hashes[..];
            for _ in 0..concurrency {
                #[allow(clippy::cast_possible_truncation)]
                let current_chunk_size = std::cmp::min(chunk_size as usize, remaining_hashes.len());
                if current_chunk_size == 0 {
                    break;
                }
                let (chunk_hashes, next_remaining) =
                    remaining_hashes.split_at_mut(current_chunk_size);
                remaining_hashes = next_remaining;
                let start_block = (num_blocks
                    - (remaining_hashes.len() as u64 + chunk_hashes.len() as u64))
                    as u64;
                let end_block = start_block + chunk_hashes.len() as u64;
                let file_ref = &file;
                s.spawn(move || {
                    #[allow(clippy::cast_possible_truncation)]
                    let mut buf = vec![0u8; BLOCK_SIZE as usize];
                    for (i, block_idx) in (start_block..end_block).enumerate() {
                        let offset = block_idx * BLOCK_SIZE;
                        if let Ok(n) = file_ref.read_at(&mut buf, offset)
                            && n > 0
                        {
                            #[allow(clippy::indexing_slicing)]
                            if let Some(h) = chunk_hashes.get_mut(i) {
                                *h = sync::fast_hash_block(&buf[..n]);
                            }
                        }
                    }
                });
            }
        });
        Ok(hashes)
    })
    .await?
}
