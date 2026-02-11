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

    // Set permissions
    std::fs::set_permissions(path, Permissions::from_mode(metadata.mode))?;

    // Set ownership
    let _ = nix::unistd::chown(
        path,
        Some(nix::unistd::Uid::from_raw(metadata.uid)),
        Some(nix::unistd::Gid::from_raw(metadata.gid)),
    );

    // Set times
    let mtime = FileTime::from_unix_time(metadata.mtime, metadata.mtime_nsec);
    // Use same for atime
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

            // High-speed search for Magic Header

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
                // Wait for more data
                return Ok(None);
            }

            // We have a full frame!
            src.advance(8); // Skip magic and length
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
    let mut open_files: std::collections::HashMap<String, std::fs::File> =
        std::collections::HashMap::new();

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
                    if meta.len() == metadata.size && !checksum {
                        // Check mtime too
                        if meta.mtime() == metadata.mtime {
                            framed
                                .send(serialize_message(&Message::EndOfFile { path })?)
                                .await?;
                            continue;
                        }
                    }
                    framed
                        .send(serialize_message(&Message::RequestHashes { path })?)
                        .await?;
                } else {
                    if let Some(parent) = full_path.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                    framed
                        .send(serialize_message(&Message::RequestHashes { path })?)
                        .await?;
                }
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
                                    #[allow(clippy::cast_possible_truncation)]
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
                    let results_guard = results
                        .lock()
                        .map_err(|_| anyhow::anyhow!("mutex poisoned"))?;
                    let mut r = results_guard.clone();
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
                // Ensure file is closed before applying metadata (some OS need this for mtime)
                open_files.remove(&path);
                apply_file_metadata(&full_path, &metadata)?;
            }
            Message::EndOfFile { path } => {
                open_files.remove(&path);
            }
            _ => {
                eprintln!("Unhandled message: {msg:?}");
            }
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
        .ok_or_else(|| anyhow::anyhow!("failed to open stdin"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("failed to open stdout"))?;

    let combined = tokio::io::join(stdout, stdin);
    let mut framed = Framed::new(combined, DsyncCodec);

    sender_loop(&mut framed, src_root, checksum, ignores, Arc::clone(&pb)).await?;

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
    // Handshake
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

    let meta = tokio::fs::metadata(src_root).await?;
    if meta.is_dir() {
        sync_remote_dir(framed, src_root, src_root, checksum, ignores, pb).await?;
    } else {
        sync_remote_file(
            framed,
            src_root.parent().unwrap_or(Path::new(".")),
            src_root,
            checksum,
            pb,
        )
        .await?;
    }

    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn sync_remote_file<T>(
    framed: &mut Framed<T, DsyncCodec>,
    src_root: &Path,
    path: &Path,
    checksum: bool,
    pb: Arc<ProgressBar>,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut relative_path = path
        .strip_prefix(src_root)
        .unwrap_or(Path::new(""))
        .to_string_lossy()
        .to_string();
    if relative_path.is_empty() {
        relative_path = path
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("invalid filename"))?
            .to_string_lossy()
            .to_string();
    }
    let meta = tokio::fs::metadata(path).await?;
    let metadata = FileMetadata::from(meta);

    let sync_msg = Message::SyncFile {
        path: relative_path.clone(),
        metadata,
        checksum,
    };

    pb.set_message(relative_path.clone());
    framed.send(serialize_message(&sync_msg)?).await?;

    while let Some(msg_result) = framed.next().await {
        let msg_bytes = msg_result?;
        let msg = deserialize_message(&msg_bytes)?;

        match msg {
            Message::RequestHashes { path: p } => {
                let full_path = if src_root == path {
                    path.to_path_buf()
                } else {
                    src_root.join(&p)
                };
                let hashes = calculate_file_hashes(&full_path).await?;
                let resp = Message::BlockHashes { path: p, hashes };
                framed.send(serialize_message(&resp)?).await?;
            }
            Message::RequestBlocks { path: p, indices } => {
                let full_path = if src_root == path {
                    path.to_path_buf()
                } else {
                    src_root.join(&p)
                };

                // CRITICAL: Calculate skipped blocks and update progress bar
                // Total blocks in file
                let num_blocks = metadata.size.div_ceil(BLOCK_SIZE);
                let requested_count = indices.len() as u64;
                let skipped_count = num_blocks - requested_count;

                // Increment PB for blocks already on the destination
                if skipped_count > 0 {
                    // We need to be careful with the last block which might be smaller than 64KB
                    // For simplicity, we approximate or use the exact bytes.
                    // Let's use metadata.size - (actual requested bytes)
                    let mut requested_bytes = 0u64;
                    for &idx in &indices {
                        let offset = u64::from(idx) * BLOCK_SIZE;
                        let n = std::cmp::min(BLOCK_SIZE, metadata.size - offset);
                        requested_bytes += n;
                    }
                    pb.inc(metadata.size - requested_bytes);
                }

                // Read and send blocks in large parallel batches
                let batch_size = 128; // 8MB per batch

                for chunk_indices in indices.chunks(batch_size) {
                    let full_path_clone = full_path.clone();
                    let chunk_indices_vec = chunk_indices.to_vec();

                    let blocks = tokio::task::spawn_blocking(move || {
                        let file = std::fs::File::open(&full_path_clone)?;
                        let mut local_blocks = Vec::with_capacity(chunk_indices_vec.len());
                        #[allow(clippy::cast_possible_truncation)]
                        let mut buf = vec![0u8; BLOCK_SIZE as usize];

                        for &idx in &chunk_indices_vec {
                            let offset = u64::from(idx) * BLOCK_SIZE;
                            let n = file.read_at(&mut buf, offset)?;
                            let chunk = buf.get(..n).unwrap_or_default();
                            local_blocks.push(Block {
                                offset,
                                data: chunk.to_vec(),
                            });
                        }
                        Ok::<Vec<Block>, anyhow::Error>(local_blocks)
                    })
                    .await??;

                    let bytes_sent: u64 = blocks.iter().map(|b| b.data.len() as u64).sum();
                    let apply = Message::ApplyBlocks {
                        path: p.clone(),
                        blocks,
                    };
                    framed.send(serialize_message(&apply)?).await?;
                    pb.inc(bytes_sent);
                }

                // Send final metadata
                let apply_meta = Message::ApplyMetadata {
                    path: p.clone(),
                    metadata,
                };
                framed.send(serialize_message(&apply_meta)?).await?;

                let end = Message::EndOfFile { path: p };
                framed.send(serialize_message(&end)?).await?;
                return Ok(());
            }
            Message::EndOfFile { path: p } => {
                // Receiver says it's already up to date
                pb.inc(metadata.size);
                let apply_meta = Message::ApplyMetadata { path: p, metadata };
                framed.send(serialize_message(&apply_meta)?).await?;
                return Ok(());
            }
            _ => anyhow::bail!("Unexpected message: {msg:?}"),
        }
    }

    anyhow::bail!("Connection closed unexpectedly")
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
    use ignore::overrides::OverrideBuilder;

    let mut override_builder = OverrideBuilder::new(current_dir);
    for pattern in ignores {
        override_builder.add(&format!("!{pattern}"))?;
    }
    let overrides = override_builder.build()?;

    let walker = WalkBuilder::new(current_dir)
        .hidden(false)
        .git_ignore(false)
        .git_global(false)
        .git_exclude(false)
        .ignore(false)
        .parents(false)
        .max_depth(Some(1)) // Process level by level to mimic recursion
        .overrides(overrides)
        .build();

    for entry in walker {
        let entry = entry?;
        let path = entry.path();
        if path == current_dir {
            continue;
        }

        let file_type = entry
            .file_type()
            .ok_or_else(|| anyhow::anyhow!("unknown file type"))?;

        if file_type.is_symlink() {
            let target = tokio::fs::read_link(path).await?;
            let rel_path = path.strip_prefix(src_root)?.to_string_lossy().to_string();
            let meta = tokio::fs::symlink_metadata(path).await?;
            let msg = Message::SyncSymlink {
                path: rel_path,
                target: target.to_string_lossy().to_string(),
                metadata: FileMetadata::from(meta),
            };
            framed.send(serialize_message(&msg)?).await?;
        } else if file_type.is_dir() {
            let rel_path = path.strip_prefix(src_root)?.to_string_lossy().to_string();
            let meta = tokio::fs::metadata(path).await?;
            let msg = Message::SyncDir {
                path: rel_path,
                metadata: FileMetadata::from(meta),
            };
            framed.send(serialize_message(&msg)?).await?;

            Box::pin(sync_remote_dir(
                framed,
                src_root,
                path,
                checksum,
                ignores,
                Arc::clone(&pb),
            ))
            .await?;
        } else if file_type.is_file() {
            sync_remote_file(framed, src_root, path, checksum, Arc::clone(&pb)).await?;
        }
    }

    Ok(())
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
            // Use split_at_mut to safely give each thread its own slice of the hashes vector
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
