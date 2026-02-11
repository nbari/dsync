use crate::dsync::sync;
use bytes::Bytes;
use filetime::FileTime;
use futures_util::{SinkExt, StreamExt};
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
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    process::Command,
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

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
/// # Panics
///
/// Panics if serialization fails.
#[must_use]
pub fn serialize_message(msg: &Message) -> Vec<u8> {
    let mut vec = AlignedVec::<16>::new();
    #[allow(clippy::expect_used)]
    to_bytes_in::<_, rkyv::rancor::Error>(msg, &mut vec).expect("failed to serialize message");
    vec.to_vec()
}

/// Deserialize a message from a byte slice.
///
/// # Panics
///
/// Panics if deserialization fails.
#[must_use]
pub fn deserialize_message(bytes: &[u8]) -> Message {
    let mut aligned = AlignedVec::<16>::new();
    aligned.extend_from_slice(bytes);

    #[allow(clippy::expect_used)]
    rkyv::from_bytes::<Message, rkyv::rancor::Error>(&aligned)
        .expect("failed to deserialize message")
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

/// Run the receiver to handle incoming sync connections.
///
/// # Errors
///
/// Returns an error if the listener fails to bind or synchronization fails.
pub async fn run_receiver(addr: &str, dst_root: &Path) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    println!("Receiver listening on {addr}");

    while let Ok((stream, peer_addr)) = listener.accept().await {
        println!("Accepted connection from {peer_addr}");

        let dst_root = dst_root.to_path_buf();
        tokio::spawn(async move {
            let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
            if let Err(e) = handle_client(&mut framed, &dst_root).await {
                eprintln!("Error handling client {peer_addr}: {e}");
            }
            println!("Connection with {peer_addr} closed");
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
    let mut framed = Framed::new(combined, LengthDelimitedCodec::new());
    handle_client(&mut framed, dst_root).await
}

#[allow(clippy::too_many_lines)]
/// Handle a client connection.
///
/// # Errors
///
/// Returns an error if synchronization or network I/O fails.
pub async fn handle_client<T>(
    framed: &mut Framed<T, LengthDelimitedCodec>,
    dst_root: &Path,
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    while let Some(result) = framed.next().await {
        let bytes = result?;
        let msg = deserialize_message(&bytes);

        match msg {
            Message::Handshake { version } => {
                println!("Client version: {version}");
                let resp = Message::Handshake {
                    version: env!("CARGO_PKG_VERSION").to_string(),
                };
                framed.send(Bytes::from(serialize_message(&resp))).await?;
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
                let full_path = dst_root.join(&path);
                if full_path.exists() {
                    let meta = tokio::fs::metadata(&full_path).await?;
                    if meta.len() == metadata.size && !checksum {
                        // Check mtime too
                        if meta.mtime() == metadata.mtime {
                            framed
                                .send(Bytes::from(serialize_message(&Message::EndOfFile { path })))
                                .await?;
                            continue;
                        }
                    }
                    framed
                        .send(Bytes::from(serialize_message(&Message::RequestHashes {
                            path,
                        })))
                        .await?;
                } else {
                    if let Some(parent) = full_path.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                    framed
                        .send(Bytes::from(serialize_message(&Message::RequestHashes {
                            path,
                        })))
                        .await?;
                }
            }
            Message::BlockHashes { path, hashes } => {
                let full_path = dst_root.join(&path);
                let mut requested = Vec::new();

                if full_path.exists() {
                    let file = std::fs::File::open(&full_path)?;
                    #[allow(clippy::cast_possible_truncation)]
                    let mut buf = vec![0u8; BLOCK_SIZE as usize];

                    for (idx, &src_hash) in hashes.iter().enumerate() {
                        #[allow(clippy::cast_possible_truncation)]
                        let offset = (idx as u64) * BLOCK_SIZE;
                        let mut matched = false;

                        if let Ok(n) = file.read_at(&mut buf, offset)
                            && n > 0
                        {
                            let chunk = buf
                                .get(..n)
                                .ok_or_else(|| anyhow::anyhow!("buffer access failed"))?;
                            let dst_hash = sync::fast_hash_block(chunk);
                            if dst_hash == src_hash {
                                matched = true;
                            }
                        }

                        if !matched {
                            #[allow(clippy::cast_possible_truncation)]
                            requested.push(idx as u32);
                        }
                    }
                } else {
                    #[allow(clippy::cast_possible_truncation)]
                    for idx in 0..hashes.len() {
                        requested.push(idx as u32);
                    }
                }

                if requested.is_empty() {
                    framed
                        .send(Bytes::from(serialize_message(&Message::EndOfFile { path })))
                        .await?;
                } else {
                    framed
                        .send(Bytes::from(serialize_message(&Message::RequestBlocks {
                            path,
                            indices: requested,
                        })))
                        .await?;
                }
            }
            Message::ApplyBlocks { path, blocks } => {
                let full_path = dst_root.join(&path);
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&full_path)?;

                for block in blocks {
                    file.write_all_at(&block.data, block.offset)?;
                }
            }
            Message::ApplyMetadata { path, metadata } => {
                let full_path = dst_root.join(&path);
                apply_file_metadata(&full_path, &metadata)?;
            }
            Message::EndOfFile { path: _ } => {
                // Here we might want to apply metadata after the file is fully received
                // but SyncFile metadata is already sent.
                // We'll trust the sender sends metadata in SyncFile.
                // For safety, let's have the sender send metadata again or ensure receiver stores it.
                // Actually, let's just make the receiver apply metadata when it gets EndOfFile if we stored it.
                // For now, let's just ensure the sender sends it and we apply it.
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
    let stream = TcpStream::connect(addr).await?;
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
    sender_loop(&mut framed, src_root, checksum, ignores).await
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
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    let combined = tokio::io::join(stdin, stdout);
    let mut framed = Framed::new(combined, LengthDelimitedCodec::new());
    sender_loop(&mut framed, src_root, checksum, ignores).await
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
    let mut cmd = Command::new("ssh");
    cmd.arg(addr);

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
    let mut framed = Framed::new(combined, LengthDelimitedCodec::new());

    sender_loop(&mut framed, src_root, checksum, ignores).await?;

    let status = child.wait().await?;
    if !status.success() {
        anyhow::bail!("SSH process exited with error: {status}");
    }

    Ok(())
}

async fn sender_loop<T>(
    framed: &mut Framed<T, LengthDelimitedCodec>,
    src_root: &Path,
    checksum: bool,
    ignores: &[String],
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    // Handshake
    let handshake = Message::Handshake {
        version: env!("CARGO_PKG_VERSION").to_string(),
    };
    framed
        .send(Bytes::from(serialize_message(&handshake)))
        .await?;

    let resp_bytes = framed
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("Connection closed during handshake"))??;
    let _resp = deserialize_message(&resp_bytes);

    println!("Handshake successful");

    let meta = tokio::fs::metadata(src_root).await?;
    if meta.is_dir() {
        sync_remote_dir(framed, src_root, src_root, checksum, ignores).await?;
    } else {
        sync_remote_file(
            framed,
            src_root.parent().unwrap_or(Path::new(".")),
            src_root,
            checksum,
        )
        .await?;
    }

    Ok(())
}

async fn sync_remote_file<T>(
    framed: &mut Framed<T, LengthDelimitedCodec>,
    src_root: &Path,
    path: &Path,
    checksum: bool,
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

    framed
        .send(Bytes::from(serialize_message(&sync_msg)))
        .await?;

    while let Some(msg_result) = framed.next().await {
        let msg_bytes = msg_result?;
        let msg = deserialize_message(&msg_bytes);

        match msg {
            Message::RequestHashes { path: p } => {
                let full_path = if src_root == path {
                    path.to_path_buf()
                } else {
                    src_root.join(&p)
                };
                let hashes = calculate_file_hashes(&full_path)?;
                let resp = Message::BlockHashes { path: p, hashes };
                framed.send(Bytes::from(serialize_message(&resp))).await?;
            }
            Message::RequestBlocks { path: p, indices } => {
                let full_path = if src_root == path {
                    path.to_path_buf()
                } else {
                    src_root.join(&p)
                };
                let file = std::fs::File::open(&full_path)?;
                let mut blocks = Vec::new();
                #[allow(clippy::cast_possible_truncation)]
                let mut buf = vec![0u8; BLOCK_SIZE as usize];

                for idx in indices {
                    let offset = u64::from(idx) * BLOCK_SIZE;
                    let n = file.read_at(&mut buf, offset)?;
                    let chunk = buf
                        .get(..n)
                        .ok_or_else(|| anyhow::anyhow!("buffer access failed"))?;
                    blocks.push(Block {
                        offset,
                        data: chunk.to_vec(),
                    });

                    if blocks.len() >= 16 {
                        let apply = Message::ApplyBlocks {
                            path: p.clone(),
                            blocks: std::mem::take(&mut blocks),
                        };
                        framed.send(Bytes::from(serialize_message(&apply))).await?;
                    }
                }

                if !blocks.is_empty() {
                    let apply = Message::ApplyBlocks {
                        path: p.clone(),
                        blocks,
                    };
                    framed.send(Bytes::from(serialize_message(&apply))).await?;
                }

                // Send final metadata to reset mtime
                let apply_meta = Message::ApplyMetadata {
                    path: p.clone(),
                    metadata,
                };
                framed
                    .send(Bytes::from(serialize_message(&apply_meta)))
                    .await?;

                let end = Message::EndOfFile { path: p };
                framed.send(Bytes::from(serialize_message(&end))).await?;
                return Ok(());
            }
            Message::EndOfFile { path: p } => {
                // Receiver says it's already up to date, but we still want to match mtime/mode
                let apply_meta = Message::ApplyMetadata { path: p, metadata };
                framed
                    .send(Bytes::from(serialize_message(&apply_meta)))
                    .await?;
                return Ok(());
            }
            _ => anyhow::bail!("Unexpected message: {msg:?}"),
        }
    }

    anyhow::bail!("Connection closed unexpectedly")
}

async fn sync_remote_dir<T>(
    framed: &mut Framed<T, LengthDelimitedCodec>,
    src_root: &Path,
    current_dir: &Path,
    checksum: bool,
    ignores: &[String],
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
            framed.send(Bytes::from(serialize_message(&msg))).await?;
        } else if file_type.is_dir() {
            let rel_path = path.strip_prefix(src_root)?.to_string_lossy().to_string();
            let meta = tokio::fs::metadata(path).await?;
            let msg = Message::SyncDir {
                path: rel_path,
                metadata: FileMetadata::from(meta),
            };
            framed.send(Bytes::from(serialize_message(&msg))).await?;

            Box::pin(sync_remote_dir(framed, src_root, path, checksum, ignores)).await?;
        } else if file_type.is_file() {
            sync_remote_file(framed, src_root, path, checksum).await?;
        }
    }

    Ok(())
}

fn calculate_file_hashes(path: &Path) -> anyhow::Result<Vec<u64>> {
    let file = std::fs::File::open(path)?;
    let len = file.metadata()?.len();
    let num_blocks = len.div_ceil(BLOCK_SIZE);
    let mut hashes = Vec::with_capacity(usize::try_from(num_blocks)?);
    #[allow(clippy::cast_possible_truncation)]
    let mut buf = vec![0u8; BLOCK_SIZE as usize];

    for i in 0..num_blocks {
        let offset = i * BLOCK_SIZE;
        let n = file.read_at(&mut buf, offset)?;
        let chunk = buf
            .get(..n)
            .ok_or_else(|| anyhow::anyhow!("buffer access failed"))?;
        hashes.push(sync::fast_hash_block(chunk));
    }

    Ok(hashes)
}
