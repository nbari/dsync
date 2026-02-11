use crate::dsync::sync;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use rkyv::{
    api::high::to_bytes_in,
    util::AlignedVec,
    {Archive, Deserialize, Serialize},
};
use std::{
    os::unix::fs::{FileExt, MetadataExt},
    path::Path,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    process::Command,
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

const BLOCK_SIZE: u64 = 64 * 1024;

#[derive(Archive, Deserialize, Serialize, Debug)]
pub enum Message {
    Handshake {
        version: String,
    },
    SyncDir {
        path: String,
    },
    SyncFile {
        path: String,
        size: u64,
        mtime: i64,
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
            Message::SyncFile {
                path,
                size,
                mtime: _,
                checksum,
            } => {
                let full_path = dst_root.join(&path);
                if full_path.exists() {
                    let meta = tokio::fs::metadata(&full_path).await?;
                    if meta.len() == size && !checksum {
                        framed
                            .send(Bytes::from(serialize_message(&Message::EndOfFile { path })))
                            .await?;
                    } else {
                        framed
                            .send(Bytes::from(serialize_message(&Message::RequestHashes {
                                path,
                            })))
                            .await?;
                    }
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
            Message::EndOfFile { path: _ } => {
                // Done with file
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
pub async fn run_sender(addr: &str, src_root: &Path, checksum: bool) -> anyhow::Result<()> {
    let stream = TcpStream::connect(addr).await?;
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
    sender_loop(&mut framed, src_root, checksum).await
}

/// Run the sender using stdin/stdout (for manual piping)
///
/// # Errors
///
/// Returns an error if synchronization fails.
pub async fn run_stdio_sender(src_root: &Path, checksum: bool) -> anyhow::Result<()> {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    let combined = tokio::io::join(stdin, stdout);
    let mut framed = Framed::new(combined, LengthDelimitedCodec::new());
    sender_loop(&mut framed, src_root, checksum).await
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
) -> anyhow::Result<()> {
    use std::process::Stdio;

    let mut child = Command::new("ssh")
        .arg(addr)
        .arg(format!("dsync --stdio --destination {dst_path}"))
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

    sender_loop(&mut framed, src_root, checksum).await?;

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
        sync_remote_dir(framed, src_root, src_root, checksum).await?;
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

    let sync_msg = Message::SyncFile {
        path: relative_path.clone(),
        size: meta.len(),
        mtime: meta.mtime(),
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

                let end = Message::EndOfFile { path: p };
                framed.send(Bytes::from(serialize_message(&end))).await?;
                return Ok(());
            }
            Message::EndOfFile { path: _ } => {
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
) -> anyhow::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut entries = tokio::fs::read_dir(current_dir).await?;

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let file_type = entry.file_type().await?;

        if file_type.is_dir() {
            Box::pin(sync_remote_dir(framed, src_root, &path, checksum)).await?;
        } else if file_type.is_file() {
            sync_remote_file(framed, src_root, &path, checksum).await?;
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
