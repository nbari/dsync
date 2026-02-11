use crate::dsync::sync;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use rkyv::api::high::to_bytes_in;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};
use std::os::unix::fs::FileExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use tokio::net::{TcpListener, TcpStream};
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
            if let Err(e) = handle_client(stream, &dst_root).await {
                eprintln!("Error handling client {peer_addr}: {e}");
            }
        });
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn handle_client(stream: TcpStream, dst_root: &Path) -> anyhow::Result<()> {
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());

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
                let file = std::fs::File::open(&full_path)?;
                let mut requested = Vec::new();
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
        .ok_or_else(|| anyhow::anyhow!("Connection closed"))??;
    let _resp = deserialize_message(&resp_bytes);

    println!("Handshake successful");

    sync_remote_dir(&mut framed, src_root, src_root, checksum).await?;

    Ok(())
}

async fn sync_remote_dir(
    framed: &mut Framed<TcpStream, LengthDelimitedCodec>,
    src_root: &Path,
    current_dir: &Path,
    checksum: bool,
) -> anyhow::Result<()> {
    let mut entries = tokio::fs::read_dir(current_dir).await?;

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let file_type = entry.file_type().await?;

        if file_type.is_dir() {
            Box::pin(sync_remote_dir(framed, src_root, &path, checksum)).await?;
        } else if file_type.is_file() {
            let relative_path = path.strip_prefix(src_root)?.to_string_lossy().to_string();
            let meta = entry.metadata().await?;

            let sync_msg = Message::SyncFile {
                path: relative_path.clone(),
                size: meta.len(),
                mtime: meta.mtime(),
                checksum,
            };

            framed
                .send(Bytes::from(serialize_message(&sync_msg)))
                .await?;

            // File loop
            loop {
                let msg_bytes = framed
                    .next()
                    .await
                    .ok_or_else(|| anyhow::anyhow!("Connection closed"))??;
                let msg = deserialize_message(&msg_bytes);

                match msg {
                    Message::RequestHashes { path: p } => {
                        let full_path = src_root.join(&p);
                        let hashes = calculate_file_hashes(&full_path)?;
                        let resp = Message::BlockHashes { path: p, hashes };
                        framed.send(Bytes::from(serialize_message(&resp))).await?;
                    }
                    Message::RequestBlocks { path: p, indices } => {
                        let full_path = src_root.join(&p);
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

                            // Batch blocks to avoid huge messages but keep it fast
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
                        break;
                    }
                    Message::EndOfFile { path: _ } => {
                        break;
                    }
                    _ => anyhow::bail!("Unexpected message from receiver: {msg:?}"),
                }
            }
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
