use anyhow::{Context, Result};
use filetime::FileTime;
use rkyv::{
    api::high::to_bytes_in,
    util::AlignedVec,
    {Archive, Deserialize, Serialize},
};
use std::{os::unix::fs::MetadataExt, path::Path};

#[cfg(unix)]
fn try_set_ownership(path: &Path, metadata: &FileMetadata, is_symlink: bool) {
    use std::os::unix::ffi::OsStrExt;

    if is_symlink {
        match std::ffi::CString::new(path.as_os_str().as_bytes()) {
            Ok(path_cstr) => {
                // SAFETY: `path_cstr` is a valid C string for the lifetime of this call.
                let result =
                    unsafe { nix::libc::lchown(path_cstr.as_ptr(), metadata.uid, metadata.gid) };
                if result != 0 {
                    let error = std::io::Error::last_os_error();
                    tracing::debug!(
                        "Could not set ownership on symlink {}: {error}",
                        path.display()
                    );
                }
            }
            Err(error) => {
                tracing::debug!(
                    "Could not encode symlink path {} for ownership update: {error}",
                    path.display()
                );
            }
        }
        return;
    }

    if let Err(error) = nix::unistd::chown(
        path,
        Some(nix::unistd::Uid::from_raw(metadata.uid)),
        Some(nix::unistd::Gid::from_raw(metadata.gid)),
    ) {
        tracing::debug!("Could not set ownership on {}: {error}", path.display());
    }
}

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
    SessionOptions {
        fsync: bool,
        delete: bool,
        path: Option<Vec<u8>>,
        single_file_name: Option<Vec<u8>>,
    },
    PullRequest {
        path: Option<Vec<u8>>,
        threshold: f32,
        checksum: bool,
        delete: bool,
        ignores: Vec<String>,
    },
    ParallelTransferConfig {
        threshold_bytes: u64,
        worker_count: u16,
    },
    SyncStart {
        total_size: u64,
    },
    SyncDir {
        path: Vec<u8>,
        metadata: FileMetadata,
    },
    SyncSymlink {
        path: Vec<u8>,
        target: Vec<u8>,
        metadata: FileMetadata,
    },
    SyncFile {
        path: Vec<u8>,
        metadata: FileMetadata,
        threshold: f32,
        checksum: bool,
    },
    RequestFullCopy {
        path: Vec<u8>,
    },
    RequestParallelFullCopy {
        path: Vec<u8>,
        transfer_id: String,
    },
    RequestHashes {
        path: Vec<u8>,
    },
    BlockHashes {
        path: Vec<u8>,
        hashes: Vec<u64>,
    },
    ApplyBlocks {
        path: Vec<u8>,
        blocks: Vec<Block>,
    },
    ApplyBlocksCompressed {
        path: Vec<u8>,
        compressed: Vec<u8>,
    },
    ApplyMetadata {
        path: Vec<u8>,
        metadata: FileMetadata,
    },
    MetadataApplied {
        path: Vec<u8>,
    },
    EndOfFile {
        path: Vec<u8>,
    },
    RequestBlocks {
        path: Vec<u8>,
        indices: Vec<u32>,
    },
    RequestParallelBlocks {
        path: Vec<u8>,
        transfer_id: String,
        indices: Vec<u32>,
    },
    ChunkWriterStart {
        transfer_id: String,
        path: Vec<u8>,
    },
    VerifyChecksum {
        path: Vec<u8>,
        hash: [u8; 32],
    },
    ChecksumVerified {
        path: Vec<u8>,
    },
    ChecksumMismatch {
        path: Vec<u8>,
    },
    SyncComplete,
    SyncCompleteAck,
    Error(String),
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
pub struct Block {
    pub offset: u64,
    pub data: Vec<u8>,
}

#[derive(Archive, Deserialize, Serialize, Debug)]
struct BlockBatchPayload {
    blocks: Vec<Block>,
}

/// Serialize a message to a byte vector.
///
/// # Errors
///
/// Returns an error if serialization fails.
pub fn serialize_message(msg: &Message) -> Result<AlignedVec<16>> {
    let mut vec = AlignedVec::<16>::new();
    to_bytes_in::<_, rkyv::rancor::Error>(msg, &mut vec)
        .map_err(|e| anyhow::anyhow!("failed to serialize message: {e}"))?;
    Ok(vec)
}

/// Deserialize a message from a byte slice.
///
/// # Errors
///
/// Returns an error if deserialization fails.
pub fn deserialize_message(bytes: &[u8]) -> Result<Message> {
    let mut aligned = AlignedVec::<16>::new();
    aligned.extend_from_slice(bytes);
    rkyv::from_bytes::<Message, rkyv::rancor::Error>(&aligned)
        .map_err(|e| anyhow::anyhow!("failed to deserialize message: {e}"))
}

/// Serialize a list of blocks for compressed transport.
///
/// # Errors
///
/// Returns an error if serialization fails.
pub(crate) fn serialize_block_batch(blocks: &[Block]) -> Result<AlignedVec<16>> {
    let payload = BlockBatchPayload {
        blocks: blocks.to_vec(),
    };
    let mut vec = AlignedVec::<16>::new();
    to_bytes_in::<_, rkyv::rancor::Error>(&payload, &mut vec)
        .map_err(|e| anyhow::anyhow!("failed to serialize block batch: {e}"))?;
    Ok(vec)
}

/// Deserialize a compressed block batch payload.
///
/// # Errors
///
/// Returns an error if deserialization fails.
pub(crate) fn deserialize_block_batch(bytes: &[u8]) -> Result<Vec<Block>> {
    let mut aligned = AlignedVec::<16>::new();
    aligned.extend_from_slice(bytes);
    let payload = rkyv::from_bytes::<BlockBatchPayload, rkyv::rancor::Error>(&aligned)
        .map_err(|e| anyhow::anyhow!("failed to deserialize block batch: {e}"))?;
    Ok(payload.blocks)
}

/// Apply metadata to a local path.
///
/// # Errors
///
/// Returns an error if any attribute fails to be applied.
pub fn apply_file_metadata(path: &Path, metadata: &FileMetadata) -> Result<()> {
    use std::fs::Permissions;
    use std::os::unix::fs::PermissionsExt;

    let is_symlink = path.is_symlink();

    if !is_symlink {
        std::fs::set_permissions(path, Permissions::from_mode(metadata.mode))
            .context("failed to set permissions")?;
    }

    #[cfg(unix)]
    try_set_ownership(path, metadata, is_symlink);

    let mtime = FileTime::from_unix_time(metadata.mtime, metadata.mtime_nsec);
    if is_symlink {
        filetime::set_symlink_file_times(path, mtime, mtime)
            .context("failed to set symlink times")?;
    } else {
        filetime::set_file_times(path, mtime, mtime).context("failed to set file times")?;
    }

    Ok(())
}
