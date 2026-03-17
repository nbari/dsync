use anyhow::Context;
use filetime::FileTime;
use rkyv::{
    api::high::to_bytes_in,
    util::AlignedVec,
    {Archive, Deserialize, Serialize},
};
use std::{os::unix::fs::MetadataExt, path::Path};

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
    SyncStart {
        total_size: u64,
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
pub fn serialize_message(msg: &Message) -> anyhow::Result<AlignedVec<16>> {
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

    let is_symlink = path.is_symlink();

    if !is_symlink {
        std::fs::set_permissions(path, Permissions::from_mode(metadata.mode))
            .context("failed to set permissions")?;
    }

    let _ = nix::unistd::chown(
        path,
        Some(nix::unistd::Uid::from_raw(metadata.uid)),
        Some(nix::unistd::Gid::from_raw(metadata.gid)),
    );

    let mtime = FileTime::from_unix_time(metadata.mtime, metadata.mtime_nsec);
    if is_symlink {
        filetime::set_symlink_file_times(path, mtime, mtime)
            .context("failed to set symlink times")?;
    } else {
        filetime::set_file_times(path, mtime, mtime).context("failed to set file times")?;
    }

    Ok(())
}
