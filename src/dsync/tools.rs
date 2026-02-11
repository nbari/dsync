use anyhow::Result;
use std::{os::unix::fs::MetadataExt, path::Path};
use tokio::{fs, io::AsyncReadExt};

/// Get the size of a file
///
/// # Errors
///
/// Returns an error if the metadata cannot be read.
pub async fn get_file_size(path: &Path) -> Result<u64> {
    Ok(fs::metadata(path).await?.len())
}

/// Return true if the file should be skipped based on metadata
///
/// # Errors
///
/// Returns an error if the metadata cannot be read.
pub async fn should_skip_file(src: &Path, dst: &Path, checksum: bool) -> Result<bool> {
    // 1. If destination doesn't exist — don't skip
    if !dst.exists() {
        return Ok(false);
    }

    // 2. Compare sizes
    let src_meta = tokio::fs::metadata(src).await?;
    let dst_meta = tokio::fs::metadata(dst).await?;
    let src_size = src_meta.len();
    let dst_size = dst_meta.len();

    if src_size != dst_size {
        return Ok(false);
    }

    // If checksum is NOT requested, rely on mtime
    if !checksum {
        return Ok(src_meta.mtime() == dst_meta.mtime());
    }

    // If checksum is requested and size is same, we still need to check contents
    // This is handled by the caller (sync_changed_blocks)
    Ok(false)
}

/// create blake3 hash for a file
///
/// # Errors
///
/// Returns an error if the file cannot be read.
pub async fn blake3(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path).await?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0_u8; 65536]; // 64 KiB

    loop {
        let size = file.read(&mut buf).await?;
        if size == 0 {
            break;
        }
        let chunk = buf
            .get(..size)
            .ok_or_else(|| anyhow::anyhow!("buffer too small"))?;
        hasher.update(chunk);
    }

    Ok(hasher.finalize().to_hex().to_string())
}
