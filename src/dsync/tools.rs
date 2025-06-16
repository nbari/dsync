use anyhow::Result;
use std::path::Path;
use tokio::{fs, io::AsyncReadExt};

pub async fn get_file_size(path: &Path) -> Result<u64> {
    Ok(fs::metadata(path).await?.len())
}

/// Return true if the destination file is missing or needs to be updated.
pub async fn should_copy_file(src: &Path, dst: &Path, threshold: f32) -> Result<bool> {
    // 1. If destination doesn't exist — copy
    if !dst.exists() {
        return Ok(true);
    }

    // 2. Compare sizes
    let src_meta = tokio::fs::metadata(src).await?;
    let dst_meta = tokio::fs::metadata(dst).await?;
    let src_size = src_meta.len();
    let dst_size = dst_meta.len();

    // 3. If dest is significantly smaller, do full copy
    if dst_size < (src_size as f64 * threshold as f64) as u64 {
        return Ok(true);
    }

    // 4. Otherwise, assume it's similar enough; no copy
    Ok(false)
}

// create blake3 hash for a file
pub async fn blake3(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path).await?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = [0_u8; 65536]; // 64 KiB

    loop {
        let size = file.read(&mut buf).await?;
        if size == 0 {
            break;
        }
        hasher.update(&buf[..size]);
    }

    Ok(hasher.finalize().to_hex().to_string())
}
