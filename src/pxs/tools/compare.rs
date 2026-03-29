use anyhow::Result;
use std::{os::unix::fs::MetadataExt, path::Path};
use tokio::fs;

const THRESHOLD_SCALE: u128 = 1_000_000;
pub const DEFAULT_THRESHOLD: f32 = 0.1;

fn metadata_mtime_nanos(meta: &std::fs::Metadata) -> Option<u32> {
    u32::try_from(meta.mtime_nsec()).ok()
}

fn scaled_threshold(threshold: f32) -> u128 {
    format!("{:.6}", threshold.clamp(0.0, 1.0))
        .replace('.', "")
        .parse::<u128>()
        .unwrap_or(0)
}

/// Get the size of a file.
///
/// # Errors
///
/// Returns an error if metadata cannot be read.
pub async fn get_file_size(path: &Path) -> Result<u64> {
    Ok(fs::metadata(path).await?.len())
}

/// Return true if the file should be skipped based on metadata.
///
/// # Errors
///
/// Returns an error if metadata cannot be read.
pub async fn should_skip_file(src: &Path, dst: &Path, checksum: bool) -> Result<bool> {
    if !dst.exists() {
        return Ok(false);
    }

    let dst_meta = tokio::fs::symlink_metadata(dst).await?;
    if !dst_meta.file_type().is_file() {
        return Ok(false);
    }

    let src_meta = tokio::fs::metadata(src).await?;
    let src_size = src_meta.len();
    let dst_size = dst_meta.len();

    if src_size != dst_size {
        return Ok(false);
    }

    if !checksum {
        return Ok(src_meta.mtime() == dst_meta.mtime()
            && metadata_mtime_nanos(&src_meta) == metadata_mtime_nanos(&dst_meta));
    }

    Ok(false)
}

/// Return true if full copy is more efficient than block comparison.
///
/// # Errors
///
/// Returns an error if metadata cannot be read.
pub async fn should_use_full_copy(src: &Path, dst: &Path, threshold: f32) -> Result<bool> {
    if !dst.exists() {
        return Ok(true);
    }

    let src_meta = fs::metadata(src).await?;
    should_use_full_copy_meta(src_meta.len(), dst, threshold).await
}

/// Return true if full copy is more efficient than block comparison, using the provided source size.
///
/// # Errors
///
/// Returns an error if metadata cannot be read.
pub async fn should_use_full_copy_meta(src_size: u64, dst: &Path, threshold: f32) -> Result<bool> {
    if !dst.exists() {
        return Ok(true);
    }

    if src_size == 0 {
        return Ok(false);
    }

    let dst_meta = fs::symlink_metadata(dst).await?;
    if !dst_meta.file_type().is_file() {
        return Ok(true);
    }
    let dst_size = dst_meta.len();

    Ok(is_below_threshold(src_size, dst_size, threshold))
}

/// Compare destination/source size ratio against threshold.
///
/// Returns true if `dst_size / src_size < threshold`, meaning the destination
/// is too small relative to the source and a full copy should be performed.
#[must_use]
pub fn is_below_threshold(src_size: u64, dst_size: u64, threshold: f32) -> bool {
    if src_size == 0 {
        return false;
    }

    u128::from(dst_size) * THRESHOLD_SCALE < u128::from(src_size) * scaled_threshold(threshold)
}
