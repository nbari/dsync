use crate::dsync::sync;
use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use std::{
    os::unix::fs::{FileExt, MetadataExt},
    path::Path,
    sync::Arc,
};
use tokio::{fs, io::AsyncReadExt};

/// Create a standardized progress bar for dsync
#[must_use]
pub fn create_progress_bar(total_size: u64) -> ProgressBar {
    let pb = ProgressBar::new(total_size);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({percent}%) {bytes_per_sec} {eta} {msg}")
            .unwrap_or_else(|_| ProgressStyle::default_bar())
            .progress_chars("#>-"),
    );
    pb
}

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
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        return Ok(src_meta.mtime() == dst_meta.mtime()
            && (src_meta.mtime_nsec() as u32) == (dst_meta.mtime_nsec() as u32));
    }

    // If checksum is requested and size is same, we still need to check contents
    // This is handled by the caller (sync_changed_blocks)
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
    let dst_meta = fs::metadata(dst).await?;

    let src_size = src_meta.len();
    if src_size == 0 {
        return Ok(false);
    }

    let dst_size = dst_meta.len();
    Ok(is_below_threshold(src_size, dst_size, threshold))
}

/// Compare destination/source size ratio against threshold without float casts.
#[must_use]
pub fn is_below_threshold(src_size: u64, dst_size: u64, threshold: f32) -> bool {
    if src_size == 0 {
        return false;
    }

    let threshold = threshold.clamp(0.0, 1.0);
    let threshold_string = format!("{threshold:.6}");
    let mut parts = threshold_string.split('.');
    let whole = parts
        .next()
        .and_then(|part| part.parse::<u128>().ok())
        .unwrap_or(0);
    let fraction = parts
        .next()
        .and_then(|part| part.parse::<u128>().ok())
        .unwrap_or(0);

    let scaled_threshold = (whole * 1_000_000) + fraction;
    let scaled_dst = u128::from(dst_size) * 1_000_000;
    let scaled_src = u128::from(src_size) * scaled_threshold;

    scaled_dst < scaled_src
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

/// Compute destination block indices that differ from provided source hashes.
///
/// # Errors
///
/// Returns an error if file IO or worker coordination fails.
pub fn compute_requested_blocks(
    full_path: &Path,
    hashes: &[u64],
    block_size: u64,
) -> anyhow::Result<Vec<u32>> {
    if !full_path.exists() {
        #[allow(clippy::cast_possible_truncation)]
        return Ok((0..hashes.len() as u32).collect());
    }

    let file = std::fs::File::open(full_path)?;
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
            let hashes_ref = hashes;
            let results_clone = Arc::clone(&results);

            s.spawn(move || {
                let mut local_requested = Vec::new();
                #[allow(clippy::cast_possible_truncation)]
                let mut buf = vec![0u8; block_size as usize];

                for idx in start_idx..end_idx {
                    let offset = idx * block_size;
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

    let mut requested = Arc::try_unwrap(results)
        .map_err(|_| anyhow::anyhow!("Arc busy"))?
        .into_inner()
        .map_err(|_| anyhow::anyhow!("Mutex poisoned"))?;
    requested.sort_unstable();
    Ok(requested)
}

/// Calculate block hashes for a file using parallel workers.
///
/// # Errors
///
/// Returns an error if file IO, task scheduling, or conversion fails.
pub async fn calculate_file_hashes(path: &Path, block_size: u64) -> anyhow::Result<Vec<u64>> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&path)?;
        let len = file.metadata()?.len();
        let num_blocks = len.div_ceil(block_size);
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
                    let mut buf = vec![0u8; block_size as usize];
                    for (i, block_idx) in (start_block..end_block).enumerate() {
                        let offset = block_idx * block_size;
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
