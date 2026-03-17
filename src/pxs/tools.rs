use crate::pxs::sync;
use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use std::{
    ops::Range,
    os::unix::fs::{FileExt, MetadataExt},
    path::Path,
};
use tokio::fs;

const THRESHOLD_SCALE: u128 = 1_000_000;

fn metadata_mtime_nanos(meta: &std::fs::Metadata) -> Option<u32> {
    u32::try_from(meta.mtime_nsec()).ok()
}

fn scaled_threshold(threshold: f32) -> u128 {
    format!("{:.6}", threshold.clamp(0.0, 1.0))
        .replace('.', "")
        .parse::<u128>()
        .unwrap_or(0)
}

fn block_index(index: usize) -> anyhow::Result<u32> {
    u32::try_from(index).map_err(|e| anyhow::anyhow!(e))
}

fn block_offset(index: usize, block_size: u64) -> anyhow::Result<u64> {
    let block_index = u64::try_from(index).map_err(|e| anyhow::anyhow!(e))?;
    block_index
        .checked_mul(block_size)
        .ok_or_else(|| anyhow::anyhow!("block offset overflow"))
}

fn mmap_range(offset: u64, block_size: usize, len: usize) -> anyhow::Result<Option<Range<usize>>> {
    let start = usize::try_from(offset).map_err(|e| anyhow::anyhow!(e))?;
    if start >= len {
        return Ok(None);
    }

    let end = start.saturating_add(block_size).min(len);
    Ok(Some(start..end))
}

fn mmap_block_matches(
    mmap: &memmap2::Mmap,
    offset: u64,
    block_size: usize,
    expected_hash: u64,
) -> anyhow::Result<bool> {
    let Some(range) = mmap_range(offset, block_size, mmap.len())? else {
        return Ok(false);
    };

    Ok(mmap
        .get(range)
        .is_some_and(|chunk| sync::fast_hash_block(chunk) == expected_hash))
}

fn read_block_matches(
    file: &std::fs::File,
    buffer: &mut [u8],
    offset: u64,
    expected_hash: u64,
) -> anyhow::Result<bool> {
    let bytes_read = file.read_at(buffer, offset)?;
    if bytes_read == 0 {
        return Ok(false);
    }

    let chunk = buffer
        .get(..bytes_read)
        .ok_or_else(|| anyhow::anyhow!("read chunk exceeds buffer"))?;
    Ok(sync::fast_hash_block(chunk) == expected_hash)
}

/// Create a standardized progress bar for pxs
#[must_use]
pub fn create_progress_bar(total_size: u64) -> ProgressBar {
    let pb = ProgressBar::new(total_size);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({percent}%) {binary_bytes_per_sec} {eta} {msg}")
            .unwrap_or_else(|_| ProgressStyle::default_bar())
            .progress_chars("#>-"),
    );
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
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
        return Ok(src_meta.mtime() == dst_meta.mtime()
            && metadata_mtime_nanos(&src_meta) == metadata_mtime_nanos(&dst_meta));
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
    should_use_full_copy_meta(src_meta.len(), dst, threshold).await
}

/// Return true if full copy is more efficient than block comparison, using provided source size.
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

    let dst_meta = fs::metadata(dst).await?;
    let dst_size = dst_meta.len();

    // If file is large and destination exists, prefer delta-sync (resume) even if destination is small
    // because hashing is fast and network/disk-write is slow.
    // Heuristic: if file > 1MB, we use a much more aggressive threshold for full copy.
    let threshold = if src_size > 1024 * 1024 {
        threshold.min(0.1)
    } else {
        threshold
    };

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

/// Pre-allocate space for a file to improve write speed and reduce fragmentation.
///
/// # Errors
///
/// Returns an error if opening the file fails.
pub fn preallocate(path: &Path, size: u64) -> Result<()> {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;

    #[cfg(target_os = "linux")]
    {
        use nix::fcntl::{FallocateFlags, fallocate};
        if let Ok(size_i64) = i64::try_from(size) {
            let _ = fallocate(&file, FallocateFlags::empty(), 0, size_i64);
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = file;
        let _ = size;
    }

    Ok(())
}

/// create blake3 hash for a file
/// Compute destination block indices that differ from provided source hashes.
///
/// Uses memory-mapped I/O for performance when available, with automatic fallback
/// to buffered reads. The file should not be modified externally during this operation.
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
        return (0..hashes.len()).map(block_index).collect();
    }

    let file = std::fs::File::open(full_path)?;
    let file_len = file.metadata()?.len();
    let block_size_usize = usize::try_from(block_size).map_err(|e| anyhow::anyhow!(e))?;

    // SAFETY: Memory-mapped file for read-only access. The caller must ensure the file
    // is not modified or truncated during sync. We use bounds checking via .get() to
    // handle any size mismatches gracefully. Falls back to pread() if mmap fails.
    let mmap = unsafe { memmap2::MmapOptions::new().map(&file) };

    #[cfg(target_os = "linux")]
    if mmap.is_err() {
        // Hint sequential access for fallback path
        let _ = nix::fcntl::posix_fadvise(
            &file,
            0,
            0,
            nix::fcntl::PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL,
        );
    }

    let concurrency = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(8);
    let num_blocks = hashes.len();
    let chunk_size = num_blocks.div_ceil(concurrency);

    let mut requested = std::thread::scope(|scope| -> anyhow::Result<Vec<u32>> {
        let mut workers = Vec::new();

        for (chunk_index, hash_chunk) in hashes.chunks(chunk_size).enumerate() {
            let start_idx = chunk_index * chunk_size;
            let mmap_ref = mmap.as_ref().ok();
            let file_ref = &file;

            workers.push(scope.spawn(move || -> anyhow::Result<Vec<u32>> {
                let mut local_requested = Vec::new();
                let mut buffer = vec![0u8; block_size_usize];

                for (relative_index, &expected_hash) in hash_chunk.iter().enumerate() {
                    let block_idx = start_idx + relative_index;
                    let offset = block_offset(block_idx, block_size)?;

                    if offset >= file_len {
                        local_requested.push(block_index(block_idx)?);
                        continue;
                    }

                    let matched = if let Some(mapped) = mmap_ref {
                        mmap_block_matches(mapped, offset, block_size_usize, expected_hash)?
                    } else {
                        read_block_matches(file_ref, &mut buffer, offset, expected_hash)?
                    };

                    if !matched {
                        local_requested.push(block_index(block_idx)?);
                    }
                }

                Ok(local_requested)
            }));
        }

        let mut requested = Vec::new();
        for worker in workers {
            let worker_result = worker
                .join()
                .map_err(|_| anyhow::anyhow!("hash worker panicked"))?;
            requested.extend(worker_result?);
        }
        Ok(requested)
    })?;
    requested.sort_unstable();
    Ok(requested)
}

/// Calculate block hashes for a file using parallel workers.
///
/// Uses memory-mapped I/O for performance when available, with automatic fallback
/// to buffered reads. The file should not be modified externally during this operation.
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
        let num_blocks_usize = usize::try_from(num_blocks).map_err(|e| anyhow::anyhow!(e))?;
        let block_size_usize = usize::try_from(block_size).map_err(|e| anyhow::anyhow!(e))?;

        // SAFETY: Memory-mapped file for read-only access. The caller must ensure the file
        // is not modified or truncated during sync. We use bounds checking via .get() to
        // handle any size mismatches gracefully. Falls back to pread() if mmap fails.
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file) };

        #[cfg(target_os = "linux")]
        if mmap.is_err() {
            // Hint sequential access for fallback path
            let _ = nix::fcntl::posix_fadvise(
                &file,
                0,
                0,
                nix::fcntl::PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL,
            );
        }

        let concurrency = std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(8);
        let mut hashes = vec![0u64; num_blocks_usize];
        let chunk_size = num_blocks_usize.div_ceil(concurrency);

        std::thread::scope(|scope| -> anyhow::Result<()> {
            let mut workers = Vec::new();

            for start_block in (0..num_blocks_usize).step_by(chunk_size) {
                let end_block = std::cmp::min(start_block + chunk_size, num_blocks_usize);
                let mmap_ref = mmap.as_ref().ok();
                let file_ref = &file;

                workers.push(scope.spawn(move || -> anyhow::Result<(usize, Vec<u64>)> {
                    let mut local_hashes = Vec::with_capacity(end_block - start_block);
                    let mut buffer = vec![0u8; block_size_usize];

                    for block_idx in start_block..end_block {
                        let offset = block_offset(block_idx, block_size)?;
                        if let Some(mapped) = mmap_ref
                            && let Some(range) = mmap_range(offset, block_size_usize, mapped.len())?
                            && let Some(chunk) = mapped.get(range)
                        {
                            local_hashes.push(sync::fast_hash_block(chunk));
                            continue;
                        }

                        let bytes_read = file_ref.read_at(&mut buffer, offset)?;
                        let chunk = buffer
                            .get(..bytes_read)
                            .ok_or_else(|| anyhow::anyhow!("read chunk exceeds buffer"))?;
                        local_hashes.push(sync::fast_hash_block(chunk));
                    }

                    Ok((start_block, local_hashes))
                }));
            }

            for worker in workers {
                let worker_result = worker
                    .join()
                    .map_err(|_| anyhow::anyhow!("hash worker panicked"))?;
                let (start_block, local_hashes) = worker_result?;
                let end_block = start_block + local_hashes.len();
                let hash_slice = hashes
                    .get_mut(start_block..end_block)
                    .ok_or_else(|| anyhow::anyhow!("hash slice out of bounds"))?;
                hash_slice.copy_from_slice(&local_hashes);
            }

            Ok(())
        })?;

        Ok(hashes)
    })
    .await?
}
