use crate::pxs::{
    sync,
    tools::{
        probes::{record_safe_mmap_success, record_sender_mmap_read_hit as probe_sender_mmap_hit},
        runtime::clamped_parallelism,
    },
};
use anyhow::Result;
use std::{ops::Range, os::unix::fs::FileExt, path::Path};

fn block_index(index: usize) -> Result<u32> {
    u32::try_from(index).map_err(|error| anyhow::anyhow!(error))
}

fn block_offset(index: usize, block_size: u64) -> Result<u64> {
    let block_index = u64::try_from(index).map_err(|error| anyhow::anyhow!(error))?;
    block_index
        .checked_mul(block_size)
        .ok_or_else(|| anyhow::anyhow!("block offset overflow"))
}

fn mmap_range(offset: u64, block_size: usize, len: usize) -> Result<Option<Range<usize>>> {
    let start = usize::try_from(offset).map_err(|error| anyhow::anyhow!(error))?;
    if start >= len {
        return Ok(None);
    }

    let end = start.saturating_add(block_size).min(len);
    Ok(Some(start..end))
}

/// Create a copy-on-write memory map that protects against in-place modifications.
///
/// # Errors
///
/// Returns an error if the map cannot be created.
pub(crate) fn safe_mmap(file: &std::fs::File) -> Result<memmap2::Mmap, std::io::Error> {
    let mmap = unsafe { memmap2::MmapOptions::new().map_copy_read_only(file) };
    if mmap.is_ok() {
        record_safe_mmap_success();
    }
    mmap
}

pub(crate) fn record_sender_mmap_read_hit() {
    probe_sender_mmap_hit();
}

fn mmap_block_matches(
    mmap: &memmap2::Mmap,
    offset: u64,
    block_size: usize,
    expected_hash: u64,
) -> Result<bool> {
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
) -> Result<bool> {
    let bytes_read = file.read_at(buffer, offset)?;
    if bytes_read == 0 {
        return Ok(false);
    }

    let chunk = buffer
        .get(..bytes_read)
        .ok_or_else(|| anyhow::anyhow!("read chunk exceeds buffer"))?;
    Ok(sync::fast_hash_block(chunk) == expected_hash)
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
) -> Result<Vec<u32>> {
    if hashes.is_empty() {
        return Ok(Vec::new());
    }

    if !full_path.exists() {
        return (0..hashes.len()).map(block_index).collect();
    }

    let file = std::fs::File::open(full_path)?;
    let file_len = file.metadata()?.len();
    let block_size_usize = usize::try_from(block_size).map_err(|error| anyhow::anyhow!(error))?;
    let mmap = safe_mmap(&file);

    #[cfg(target_os = "linux")]
    if mmap.is_err() {
        let _ = nix::fcntl::posix_fadvise(
            &file,
            0,
            0,
            nix::fcntl::PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL,
        );
    }

    let concurrency = clamped_parallelism();
    let num_blocks = hashes.len();
    let chunk_size = num_blocks.div_ceil(concurrency);

    let mut requested = std::thread::scope(|scope| -> Result<Vec<u32>> {
        let mut workers = Vec::new();

        for (chunk_index, hash_chunk) in hashes.chunks(chunk_size).enumerate() {
            let start_idx = chunk_index * chunk_size;
            let mmap_ref = mmap.as_ref().ok();
            let file_ref = &file;

            workers.push(scope.spawn(move || -> Result<Vec<u32>> {
                let mut local_requested = Vec::new();
                let mut buffer = vec![0_u8; block_size_usize];

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

/// Calculate block hashes for an already-open file using parallel workers.
///
/// # Errors
///
/// Returns an error if file IO, task scheduling, or conversion fails.
pub(crate) fn calculate_file_hashes_for_open_file(
    file: &std::fs::File,
    block_size: u64,
    mmap: Option<&memmap2::Mmap>,
) -> Result<Vec<u64>> {
    let len = file.metadata()?.len();
    let num_blocks = len.div_ceil(block_size);
    let num_blocks_usize = usize::try_from(num_blocks).map_err(|error| anyhow::anyhow!(error))?;
    if num_blocks_usize == 0 {
        return Ok(Vec::new());
    }

    let block_size_usize = usize::try_from(block_size).map_err(|error| anyhow::anyhow!(error))?;

    #[cfg(target_os = "linux")]
    if mmap.is_none() {
        let _ = nix::fcntl::posix_fadvise(
            file,
            0,
            0,
            nix::fcntl::PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL,
        );
    }

    let concurrency = clamped_parallelism();
    let mut hashes = vec![0_u64; num_blocks_usize];
    let chunk_size = num_blocks_usize.div_ceil(concurrency);

    std::thread::scope(|scope| -> Result<()> {
        let mut workers = Vec::new();

        for start_block in (0..num_blocks_usize).step_by(chunk_size) {
            let end_block = std::cmp::min(start_block + chunk_size, num_blocks_usize);
            let mmap_ref = mmap;
            let file_ref = file;

            workers.push(scope.spawn(move || -> Result<(usize, Vec<u64>)> {
                let mut local_hashes = Vec::with_capacity(end_block - start_block);
                let mut buffer = vec![0_u8; block_size_usize];

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
}

/// Calculate block hashes for a file using parallel workers.
///
/// # Errors
///
/// Returns an error if file IO, task scheduling, or conversion fails.
pub async fn calculate_file_hashes(path: &Path, block_size: u64) -> Result<Vec<u64>> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&path)?;
        let mmap = safe_mmap(&file);
        calculate_file_hashes_for_open_file(&file, block_size, mmap.as_ref().ok())
    })
    .await?
}

/// Compute the BLAKE3 hash of a file for end-to-end verification.
///
/// # Errors
///
/// Returns an error if file IO fails.
pub async fn blake3_file_hash(path: &Path) -> Result<[u8; 32]> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut hasher = blake3::Hasher::new();
        hasher.update_mmap_rayon(&path)?;
        Ok(*hasher.finalize().as_bytes())
    })
    .await?
}
