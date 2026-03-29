use crate::pxs::sync::{SyncStats, meta};
use crate::pxs::tools::{self, ProgressBarLike, clamped_parallelism};
use anyhow::{Context, Result};
use futures_util::StreamExt;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Semaphore;

pub(crate) const BLOCK_SIZE: usize = 128 * 1024;

fn mapped_chunk(mmap: &memmap2::Mmap, offset: u64, len: usize) -> Result<Option<&[u8]>> {
    let start = usize::try_from(offset).map_err(|e| anyhow::anyhow!(e))?;
    let end = start
        .checked_add(len)
        .ok_or_else(|| anyhow::anyhow!("mapped chunk end overflow"))?;
    Ok(mmap.get(start..end))
}

pub(crate) struct WorkerContext {
    pub chunk_size: u64,
    pub src_len: u64,
    pub src: Arc<std::fs::File>,
    pub dst: Arc<std::fs::File>,
    pub src_mmap: Option<Arc<memmap2::Mmap>>,
    pub dst_mmap: Option<Arc<memmap2::Mmap>>,
    pub pb: Arc<dyn ProgressBarLike>,
    pub semaphore: Arc<Semaphore>,
    pub full_copy: bool,
}

/// Synchronize changed blocks with progress bar.
///
/// # Errors
///
/// Returns an error if any IO operation fails.
pub async fn sync_changed_blocks_with_pb(
    src_path: &Path,
    dst_path: &Path,
    full_copy: bool,
    pb: Arc<dyn ProgressBarLike>,
    fsync: bool,
    semaphore: Option<Arc<Semaphore>>,
) -> Result<SyncStats> {
    let mut staged_file = tools::StagedFile::new(dst_path)?;
    let seed_from_existing = matches!(std::fs::symlink_metadata(dst_path), Ok(meta) if meta.file_type().is_file())
        && !full_copy;
    staged_file
        .prepare(tools::get_file_size(src_path).await?, seed_from_existing)
        .context("failed to prepare staged destination file")?;
    let sync_result =
        sync_changed_blocks_to_staging(src_path, staged_file.path(), full_copy, pb, semaphore)
            .await;

    let stats = match sync_result {
        Ok(stats) => stats,
        Err(error) => {
            let _ = staged_file.cleanup();
            return Err(error);
        }
    };

    let metadata_result = meta::apply_metadata(src_path, staged_file.path());
    if let Err(error) = metadata_result {
        let _ = staged_file.cleanup();
        return Err(error);
    }

    if fsync {
        let staged_handle = std::fs::OpenOptions::new()
            .read(true)
            .open(staged_file.path())
            .with_context(|| {
                format!(
                    "failed to open staged destination file: {}",
                    staged_file.path().display()
                )
            })?;
        staged_handle
            .sync_all()
            .context("failed to sync staged destination file to disk")?;
    }

    let commit_result = staged_file.commit();
    if let Err(error) = commit_result {
        let _ = staged_file.cleanup();
        return Err(error);
    }

    if fsync {
        tools::sync_parent_directory(dst_path)
            .context("failed to sync destination parent directory")?;
    }

    Ok(stats)
}

async fn sync_changed_blocks_to_staging(
    src_path: &Path,
    staged_path: &Path,
    full_copy: bool,
    pb: Arc<dyn ProgressBarLike>,
    semaphore: Option<Arc<Semaphore>>,
) -> Result<SyncStats> {
    let src_file = std::fs::File::open(src_path)
        .with_context(|| format!("failed to open source file: {}", src_path.display()))?;

    let src_len = src_file
        .metadata()
        .context("failed to get source metadata")?
        .len();

    let dst_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(staged_path)
        .with_context(|| {
            format!(
                "failed to open staged destination file: {}",
                staged_path.display()
            )
        })?;

    let dst_len = dst_file
        .metadata()
        .context("failed to get staged destination metadata")?
        .len();

    // Hint sequential access
    #[cfg(target_os = "linux")]
    {
        let _ = nix::fcntl::posix_fadvise(
            &src_file,
            0,
            0,
            nix::fcntl::PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL,
        );
        let _ = nix::fcntl::posix_fadvise(
            &dst_file,
            0,
            0,
            nix::fcntl::PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL,
        );
    }

    let src_arc = Arc::new(src_file);
    let dst_arc = Arc::new(dst_file);
    let src_mmap = tools::safe_mmap(src_arc.as_ref()).ok().map(Arc::new);
    let dst_mmap = tools::safe_mmap(dst_arc.as_ref()).ok().map(Arc::new);

    let concurrency = clamped_parallelism();
    let semaphore = semaphore.unwrap_or_else(|| Arc::new(Semaphore::new(concurrency)));
    let chunk_size: u64 = 1024 * 1024; // 1MB chunks for parallel processing
    let worker_context = Arc::new(WorkerContext {
        chunk_size,
        src_len,
        src: Arc::clone(&src_arc),
        dst: Arc::clone(&dst_arc),
        src_mmap,
        dst_mmap,
        pb: Arc::clone(&pb),
        semaphore: Arc::clone(&semaphore),
        full_copy,
    });
    let num_chunks = src_len.div_ceil(chunk_size);
    let mut tasks = futures_util::stream::FuturesUnordered::new();
    let mut stats = SyncStats::default();

    for chunk_index in 0..num_chunks {
        if tasks.len() >= concurrency
            && let Some(res) = tasks.next().await
        {
            let res: std::result::Result<Result<SyncStats>, tokio::task::JoinError> = res;
            let chunk_stats: SyncStats = res
                .map_err(|e| anyhow::anyhow!(e))
                .context("worker task panicked")??;
            stats.total_blocks += chunk_stats.total_blocks;
            stats.updated_blocks += chunk_stats.updated_blocks;
        }
        tasks.push(spawn_sync_worker(chunk_index, Arc::clone(&worker_context)));
    }

    while let Some(res) = tasks.next().await {
        let res: std::result::Result<Result<SyncStats>, tokio::task::JoinError> = res;
        let chunk_stats: SyncStats = res
            .map_err(|e| anyhow::anyhow!(e))
            .context("worker task panicked")??;
        stats.total_blocks += chunk_stats.total_blocks;
        stats.updated_blocks += chunk_stats.updated_blocks;
    }

    if dst_len > src_len {
        dst_arc
            .set_len(src_len)
            .context("failed to truncate destination file")?;
    }

    Ok(stats)
}

fn spawn_sync_worker(
    chunk_index: u64,
    context: Arc<WorkerContext>,
) -> tokio::task::JoinHandle<Result<SyncStats>> {
    tokio::spawn(async move {
        let semaphore = Arc::clone(&context.semaphore);
        let worker_context = Arc::clone(&context);
        let _permit = semaphore
            .acquire()
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))
            .context("failed to acquire semaphore")?;

        tokio::task::spawn_blocking(move || {
            let start_offset = chunk_index * worker_context.chunk_size;
            let end_offset = std::cmp::min(
                start_offset + worker_context.chunk_size,
                worker_context.src_len,
            );
            let mut offset = start_offset;
            let mut src_buf = vec![0u8; BLOCK_SIZE];
            let mut dst_buf = vec![0u8; BLOCK_SIZE];
            let mut chunk_updated = 0;
            let mut chunk_total = 0;

            while offset < end_offset {
                let to_read_u64 = std::cmp::min(BLOCK_SIZE as u64, end_offset - offset);
                let to_read = usize::try_from(to_read_u64).map_err(|e| anyhow::anyhow!(e))?;

                let mut write_if_needed = |src_chunk: &[u8], needs_write: bool| -> Result<()> {
                    if needs_write {
                        if let Err(e) = worker_context.dst.write_all_at(src_chunk, offset) {
                            if e.raw_os_error() == Some(nix::libc::ENOSPC) {
                                anyhow::bail!(
                                    "Disk full: not enough space to write to destination"
                                );
                            }
                            return Err(e).context("failed to write to destination");
                        }
                        chunk_updated += 1;
                    }
                    Ok(())
                };

                if let Some(src_mmap) = &worker_context.src_mmap
                    && let Some(src_chunk) = mapped_chunk(src_mmap, offset, to_read)?
                {
                    let needs_write = if worker_context.full_copy {
                        true
                    } else if let Some(dst_mmap) = &worker_context.dst_mmap {
                        match mapped_chunk(dst_mmap, offset, to_read)? {
                            Some(dst_chunk) => src_chunk != dst_chunk,
                            None => true,
                        }
                    } else {
                        let dst_chunk = dst_buf
                            .get_mut(..to_read)
                            .ok_or_else(|| anyhow::anyhow!("dst_buf too small"))?;
                        match worker_context.dst.read_exact_at(dst_chunk, offset) {
                            Ok(()) => src_chunk != dst_chunk,
                            Err(_) => true,
                        }
                    };
                    write_if_needed(src_chunk, needs_write)?;
                    chunk_total += 1;
                    worker_context.pb.inc(to_read_u64);
                    offset += to_read as u64;
                    continue;
                }

                let src_chunk = src_buf
                    .get_mut(..to_read)
                    .ok_or_else(|| anyhow::anyhow!("src_buf too small"))?;
                worker_context
                    .src
                    .read_exact_at(src_chunk, offset)
                    .context("failed to read from source")?;

                let needs_write = if worker_context.full_copy {
                    true
                } else {
                    let dst_chunk = dst_buf
                        .get_mut(..to_read)
                        .ok_or_else(|| anyhow::anyhow!("dst_buf too small"))?;
                    match worker_context.dst.read_exact_at(dst_chunk, offset) {
                        Ok(()) => src_chunk != dst_chunk,
                        Err(_) => true,
                    }
                };

                write_if_needed(src_chunk, needs_write)?;

                chunk_total += 1;
                worker_context.pb.inc(to_read_u64);
                offset += to_read as u64;
            }
            Ok::<SyncStats, anyhow::Error>(SyncStats {
                total_blocks: chunk_total,
                updated_blocks: chunk_updated,
            })
        })
        .await?
    })
}
