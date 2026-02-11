use crate::dsync::tools;
use indicatif::{ProgressBar, ProgressStyle};
use std::{hash::Hasher, os::unix::fs::FileExt, path::Path, sync::Arc};
use tokio::sync::Semaphore;
use twox_hash::XxHash64;

const BLOCK_SIZE: usize = 64 * 1024;

#[derive(Debug, Default, Clone, Copy)]
pub struct SyncStats {
    pub total_blocks: usize,
    pub updated_blocks: usize,
}

/// Fast hash a block of data.
///
/// # Panics
///
/// Panics if the hasher fails.
#[must_use]
pub fn fast_hash_block(data: &[u8]) -> u64 {
    let mut hasher = XxHash64::with_seed(0);
    hasher.write(data);
    hasher.finish()
}

/// Calculate the total size of a directory
///
/// # Errors
///
/// Returns an error if any IO operation fails.
pub async fn calculate_total_size(path: &Path) -> std::io::Result<u64> {
    let mut total = 0;
    let mut entries = tokio::fs::read_dir(path).await?;
    while let Some(entry) = entries.next_entry().await? {
        let file_type = entry.file_type().await?;
        if file_type.is_dir() {
            total += Box::pin(calculate_total_size(&entry.path())).await?;
        } else {
            total += entry.metadata().await?.len();
        }
    }
    Ok(total)
}

/// Synchronize a directory
///
/// # Errors
///
/// Returns an error if any synchronization task fails.
pub async fn sync_dir(
    src_dir: &Path,
    dst_dir: &Path,
    threshold: f32,
    checksum: bool,
    dry_run: bool,
) -> anyhow::Result<()> {
    println!("Calculating total size...");
    let total_size = calculate_total_size(src_dir).await?;

    let pb = ProgressBar::new(total_size);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")?
            .progress_chars("#>-"),
    );

    let pb = Arc::new(pb);

    sync_dir_recursive(
        src_dir,
        dst_dir,
        threshold,
        checksum,
        dry_run,
        Arc::clone(&pb),
    )
    .await?;

    pb.finish_with_message("Done");
    Ok(())
}

async fn sync_dir_recursive(
    src_dir: &Path,
    dst_dir: &Path,
    threshold: f32,
    checksum: bool,
    dry_run: bool,
    pb: Arc<ProgressBar>,
) -> anyhow::Result<()> {
    if !dst_dir.exists() {
        if dry_run {
            println!("(dry-run) create directory: {}", dst_dir.display());
        } else {
            tokio::fs::create_dir_all(dst_dir).await?;
        }
    }

    let mut entries = tokio::fs::read_dir(src_dir).await?;
    let semaphore = Arc::new(Semaphore::new(
        std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(8),
    ));

    let mut tasks = Vec::new();

    while let Some(entry) = entries.next_entry().await? {
        let src_path = entry.path();
        let relative_path = src_path.strip_prefix(src_dir)?;
        let dst_path = dst_dir.join(relative_path);

        let file_type = entry.file_type().await?;

        if file_type.is_dir() {
            let pb_clone = Arc::clone(&pb);
            Box::pin(sync_dir_recursive(
                &src_path, &dst_path, threshold, checksum, dry_run, pb_clone,
            ))
            .await?;
        } else if file_type.is_file() {
            let sem = Arc::clone(&semaphore);
            let src = src_path.clone();
            let dst = dst_path.clone();
            let pb_clone = Arc::clone(&pb);

            tasks.push(tokio::spawn(async move {
                let _permit = sem
                    .acquire()
                    .await
                    .map_err(|e| anyhow::anyhow!("failed to acquire semaphore: {e}"))?;

                if tools::should_skip_file(&src, &dst, checksum).await? {
                    let src_size = tools::get_file_size(&src).await?;
                    pb_clone.inc(src_size);
                    return Ok::<(), anyhow::Error>(());
                }

                if dry_run {
                    let src_size = tools::get_file_size(&src).await?;
                    println!(
                        "(dry-run) sync file: {} ({} bytes)",
                        src.display(),
                        src_size
                    );
                    pb_clone.inc(src_size);
                    return Ok::<(), anyhow::Error>(());
                }

                sync_changed_blocks_with_pb(&src, &dst, false, pb_clone).await?;
                Ok::<(), anyhow::Error>(())
            }));
        }
    }

    for task in tasks {
        task.await??;
    }

    Ok(())
}

/// Synchronize changed blocks with progress bar
///
/// # Errors
///
/// Returns an error if any IO operation fails.
pub async fn sync_changed_blocks_with_pb(
    src_path: &Path,
    dst_path: &Path,
    full_copy: bool,
    pb: Arc<ProgressBar>,
) -> anyhow::Result<SyncStats> {
    let src_file = std::fs::File::open(src_path)?;
    let dst_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dst_path)?;

    let src_len = src_file.metadata()?.len();
    let dst_len = dst_file.metadata()?.len();

    if dst_len < src_len {
        dst_file.set_len(src_len)?;
    }

    let src_arc = Arc::new(src_file);
    let dst_arc = Arc::new(dst_file);

    let concurrency = std::thread::available_parallelism()
        .map(std::num::NonZero::get)
        .unwrap_or(8);

    let semaphore = Arc::new(Semaphore::new(concurrency));
    let mut handles = Vec::new();
    let chunk_size = 1024 * 1024; // 1MB chunks for parallel processing

    let num_chunks = src_len.div_ceil(chunk_size);

    for i in 0..num_chunks {
        let src = Arc::clone(&src_arc);
        let dst = Arc::clone(&dst_arc);
        let start_offset = i * chunk_size;
        let end_offset = std::cmp::min(start_offset + chunk_size, src_len);
        let pb_worker = Arc::clone(&pb);
        let sem = Arc::clone(&semaphore);

        let handle = tokio::spawn(async move {
            let _permit = sem
                .acquire()
                .await
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            tokio::task::spawn_blocking(move || {
                let mut offset = start_offset;
                let mut src_buf = vec![0u8; BLOCK_SIZE];
                let mut dst_buf = vec![0u8; BLOCK_SIZE];
                let mut chunk_updated = 0;
                let mut chunk_total = 0;

                while offset < end_offset {
                    let to_read_u64 = std::cmp::min(BLOCK_SIZE as u64, end_offset - offset);
                    let to_read = usize::try_from(to_read_u64).map_err(|e| anyhow::anyhow!(e))?;

                    let src_chunk = src_buf
                        .get_mut(..to_read)
                        .ok_or_else(|| anyhow::anyhow!("src_buf too small"))?;
                    src.read_exact_at(src_chunk, offset)?;

                    let needs_write = if full_copy {
                        true
                    } else {
                        let dst_chunk = dst_buf
                            .get_mut(..to_read)
                            .ok_or_else(|| anyhow::anyhow!("dst_buf too small"))?;
                        let dst_read_result = dst.read_exact_at(dst_chunk, offset);
                        match dst_read_result {
                            Ok(()) => {
                                let src_hash = fast_hash_block(src_chunk);
                                let dst_hash = fast_hash_block(dst_chunk);
                                src_hash != dst_hash
                            }
                            Err(_) => true,
                        }
                    };

                    if needs_write {
                        dst.write_all_at(src_chunk, offset)?;
                        chunk_updated += 1;
                    }

                    chunk_total += 1;
                    pb_worker.inc(to_read as u64);
                    offset += to_read as u64;
                }
                Ok::<SyncStats, anyhow::Error>(SyncStats {
                    total_blocks: chunk_total,
                    updated_blocks: chunk_updated,
                })
            })
            .await?
        });
        handles.push(handle);
    }

    let mut stats = SyncStats::default();
    for handle in handles {
        let chunk_stats = handle.await.map_err(|e| anyhow::anyhow!(e))??;
        stats.total_blocks += chunk_stats.total_blocks;
        stats.updated_blocks += chunk_stats.updated_blocks;
    }

    if dst_len > src_len {
        dst_arc.set_len(src_len)?;
    }

    Ok(stats)
}

/// Synchronize changed blocks
///
/// # Errors
///
/// Returns an error if any IO operation fails.
pub async fn sync_changed_blocks(
    src_path: &Path,
    dst_path: &Path,
    full_copy: bool,
) -> anyhow::Result<SyncStats> {
    let pb = Arc::new(ProgressBar::hidden());
    sync_changed_blocks_with_pb(src_path, dst_path, full_copy, pb).await
}
