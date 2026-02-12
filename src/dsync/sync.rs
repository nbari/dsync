use crate::dsync::tools;
use filetime::{FileTime, set_file_times};
use indicatif::ProgressBar;
use std::{hash::Hasher, os::unix::fs::FileExt, path::Path, sync::Arc};
use tokio::sync::Semaphore;
use twox_hash::XxHash64;

const BLOCK_SIZE: usize = 64 * 1024;

#[derive(Debug, Default, Clone, Copy)]
pub struct SyncStats {
    pub total_blocks: usize,
    pub updated_blocks: usize,
}

/// Apply metadata (mode, uid, gid, mtime) from src to dst
///
/// # Errors
///
/// Returns an error if any attribute fails to be applied.
pub fn apply_metadata(src: &Path, dst: &Path) -> anyhow::Result<()> {
    let meta = std::fs::metadata(src)?;
    let permissions = meta.permissions();
    std::fs::set_permissions(dst, permissions)?;

    // Set ownership if running as root
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let uid = meta.uid();
        let gid = meta.gid();
        // Ignoring errors for chown if not root
        let _ = nix::unistd::chown(
            dst,
            Some(nix::unistd::Uid::from_raw(uid)),
            Some(nix::unistd::Gid::from_raw(gid)),
        );
    }

    // Set mtime
    let mtime = FileTime::from_last_modification_time(&meta);
    let atime = FileTime::from_last_access_time(&meta);
    set_file_times(dst, atime, mtime)?;

    Ok(())
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
pub fn calculate_total_size(path: &Path, ignores: &[String]) -> std::io::Result<u64> {
    use ignore::WalkBuilder;
    use ignore::overrides::OverrideBuilder;

    let mut override_builder = OverrideBuilder::new(path);
    for pattern in ignores {
        override_builder
            .add(&format!("!{pattern}"))
            .map_err(std::io::Error::other)?;
    }
    let overrides = override_builder.build().map_err(std::io::Error::other)?;

    let walker = WalkBuilder::new(path)
        .hidden(false)
        .git_ignore(false)
        .git_global(false)
        .git_exclude(false)
        .ignore(false)
        .parents(false)
        .overrides(overrides)
        .build();

    let mut total = 0;
    for entry in walker {
        let entry = entry.map_err(std::io::Error::other)?;
        if entry.file_type().is_some_and(|ft| ft.is_file()) {
            let meta = entry.metadata().map_err(std::io::Error::other)?;
            total += meta.len();
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
    ignores: &[String],
) -> anyhow::Result<()> {
    println!("Calculating total size for {}...", src_dir.display());
    let total_size = calculate_total_size(src_dir, ignores)?;

    let pb = Arc::new(tools::create_progress_bar(total_size));

    sync_dir_recursive(
        src_dir,
        dst_dir,
        threshold,
        checksum,
        dry_run,
        ignores,
        Arc::clone(&pb),
    )
    .await?;

    pb.finish_with_message("Done");
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn sync_dir_recursive(
    src_dir: &Path,
    dst_dir: &Path,
    threshold: f32,
    checksum: bool,
    dry_run: bool,
    ignores: &[String],
    pb: Arc<ProgressBar>,
) -> anyhow::Result<()> {
    use ignore::WalkBuilder;
    use ignore::overrides::OverrideBuilder;

    // 1. Create destination root if it doesn't exist
    if !dst_dir.exists() {
        if dry_run {
            println!("(dry-run) create directory: {}", dst_dir.display());
        } else {
            tokio::fs::create_dir_all(dst_dir).await?;
        }
    }

    let semaphore = Arc::new(Semaphore::new(
        std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(8),
    ));

    let mut tasks = Vec::new();

    // Configure overrides for ignores
    let mut override_builder = OverrideBuilder::new(src_dir);
    for pattern in ignores {
        override_builder.add(&format!("!{pattern}"))?;
    }
    let overrides = override_builder.build()?;

    // We use WalkBuilder to handle all files and directories
    let walker = WalkBuilder::new(src_dir)
        .hidden(false)
        .git_ignore(false)
        .git_global(false)
        .git_exclude(false)
        .ignore(false)
        .parents(false)
        .overrides(overrides.clone())
        .build();

    for entry in walker {
        let entry = entry?;
        let src_path = entry.path();
        if src_path == src_dir {
            continue;
        }

        let rel_path = src_path.strip_prefix(src_dir)?;
        let dst_path = dst_dir.join(rel_path);
        let file_type = entry
            .file_type()
            .ok_or_else(|| anyhow::anyhow!("unknown file type"))?;

        if file_type.is_dir() {
            if !dst_path.exists() {
                if dry_run {
                    println!("(dry-run) create directory: {}", dst_path.display());
                } else {
                    tokio::fs::create_dir_all(&dst_path).await?;
                }
            }
        } else if file_type.is_symlink() {
            let target = tokio::fs::read_link(src_path).await?;
            if dry_run {
                println!(
                    "(dry-run) symlink {} -> {}",
                    dst_path.display(),
                    target.display()
                );
            } else {
                if dst_path.exists() {
                    tokio::fs::remove_file(&dst_path).await?;
                }
                tokio::fs::symlink(&target, &dst_path).await?;
                apply_metadata(src_path, &dst_path)?;
            }
        } else if file_type.is_file() {
            let sem = Arc::clone(&semaphore);
            let src = src_path.to_path_buf();
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
                    println!("(dry-run) sync file: {} ({src_size} bytes)", src.display());
                    pb_clone.inc(src_size);
                    return Ok::<(), anyhow::Error>(());
                }

                pb_clone.set_message(format!("{}", src.display()));
                let full_copy = tools::should_use_full_copy(&src, &dst, threshold).await?;
                sync_changed_blocks_with_pb(&src, &dst, full_copy, pb_clone).await?;
                apply_metadata(&src, &dst)?;
                Ok::<(), anyhow::Error>(())
            }));
        }
    }

    for task in tasks {
        task.await??;
    }

    // Final pass: Apply metadata to all entries (directories, files, etc)
    let walker = WalkBuilder::new(src_dir)
        .hidden(false)
        .git_ignore(false)
        .git_global(false)
        .git_exclude(false)
        .ignore(false)
        .parents(false)
        .overrides(overrides)
        .build();

    if !dry_run {
        // Collect directories to apply them bottom-up
        let mut all_entries = Vec::new();
        for entry in walker {
            all_entries.push(entry?.path().to_path_buf());
        }

        // Apply metadata from deepest to shallowest
        for src_path in all_entries.iter().rev() {
            let rel_path = src_path.strip_prefix(src_dir)?;
            let dst_path = dst_dir.join(rel_path);
            apply_metadata(src_path, &dst_path)?;
        }
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
    let chunk_size: u64 = 1024 * 1024; // 1MB chunks for parallel processing

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
    let stats = sync_changed_blocks_with_pb(src_path, dst_path, full_copy, pb).await?;
    apply_metadata(src_path, dst_path)?;
    Ok(stats)
}
