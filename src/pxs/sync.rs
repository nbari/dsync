use crate::pxs::tools;
use anyhow::Context;
use filetime::{FileTime, set_file_times};
use indicatif::ProgressBar;
use std::{
    hash::Hasher,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::Semaphore;
use twox_hash::XxHash64;

const BLOCK_SIZE: usize = 128 * 1024;

#[derive(Debug, Default, Clone, Copy)]
pub struct SyncStats {
    pub total_blocks: usize,
    pub updated_blocks: usize,
}

struct DirectorySyncContext<'a> {
    src_dir: &'a Path,
    dst_dir: &'a Path,
    threshold: f32,
    checksum: bool,
    dry_run: bool,
    ignores: &'a [String],
    pb: Arc<ProgressBar>,
    fsync: bool,
}

struct DirectoryWalkState {
    directory_paths: Vec<PathBuf>,
    tasks: Vec<tokio::task::JoinHandle<anyhow::Result<()>>>,
    semaphore: Arc<Semaphore>,
}

struct WorkerContext {
    chunk_size: u64,
    src_len: u64,
    src: Arc<std::fs::File>,
    dst: Arc<std::fs::File>,
    pb: Arc<ProgressBar>,
    semaphore: Arc<Semaphore>,
    full_copy: bool,
}

impl DirectoryWalkState {
    fn new() -> Self {
        Self {
            directory_paths: Vec::new(),
            tasks: Vec::new(),
            semaphore: Arc::new(Semaphore::new(
                std::thread::available_parallelism()
                    .map(std::num::NonZeroUsize::get)
                    .unwrap_or(8),
            )),
        }
    }
}

/// Apply metadata (mode, uid, gid, mtime) from src to dst
///
/// # Errors
///
/// Returns an error if any attribute fails to be applied.
pub fn apply_metadata(src: &Path, dst: &Path) -> anyhow::Result<()> {
    let meta = std::fs::symlink_metadata(src).context("failed to read source metadata")?;
    let permissions = meta.permissions();
    if !meta.file_type().is_symlink() {
        std::fs::set_permissions(dst, permissions)
            .context("failed to set destination permissions")?;
    }

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
    if meta.file_type().is_symlink() {
        filetime::set_symlink_file_times(dst, atime, mtime)
            .context("failed to set destination symlink times")?;
    } else {
        set_file_times(dst, atime, mtime).context("failed to set destination file times")?;
    }

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
    fsync: bool,
) -> anyhow::Result<()> {
    eprintln!("Calculating total size for {}...", src_dir.display());
    let total_size = calculate_total_size(src_dir, ignores)?;

    let pb = Arc::new(tools::create_progress_bar(total_size));
    let context = DirectorySyncContext {
        src_dir,
        dst_dir,
        threshold,
        checksum,
        dry_run,
        ignores,
        pb: Arc::clone(&pb),
        fsync,
    };

    sync_dir_recursive(&context).await?;

    pb.finish_with_message("Done");
    Ok(())
}

fn build_overrides(
    src_dir: &Path,
    ignores: &[String],
) -> anyhow::Result<ignore::overrides::Override> {
    use ignore::overrides::OverrideBuilder;

    let mut override_builder = OverrideBuilder::new(src_dir);
    for pattern in ignores {
        override_builder.add(&format!("!{pattern}"))?;
    }
    Ok(override_builder.build()?)
}

fn build_walker(src_dir: &Path, overrides: ignore::overrides::Override) -> ignore::Walk {
    use ignore::WalkBuilder;

    WalkBuilder::new(src_dir)
        .hidden(false)
        .git_ignore(false)
        .git_global(false)
        .git_exclude(false)
        .ignore(false)
        .parents(false)
        .overrides(overrides)
        .build()
}

async fn ensure_destination_root(context: &DirectorySyncContext<'_>) -> anyhow::Result<()> {
    if context.dst_dir.exists() {
        return Ok(());
    }

    if context.dry_run {
        eprintln!("(dry-run) create directory: {}", context.dst_dir.display());
    } else {
        tokio::fs::create_dir_all(context.dst_dir).await?;
    }

    Ok(())
}

async fn handle_directory_entry(
    context: &DirectorySyncContext<'_>,
    state: &mut DirectoryWalkState,
    src_path: &Path,
    dst_path: &Path,
) -> anyhow::Result<()> {
    state.directory_paths.push(src_path.to_path_buf());
    if !dst_path.exists() {
        if context.dry_run {
            eprintln!("(dry-run) create directory: {}", dst_path.display());
        } else {
            tokio::fs::create_dir_all(dst_path).await?;
        }
    }

    Ok(())
}

async fn handle_symlink_entry(
    context: &DirectorySyncContext<'_>,
    src_path: &Path,
    dst_path: &Path,
) -> anyhow::Result<()> {
    let target = tokio::fs::read_link(src_path).await?;
    if context.dry_run {
        eprintln!(
            "(dry-run) symlink {} -> {}",
            dst_path.display(),
            target.display()
        );
        return Ok(());
    }

    if dst_path.exists() {
        if dst_path.is_dir() {
            tokio::fs::remove_dir_all(dst_path).await?;
        } else {
            tokio::fs::remove_file(dst_path).await?;
        }
    }
    tokio::fs::symlink(&target, dst_path).await?;
    apply_metadata(src_path, dst_path)?;
    Ok(())
}

fn spawn_file_sync_task(
    context: &DirectorySyncContext<'_>,
    semaphore: Arc<Semaphore>,
    src: PathBuf,
    dst: PathBuf,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    let pb = Arc::clone(&context.pb);
    let checksum = context.checksum;
    let dry_run = context.dry_run;
    let threshold = context.threshold;
    let fsync = context.fsync;

    tokio::spawn(async move {
        let _permit = semaphore
            .acquire()
            .await
            .map_err(|e| anyhow::anyhow!("failed to acquire semaphore: {e}"))?;

        if tools::should_skip_file(&src, &dst, checksum).await? {
            let src_size = tools::get_file_size(&src).await?;
            pb.inc(src_size);
            return Ok(());
        }

        if dry_run {
            let src_size = tools::get_file_size(&src).await?;
            eprintln!("(dry-run) sync file: {} ({src_size} bytes)", src.display());
            pb.inc(src_size);
            return Ok(());
        }

        pb.set_message(src.display().to_string());
        let full_copy = tools::should_use_full_copy(&src, &dst, threshold).await?;
        sync_changed_blocks_with_pb(&src, &dst, full_copy, pb, fsync).await?;
        apply_metadata(&src, &dst)?;
        Ok(())
    })
}

async fn handle_walk_entry(
    context: &DirectorySyncContext<'_>,
    state: &mut DirectoryWalkState,
    entry: ignore::DirEntry,
) -> anyhow::Result<()> {
    let src_path = entry.path();
    if src_path == context.src_dir {
        return Ok(());
    }

    let rel_path = src_path.strip_prefix(context.src_dir)?;
    let dst_path = context.dst_dir.join(rel_path);
    let file_type = entry
        .file_type()
        .ok_or_else(|| anyhow::anyhow!("unknown file type"))?;

    if file_type.is_dir() {
        return handle_directory_entry(context, state, src_path, &dst_path).await;
    }

    if file_type.is_symlink() {
        return handle_symlink_entry(context, src_path, &dst_path).await;
    }

    if file_type.is_file() {
        state.tasks.push(spawn_file_sync_task(
            context,
            Arc::clone(&state.semaphore),
            src_path.to_path_buf(),
            dst_path,
        ));
    }

    Ok(())
}

async fn wait_for_sync_tasks(
    tasks: Vec<tokio::task::JoinHandle<anyhow::Result<()>>>,
) -> anyhow::Result<()> {
    for task in tasks {
        task.await??;
    }
    Ok(())
}

fn apply_directory_metadata(
    context: &DirectorySyncContext<'_>,
    mut directory_paths: Vec<PathBuf>,
) -> anyhow::Result<()> {
    if context.dry_run {
        return Ok(());
    }

    // Apply metadata to directories from deepest to shallowest to ensure mtimes are preserved.
    directory_paths.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for src_path in directory_paths {
        let rel_path = src_path.strip_prefix(context.src_dir)?;
        let dst_path = context.dst_dir.join(rel_path);
        apply_metadata(&src_path, &dst_path)?;
    }
    apply_metadata(context.src_dir, context.dst_dir)?;

    Ok(())
}

async fn sync_dir_recursive(context: &DirectorySyncContext<'_>) -> anyhow::Result<()> {
    ensure_destination_root(context).await?;

    let overrides = build_overrides(context.src_dir, context.ignores)?;
    let walker = build_walker(context.src_dir, overrides);
    let mut state = DirectoryWalkState::new();

    for entry in walker {
        handle_walk_entry(context, &mut state, entry?).await?;
    }

    wait_for_sync_tasks(state.tasks).await?;
    apply_directory_metadata(context, state.directory_paths)?;
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
    fsync: bool,
) -> anyhow::Result<SyncStats> {
    let src_file = std::fs::File::open(src_path)
        .with_context(|| format!("failed to open source file: {}", src_path.display()))?;

    let src_len = src_file
        .metadata()
        .context("failed to get source metadata")?
        .len();

    if dst_path.is_dir() {
        tokio::fs::remove_dir_all(dst_path).await?;
    }

    // Pre-allocate space
    tools::preallocate(dst_path, src_len).context("failed to pre-allocate destination space")?;

    let dst_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dst_path)
        .with_context(|| format!("failed to open destination file: {}", dst_path.display()))?;

    let dst_len = dst_file
        .metadata()
        .context("failed to get destination metadata")?
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

    let concurrency = std::thread::available_parallelism()
        .map(std::num::NonZero::get)
        .unwrap_or(8);

    let semaphore = Arc::new(Semaphore::new(concurrency));
    let chunk_size: u64 = 1024 * 1024; // 1MB chunks for parallel processing
    let worker_context = Arc::new(WorkerContext {
        chunk_size,
        src_len,
        src: Arc::clone(&src_arc),
        dst: Arc::clone(&dst_arc),
        pb: Arc::clone(&pb),
        semaphore: Arc::clone(&semaphore),
        full_copy,
    });
    let mut handles = Vec::new();
    let num_chunks = src_len.div_ceil(chunk_size);

    for i in 0..num_chunks {
        let handle = spawn_sync_worker(i, Arc::clone(&worker_context));
        handles.push(handle);
    }

    let mut stats = SyncStats::default();
    for handle in handles {
        let chunk_stats = handle
            .await
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

    if fsync {
        dst_arc
            .sync_all()
            .context("failed to sync destination file to disk")?;
    }

    Ok(stats)
}

fn spawn_sync_worker(
    chunk_index: u64,
    context: Arc<WorkerContext>,
) -> tokio::task::JoinHandle<anyhow::Result<SyncStats>> {
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
                    let dst_read_result = worker_context.dst.read_exact_at(dst_chunk, offset);
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
                    worker_context
                        .dst
                        .write_all_at(src_chunk, offset)
                        .context("failed to write to destination")?;
                    chunk_updated += 1;
                }

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

/// Synchronize changed blocks
///
/// # Errors
///
/// Returns an error if any IO operation fails.
pub async fn sync_changed_blocks(
    src_path: &Path,
    dst_path: &Path,
    full_copy: bool,
    fsync: bool,
) -> anyhow::Result<SyncStats> {
    let pb = Arc::new(ProgressBar::hidden());
    let stats = sync_changed_blocks_with_pb(src_path, dst_path, full_copy, pb, fsync).await?;
    apply_metadata(src_path, dst_path)?;
    Ok(stats)
}
