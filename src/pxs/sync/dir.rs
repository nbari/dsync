use crate::pxs::sync::{SyncOptions, delete, file, meta};
use crate::pxs::tools::{self, clamped_parallelism};
use anyhow::{Context, Result};
use futures_util::{StreamExt, stream::FuturesUnordered};
use indicatif::{MultiProgress, ProgressBar};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::Semaphore;

pub(crate) struct DirectorySyncContext<'a> {
    pub src_dir: &'a Path,
    pub dst_dir: &'a Path,
    pub options: &'a SyncOptions,
    pub main_pb: Arc<ProgressBar>,
    pub multi_progress: Arc<MultiProgress>,
    pub block_semaphore: Arc<Semaphore>,
}

pub(crate) struct DirectoryWalkState {
    pub directory_paths: Vec<PathBuf>,
    pub tasks: FuturesUnordered<tokio::task::JoinHandle<Result<crate::pxs::sync::SyncStats>>>,
    pub max_in_flight: usize,
    pub stats: crate::pxs::sync::SyncStats,
}

impl DirectoryWalkState {
    pub fn new() -> Self {
        Self {
            directory_paths: Vec::new(),
            tasks: FuturesUnordered::new(),
            max_in_flight: clamped_parallelism(),
            stats: crate::pxs::sync::SyncStats::default(),
        }
    }
}

/// Calculate the total size of a directory.
///
/// # Errors
///
/// Returns an error if any IO operation fails.
pub fn calculate_total_size(path: &Path, ignores: &[String]) -> Result<u64> {
    use ignore::WalkBuilder;
    use ignore::overrides::OverrideBuilder;

    let mut override_builder = OverrideBuilder::new(path);
    for pattern in ignores {
        override_builder
            .add(&format!("!{pattern}"))
            .map_err(|e| anyhow::anyhow!("failed to add ignore pattern: {e}"))?;
    }
    let overrides = override_builder
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build overrides: {e}"))?;

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
        let entry = entry.map_err(|e| {
            anyhow::anyhow!("failed to walk source directory {}: {e}", path.display())
        })?;
        if entry.file_type().is_some_and(|ft| ft.is_file()) {
            let meta = entry.metadata().map_err(|e| {
                anyhow::anyhow!(
                    "failed to read metadata for {}: {e}",
                    entry.path().display()
                )
            })?;
            total += meta.len();
        }
    }
    Ok(total)
}

pub(crate) async fn sync_dir_recursive(
    context: &DirectorySyncContext<'_>,
) -> Result<crate::pxs::sync::SyncStats> {
    ensure_directory_exists(context, context.dst_dir).await?;

    let overrides = build_overrides(context.src_dir, &context.options.ignores)?;
    let walker = build_walker(context.src_dir, overrides);
    let mut state = DirectoryWalkState::new();

    let walk_result = async {
        for entry in walker {
            handle_walk_entry(context, &mut state, entry?).await?;
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;

    // Ensure all background sync tasks are awaited even if the loop fails
    let sync_stats = wait_for_sync_tasks(&mut state).await?;

    // Propagate the first error encountered
    walk_result?;

    if context.options.delete {
        delete::delete_extraneous_files(context).await?;
    }

    apply_directory_metadata(context, state.directory_paths)?;

    Ok(sync_stats)
}

fn build_overrides(src_dir: &Path, ignores: &[String]) -> Result<ignore::overrides::Override> {
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

async fn handle_directory_entry(
    context: &DirectorySyncContext<'_>,
    state: &mut DirectoryWalkState,
    src_path: &Path,
    dst_path: &Path,
) -> Result<()> {
    state.directory_paths.push(src_path.to_path_buf());
    ensure_directory_exists(context, dst_path).await
}

async fn ensure_directory_exists(
    context: &DirectorySyncContext<'_>,
    dst_path: &Path,
) -> Result<()> {
    tools::ensure_no_symlink_ancestors_under_root(context.dst_dir, dst_path)?;

    let dst_meta = tokio::fs::symlink_metadata(dst_path).await;
    let exists = dst_meta.is_ok();
    let is_dir = matches!(&dst_meta, Ok(m) if m.is_dir());

    if exists && !is_dir {
        if context.options.dry_run {
            eprintln!(
                "(dry-run) remove entry and create directory: {}",
                dst_path.display()
            );
        } else {
            tools::ensure_directory_path(dst_path)
                .with_context(|| format!("failed to create directory {}", dst_path.display()))?;
        }
    } else if !exists {
        if context.options.dry_run {
            eprintln!("(dry-run) create directory: {}", dst_path.display());
        } else {
            tokio::fs::create_dir_all(dst_path)
                .await
                .with_context(|| format!("failed to create directory {}", dst_path.display()))?;
        }
    }
    if context.options.fsync && !context.options.dry_run && dst_path.exists() {
        tools::sync_directory_and_parent(dst_path)?;
    }

    Ok(())
}

async fn handle_symlink_entry(
    context: &DirectorySyncContext<'_>,
    src_path: &Path,
    dst_path: &Path,
) -> Result<()> {
    tools::ensure_no_symlink_ancestors_under_root(context.dst_dir, dst_path)?;

    let target = tokio::fs::read_link(src_path)
        .await
        .with_context(|| format!("failed to read symlink target: {}", src_path.display()))?;
    if context.options.dry_run {
        eprintln!(
            "(dry-run) symlink {} -> {}",
            dst_path.display(),
            target.display()
        );
        return Ok(());
    }

    if let Some(parent) = dst_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    let prepared_path = tools::create_prepared_symlink(&target, dst_path).with_context(|| {
        format!(
            "failed to create staged symlink {} -> {}",
            dst_path.display(),
            target.display()
        )
    })?;
    if let Err(error) = meta::apply_metadata(src_path, &prepared_path) {
        let _ = std::fs::remove_file(&prepared_path);
        return Err(error);
    }
    tools::install_prepared_path(&prepared_path, dst_path).with_context(|| {
        format!(
            "failed to install symlink {} -> {}",
            dst_path.display(),
            target.display()
        )
    })?;
    if context.options.fsync {
        tools::sync_parent_directory(dst_path)?;
    }
    Ok(())
}

fn spawn_file_sync_task(
    context: &DirectorySyncContext<'_>,
    src: PathBuf,
    dst: PathBuf,
) -> tokio::task::JoinHandle<Result<crate::pxs::sync::SyncStats>> {
    let main_pb = Arc::clone(&context.main_pb);
    let multi_progress = Arc::clone(&context.multi_progress);
    let checksum = context.options.checksum;
    let dry_run = context.options.dry_run;
    let threshold = context.options.threshold;
    let fsync = context.options.fsync;
    let quiet = context.options.quiet;
    let block_semaphore = Arc::clone(&context.block_semaphore);

    tokio::spawn(async move {
        let src_size = tools::get_file_size(&src).await?;

        if tools::should_skip_file(&src, &dst, checksum).await? {
            main_pb.inc(src_size);
            let total_blocks = usize::try_from(src_size.div_ceil(128 * 1024)).unwrap_or(0);
            return Ok(crate::pxs::sync::SyncStats {
                total_blocks,
                updated_blocks: 0,
            });
        }

        if dry_run {
            if !quiet {
                eprintln!("(dry-run) sync file: {} ({src_size} bytes)", src.display());
            }
            main_pb.inc(src_size);
            let total_blocks = usize::try_from(src_size.div_ceil(128 * 1024)).unwrap_or(0);
            return Ok(crate::pxs::sync::SyncStats {
                total_blocks,
                updated_blocks: 0,
            });
        }

        // Create a sub-progressbar for files larger than a certain threshold to avoid screen clutter
        // For small files, the sub-progress bar is so fast it just creates flicker.
        // Let's say files larger than 1MB get their own bar.
        let file_pb = if src_size > 1024 * 1024 && !quiet {
            let pb = multi_progress.add(ProgressBar::new(src_size));
            pb.set_style(
                indicatif::ProgressStyle::default_bar()
                    .template("{spinner:.blue} [{bar:20.blue/white}] {bytes}/{total_bytes} {msg}")
                    .unwrap_or_else(|_| indicatif::ProgressStyle::default_bar())
                    .progress_chars("=>-"),
            );
            let msg = src
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            pb.set_message(msg);
            Arc::new(pb)
        } else {
            Arc::new(ProgressBar::hidden())
        };

        let full_copy = tools::should_use_full_copy_meta(src_size, &dst, threshold).await?;

        // We need a combined progress bar that increments BOTH the main dir pb and the file pb
        let combined_pb = Arc::new(tools::CombinedProgressBar::new(main_pb, file_pb.clone()));

        let result = file::sync_changed_blocks_with_pb(
            &src,
            &dst,
            full_copy,
            combined_pb,
            fsync,
            Some(block_semaphore),
        )
        .await;

        if !file_pb.is_hidden() {
            file_pb.finish_and_clear();
            multi_progress.remove(file_pb.as_ref());
        }

        result
    })
}

async fn handle_walk_entry(
    context: &DirectorySyncContext<'_>,
    state: &mut DirectoryWalkState,
    entry: ignore::DirEntry,
) -> Result<()> {
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
        tools::ensure_no_symlink_ancestors_under_root(context.dst_dir, &dst_path)?;
        if state.tasks.len() >= state.max_in_flight {
            let task_stats = wait_for_next_sync_task(&mut state.tasks).await?;
            state.stats.total_blocks += task_stats.total_blocks;
            state.stats.updated_blocks += task_stats.updated_blocks;
        }
        state.tasks.push(spawn_file_sync_task(
            context,
            src_path.to_path_buf(),
            dst_path,
        ));
    } else {
        tracing::debug!("skipping unsupported file type: {}", src_path.display());
    }

    Ok(())
}

async fn wait_for_next_sync_task(
    tasks: &mut FuturesUnordered<tokio::task::JoinHandle<Result<crate::pxs::sync::SyncStats>>>,
) -> Result<crate::pxs::sync::SyncStats> {
    let task_result = tasks
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("missing sync task"))?;
    let stats = task_result
        .map_err(|e| anyhow::anyhow!(e))
        .context("worker task panicked")??;
    Ok(stats)
}

async fn wait_for_sync_tasks(
    state: &mut DirectoryWalkState,
) -> Result<crate::pxs::sync::SyncStats> {
    while let Some(task_result) = state.tasks.next().await {
        let task_stats = task_result
            .map_err(|e| anyhow::anyhow!(e))
            .context("worker task panicked")??;
        state.stats.total_blocks += task_stats.total_blocks;
        state.stats.updated_blocks += task_stats.updated_blocks;
    }
    Ok(state.stats)
}

fn apply_directory_metadata(
    context: &DirectorySyncContext<'_>,
    mut directory_paths: Vec<PathBuf>,
) -> Result<()> {
    if context.options.dry_run {
        return Ok(());
    }

    // Apply metadata to directories from deepest to shallowest to ensure mtimes are preserved.
    directory_paths.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for src_path in directory_paths {
        let rel_path = src_path.strip_prefix(context.src_dir)?;
        let dst_path = context.dst_dir.join(rel_path);
        meta::apply_metadata(&src_path, &dst_path)?;
        if context.options.fsync {
            tools::sync_directory_and_parent(&dst_path)?;
        }
    }
    meta::apply_metadata(context.src_dir, context.dst_dir)?;
    if context.options.fsync {
        tools::sync_directory_and_parent(context.dst_dir)?;
    }

    Ok(())
}
