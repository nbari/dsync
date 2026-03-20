pub mod delete;
pub mod dir;
pub mod file;
pub mod meta;

use crate::pxs::tools;
use anyhow::Result;
use indicatif::{MultiProgress, ProgressBar};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Semaphore;

pub use self::meta::{apply_metadata, fast_hash_block};

#[derive(Debug, Default, Clone, Copy)]
pub struct SyncStats {
    pub total_blocks: usize,
    pub updated_blocks: usize,
}

/// Options for directory synchronization.
#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)]
pub struct SyncOptions {
    pub threshold: f32,
    pub checksum: bool,
    pub dry_run: bool,
    pub delete: bool,
    pub ignores: Vec<String>,
    pub fsync: bool,
    pub quiet: bool,
}

impl SyncOptions {
    /// Create a new `SyncOptions` builder from basic Sync parameters.
    #[must_use]
    #[allow(clippy::fn_params_excessive_bools)]
    pub fn new(
        threshold: f32,
        checksum: bool,
        dry_run: bool,
        delete: bool,
        ignores: Vec<String>,
        fsync: bool,
        quiet: bool,
    ) -> Self {
        Self {
            threshold,
            checksum,
            dry_run,
            delete,
            ignores,
            fsync,
            quiet,
        }
    }
}

/// Synchronize a directory.
///
/// # Errors
///
/// Returns an error if any synchronization task fails.
pub async fn sync_dir(src_dir: &Path, dst_dir: &Path, options: &SyncOptions) -> Result<SyncStats> {
    if !options.quiet {
        eprintln!("Calculating total size for {}...", src_dir.display());
    }
    let total_size = dir::calculate_total_size(src_dir, &options.ignores)?;

    let multi_progress = Arc::new(MultiProgress::new());
    let main_pb = if options.quiet {
        ProgressBar::hidden()
    } else {
        multi_progress.add(tools::create_progress_bar(total_size))
    };
    let main_pb = Arc::new(main_pb);
    let block_semaphore = Arc::new(Semaphore::new(tools::clamped_parallelism()));

    let context = dir::DirectorySyncContext {
        src_dir,
        dst_dir,
        options,
        main_pb: Arc::clone(&main_pb),
        multi_progress: Arc::clone(&multi_progress),
        block_semaphore,
    };

    let stats = dir::sync_dir_recursive(&context).await?;

    if !options.quiet {
        main_pb.finish_with_message("Done");
    }
    Ok(stats)
}

/// Synchronize changed blocks for a single file.
///
/// # Errors
///
/// Returns an error if any IO operation fails.
pub async fn sync_changed_blocks(
    src_path: &Path,
    dst_path: &Path,
    full_copy: bool,
    fsync: bool,
    quiet: bool,
) -> Result<SyncStats> {
    let pb: Arc<dyn tools::ProgressBarLike> = if quiet {
        Arc::new(ProgressBar::hidden())
    } else {
        let size = tools::get_file_size(src_path).await?;
        Arc::new(tools::create_progress_bar(size))
    };
    file::sync_changed_blocks_with_pb(src_path, dst_path, full_copy, pb, fsync, None).await
}
