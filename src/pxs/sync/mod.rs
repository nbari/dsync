pub mod delete;
pub mod dir;
pub mod file;
pub mod meta;

use crate::pxs::tools;
use anyhow::Result;
use indicatif::{MultiProgress, ProgressBar};
use std::sync::Arc;
use std::{
    fmt,
    path::{Path, PathBuf},
};
use tokio::sync::Semaphore;

pub use self::meta::{apply_metadata, fast_hash_block};

/// Marker error used when a source path disappears during a directory sync.
#[derive(Debug)]
pub(crate) struct SourceEntryDisappeared {
    path: PathBuf,
    operation: &'static str,
}

impl SourceEntryDisappeared {
    fn new(path: &Path, operation: &'static str) -> Self {
        Self {
            path: path.to_path_buf(),
            operation,
        }
    }
}

impl fmt::Display for SourceEntryDisappeared {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "source entry disappeared while {}: {}",
            self.operation,
            self.path.display()
        )
    }
}

impl std::error::Error for SourceEntryDisappeared {}

/// Convert a source-side I/O error into a typed transient disappearance when appropriate.
pub(crate) fn classify_source_io_error(
    error: std::io::Error,
    path: &Path,
    operation: &'static str,
) -> anyhow::Error {
    if error.kind() == std::io::ErrorKind::NotFound {
        return SourceEntryDisappeared::new(path, operation).into();
    }

    error.into()
}

/// Convert an arbitrary source-side error into a typed transient disappearance when appropriate.
pub(crate) fn classify_source_anyhow_error(
    error: anyhow::Error,
    path: &Path,
    operation: &'static str,
) -> anyhow::Error {
    if error
        .downcast_ref::<std::io::Error>()
        .is_some_and(|io_error| io_error.kind() == std::io::ErrorKind::NotFound)
    {
        return SourceEntryDisappeared::new(path, operation).into();
    }

    error
}

/// Return true when an error represents a transiently vanished source entry.
pub(crate) fn is_source_entry_disappeared(error: &anyhow::Error) -> bool {
    error.downcast_ref::<SourceEntryDisappeared>().is_some()
}

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
    tools::ensure_no_symlink_ancestors(dst_path)?;
    let pb: Arc<dyn tools::ProgressBarLike> = if quiet {
        Arc::new(ProgressBar::hidden())
    } else {
        let size = tools::get_file_size(src_path).await?;
        Arc::new(tools::create_progress_bar(size))
    };
    file::sync_changed_blocks_with_pb(src_path, dst_path, full_copy, pb, fsync, None).await
}

#[cfg(test)]
mod tests {
    use super::{classify_source_anyhow_error, is_source_entry_disappeared};
    use std::{io, path::Path};

    #[test]
    fn test_classify_source_anyhow_error_marks_not_found_as_disappeared() {
        let error = classify_source_anyhow_error(
            io::Error::from(io::ErrorKind::NotFound).into(),
            Path::new("missing.bin"),
            "reading source file size",
        );

        assert!(is_source_entry_disappeared(&error));
    }

    #[test]
    fn test_classify_source_anyhow_error_preserves_non_not_found_errors() {
        let error = classify_source_anyhow_error(
            io::Error::from(io::ErrorKind::PermissionDenied).into(),
            Path::new("source.bin"),
            "reading source file size",
        );

        assert!(!is_source_entry_disappeared(&error));
        assert_eq!(
            error.downcast_ref::<io::Error>().map(io::Error::kind),
            Some(io::ErrorKind::PermissionDenied)
        );
    }
}
