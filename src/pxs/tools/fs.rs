use crate::pxs::tools::probes::{record_synced_parent_target, record_synced_path};
use anyhow::Result;
use std::path::Path;

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

        if let Ok(size_i64) = i64::try_from(size)
            && let Err(nix::errno::Errno::ENOSPC) =
                fallocate(&file, FallocateFlags::empty(), 0, size_i64)
        {
            anyhow::bail!("Disk full: not enough space to pre-allocate destination file");
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = file;
        let _ = size;
    }

    Ok(())
}

/// Fsync the parent directory of `path`.
///
/// # Errors
///
/// Returns an error if the parent directory cannot be opened or synced.
pub fn sync_parent_directory(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("path has no parent directory: {}", path.display()))?;
    let directory = std::fs::File::open(parent)?;
    directory.sync_all()?;
    record_synced_parent_target(path);
    Ok(())
}

/// Fsync the path itself.
///
/// # Errors
///
/// Returns an error if the path cannot be opened or synced.
pub fn sync_path(path: &Path) -> Result<()> {
    let handle = std::fs::File::open(path)?;
    handle.sync_all()?;
    record_synced_path(path);
    Ok(())
}

/// Fsync a directory and then fsync its parent directory entry.
///
/// # Errors
///
/// Returns an error if either sync operation fails.
pub fn sync_directory_and_parent(path: &Path) -> Result<()> {
    sync_path(path)?;
    sync_parent_directory(path)?;
    Ok(())
}
