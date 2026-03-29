use anyhow::{Context, Result};
use filetime::{FileTime, set_file_times};
use std::hash::Hasher;
use std::path::Path;
use twox_hash::XxHash64;

/// Apply metadata (mode, uid, gid, mtime) from src to dst.
///
/// # Errors
///
/// Returns an error if any attribute fails to be applied.
pub fn apply_metadata(src: &Path, dst: &Path) -> Result<()> {
    let meta = std::fs::symlink_metadata(src).context("failed to read source metadata")?;
    let permissions = meta.permissions();
    if !meta.file_type().is_symlink() {
        std::fs::set_permissions(dst, permissions)
            .context("failed to set destination permissions")?;
    }

    // Set ownership if running as root
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::fs::MetadataExt;
        let uid = meta.uid();
        let gid = meta.gid();
        // Use lchown to avoid following symlinks
        let path = std::ffi::CString::new(dst.as_os_str().as_bytes()).unwrap_or_default();
        unsafe {
            let _ = nix::libc::lchown(path.as_ptr(), uid, gid);
        }
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
