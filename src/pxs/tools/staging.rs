use crate::pxs::tools::{
    fs::preallocate,
    probes::{record_staged_copy_fallback, record_staged_seed_invocation},
};
use anyhow::{Context, Result};
use std::{
    io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;

#[cfg(target_os = "linux")]
use crate::pxs::tools::probes::{record_staged_clone_attempt, record_staged_clone_success};

static STAGED_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Best-effort clone of `src` into `dst`.
///
/// On Linux this uses `FICLONE` so delta-safe staging stays cheap on copy-on-write
/// filesystems. Other platforms fall back to an ordinary copy.
#[cfg(target_os = "linux")]
fn try_clone_existing_file(src: &std::fs::File, dst: &std::fs::File) -> io::Result<bool> {
    record_staged_clone_attempt();
    let clone_result =
        unsafe { nix::libc::ioctl(dst.as_raw_fd(), nix::libc::FICLONE as _, src.as_raw_fd()) };
    if clone_result == 0 {
        record_staged_clone_success();
        return Ok(true);
    }

    let error = io::Error::last_os_error();
    if matches!(
        error.raw_os_error(),
        Some(code)
            if code == nix::libc::EOPNOTSUPP
                || code == nix::libc::EXDEV
                || code == nix::libc::EINVAL
                || code == nix::libc::ENOTTY
                || code == nix::libc::ENOSYS
                || code == nix::libc::EPERM
    ) {
        return Ok(false);
    }

    Err(error)
}

#[cfg(not(target_os = "linux"))]
fn try_clone_existing_file(_src: &std::fs::File, _dst: &std::fs::File) -> io::Result<bool> {
    Ok(false)
}

/// Seed a staging file from the current destination contents.
///
/// This first attempts a cheap filesystem clone and falls back to a byte copy.
/// If the destination disappears between scheduling and staging, the caller can
/// continue as a full copy from an empty file.
fn seed_staged_file(staged: &mut std::fs::File, final_path: &Path) -> Result<()> {
    record_staged_seed_invocation();
    let mut existing = match std::fs::File::open(final_path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };

    if try_clone_existing_file(&existing, staged)? {
        return Ok(());
    }

    record_staged_copy_fallback();
    std::io::copy(&mut existing, staged)?;
    Ok(())
}

fn remove_path_if_exists(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.is_dir() {
                std::fs::remove_dir_all(path)?;
            } else {
                std::fs::remove_file(path)?;
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn unique_sibling_path(path: &Path, tag: &str) -> Result<PathBuf> {
    let parent = path.parent().ok_or_else(|| {
        anyhow::anyhow!(
            "path has no parent directory for sibling generation: {}",
            path.display()
        )
    })?;
    let file_name = path.file_name().ok_or_else(|| {
        anyhow::anyhow!(
            "path has no file name for sibling generation: {}",
            path.display()
        )
    })?;
    let file_name = file_name.to_string_lossy();

    loop {
        let counter = STAGED_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let candidate = parent.join(format!(
            ".{file_name}.pxs.{tag}.{pid}.{counter}.tmp",
            pid = std::process::id()
        ));
        match std::fs::symlink_metadata(&candidate) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(candidate),
            Err(error) => return Err(error.into()),
        }
    }
}

/// Temporary file used to stage an atomic replacement of a destination path.
#[derive(Debug)]
pub struct StagedFile {
    final_path: PathBuf,
    staged_path: PathBuf,
    committed: bool,
}

impl StagedFile {
    /// Create a new staging file descriptor for `final_path`.
    ///
    /// The temporary file is placed in the same directory so the final rename
    /// remains atomic on Unix filesystems.
    ///
    /// # Errors
    ///
    /// Returns an error if the destination has no parent directory or file name.
    pub fn new(final_path: &Path) -> Result<Self> {
        let parent = final_path.parent().ok_or_else(|| {
            anyhow::anyhow!(
                "destination has no parent directory: {}",
                final_path.display()
            )
        })?;
        let file_name = final_path.file_name().ok_or_else(|| {
            anyhow::anyhow!("destination has no file name: {}", final_path.display())
        })?;
        let file_name = file_name.to_string_lossy();

        loop {
            let counter = STAGED_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
            let staged_name = format!(
                ".{file_name}.pxs.{pid}.{counter}.tmp",
                pid = std::process::id()
            );
            let staged_path = parent.join(staged_name);
            match std::fs::symlink_metadata(&staged_path) {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(Self {
                        final_path: final_path.to_path_buf(),
                        staged_path,
                        committed: false,
                    });
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Return the on-disk path of the staging file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.staged_path
    }

    /// Create and initialize the staging file.
    ///
    /// # Errors
    ///
    /// Returns an error if the staging file cannot be created or initialized.
    pub fn prepare(&self, size: u64, seed_from_existing: bool) -> Result<()> {
        if let Some(parent) = self.staged_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create directory {}", parent.display()))?;
        }

        let mut staged = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&self.staged_path)
            .with_context(|| {
                format!(
                    "failed to create staged file {}",
                    self.staged_path.display()
                )
            })?;

        if seed_from_existing {
            seed_staged_file(&mut staged, &self.final_path)?;
        }

        drop(staged);
        preallocate(&self.staged_path, size)?;
        Ok(())
    }

    /// Atomically replace the destination with the staging file.
    ///
    /// # Errors
    ///
    /// Returns an error if the final path cannot be replaced.
    pub fn commit(&mut self) -> Result<()> {
        install_prepared_path(&self.staged_path, &self.final_path)?;
        self.committed = true;
        Ok(())
    }

    /// Remove the staging file if it still exists.
    ///
    /// # Errors
    ///
    /// Returns an error if cleanup fails for reasons other than the file already being absent.
    pub fn cleanup(&self) -> Result<()> {
        match std::fs::remove_file(&self.staged_path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

impl Drop for StagedFile {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self.cleanup();
        }
    }
}

#[derive(Debug)]
struct ReplacedEntry {
    original_path: PathBuf,
    backup_path: PathBuf,
    active: bool,
}

impl ReplacedEntry {
    fn move_aside(path: &Path) -> Result<Option<Self>> {
        match std::fs::symlink_metadata(path) {
            Ok(_) => {
                let backup_path = unique_sibling_path(path, "backup")?;
                std::fs::rename(path, &backup_path)?;
                Ok(Some(Self {
                    original_path: path.to_path_buf(),
                    backup_path,
                    active: true,
                }))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn restore(&mut self) -> Result<()> {
        if !self.active {
            return Ok(());
        }
        remove_path_if_exists(&self.original_path)?;
        std::fs::rename(&self.backup_path, &self.original_path)?;
        self.active = false;
        Ok(())
    }

    fn discard(&mut self) -> Result<()> {
        if !self.active {
            return Ok(());
        }
        remove_path_if_exists(&self.backup_path)?;
        self.active = false;
        Ok(())
    }
}

#[cfg(test)]
static REPLACEMENT_FAILURE_HOOK: std::sync::Mutex<bool> = std::sync::Mutex::new(false);

#[cfg(test)]
fn should_fail_after_backup_move() -> Result<bool> {
    let mut hook = REPLACEMENT_FAILURE_HOOK
        .lock()
        .map_err(|_| anyhow::anyhow!("replacement failure hook mutex poisoned"))?;
    if *hook {
        *hook = false;
        return Ok(true);
    }
    Ok(false)
}

#[cfg(test)]
pub(crate) fn arm_replacement_failure_hook() -> Result<()> {
    let mut hook = REPLACEMENT_FAILURE_HOOK
        .lock()
        .map_err(|_| anyhow::anyhow!("replacement failure hook mutex poisoned"))?;
    *hook = true;
    Ok(())
}

/// Create a temporary symlink in the same directory as `final_path`.
///
/// The returned path is ready to be installed with [`install_prepared_path`].
///
/// # Errors
///
/// Returns an error if the temporary symlink cannot be created.
pub fn create_prepared_symlink(target: &Path, final_path: &Path) -> Result<PathBuf> {
    use std::os::unix::fs::symlink;

    let prepared_path = unique_sibling_path(final_path, "symlink")?;
    if let Err(error) = symlink(target, &prepared_path) {
        let _ = remove_path_if_exists(&prepared_path);
        return Err(error.into());
    }
    Ok(prepared_path)
}

/// Atomically install a prepared replacement path at `final_path`.
///
/// If `final_path` currently names a directory, it is first moved aside and only
/// removed after the prepared path has been installed successfully. On failure,
/// the original directory is restored.
///
/// # Errors
///
/// Returns an error if the replacement cannot be installed.
pub fn install_prepared_path(prepared_path: &Path, final_path: &Path) -> Result<()> {
    let mut replaced = match std::fs::symlink_metadata(final_path) {
        Ok(meta) if meta.is_dir() => ReplacedEntry::move_aside(final_path)?,
        Ok(_) => None,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };

    #[cfg(test)]
    if replaced.is_some() && should_fail_after_backup_move()? {
        let _ = remove_path_if_exists(prepared_path);
        if let Some(replaced) = replaced.as_mut() {
            let _ = replaced.restore();
        }
        anyhow::bail!("injected replacement failure after moving conflicting destination aside");
    }

    if let Err(error) = std::fs::rename(prepared_path, final_path) {
        let _ = remove_path_if_exists(prepared_path);
        if let Some(replaced) = replaced.as_mut() {
            let _ = replaced.restore();
        }
        return Err(error.into());
    }

    if let Some(replaced) = replaced.as_mut() {
        replaced.discard()?;
    }
    Ok(())
}

/// Ensure that `path` exists as a directory without deleting conflicting entries
/// before the replacement directory is ready.
///
/// # Errors
///
/// Returns an error if the destination directory cannot be created safely.
pub fn ensure_directory_path(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.is_dir() => return Ok(()),
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(path)?;
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    }

    let temp_dir = unique_sibling_path(path, "dir")?;
    std::fs::create_dir(&temp_dir)?;
    let mut replaced = ReplacedEntry::move_aside(path)?
        .ok_or_else(|| anyhow::anyhow!("missing conflicting path for {}", path.display()))?;

    #[cfg(test)]
    if should_fail_after_backup_move()? {
        let _ = remove_path_if_exists(&temp_dir);
        let _ = replaced.restore();
        anyhow::bail!("injected replacement failure after moving conflicting destination aside");
    }

    if let Err(error) = std::fs::rename(&temp_dir, path) {
        let _ = remove_path_if_exists(&temp_dir);
        let _ = replaced.restore();
        return Err(error.into());
    }

    replaced.discard()?;
    Ok(())
}
