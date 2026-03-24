use crate::pxs::sync;
use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use std::{
    io,
    ops::Range,
    os::unix::fs::{FileExt, MetadataExt},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::fs;

#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;

const THRESHOLD_SCALE: u128 = 1_000_000;
/// The maximum number of concurrent workers allowed across the system.
pub const MAX_PARALLELISM: usize = 64;
static STAGED_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);
const PROGRESS_CHARS: &str = "█▉▊▋▌▍▎▏  ·";
const PROGRESS_TICK_STRINGS: &[&str] = &["◜", "◠", "◝", "◞", "◡", "◟", "·"];

#[cfg(test)]
struct OptimizationProbeState {
    safe_mmap_successes: AtomicU64,
    sender_mmap_read_hits: AtomicU64,
    staged_seed_invocations: AtomicU64,
    staged_clone_attempts: AtomicU64,
    staged_clone_successes: AtomicU64,
    staged_copy_fallbacks: AtomicU64,
}

#[cfg(test)]
impl OptimizationProbeState {
    const fn new() -> Self {
        Self {
            safe_mmap_successes: AtomicU64::new(0),
            sender_mmap_read_hits: AtomicU64::new(0),
            staged_seed_invocations: AtomicU64::new(0),
            staged_clone_attempts: AtomicU64::new(0),
            staged_clone_successes: AtomicU64::new(0),
            staged_copy_fallbacks: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> test_support::OptimizationProbeSnapshot {
        test_support::OptimizationProbeSnapshot {
            safe_mmap_successes: self.safe_mmap_successes.load(Ordering::Relaxed),
            sender_mmap_read_hits: self.sender_mmap_read_hits.load(Ordering::Relaxed),
            staged_seed_invocations: self.staged_seed_invocations.load(Ordering::Relaxed),
            staged_clone_attempts: self.staged_clone_attempts.load(Ordering::Relaxed),
            staged_clone_successes: self.staged_clone_successes.load(Ordering::Relaxed),
            staged_copy_fallbacks: self.staged_copy_fallbacks.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
#[derive(Default)]
struct DurabilityProbeState {
    synced_paths: std::sync::Mutex<Vec<PathBuf>>,
    synced_parent_targets: std::sync::Mutex<Vec<PathBuf>>,
}

#[cfg(test)]
impl DurabilityProbeState {
    fn snapshot(&self) -> anyhow::Result<test_support::DurabilityProbeSnapshot> {
        let synced_paths = self
            .synced_paths
            .lock()
            .map_err(|_| anyhow::anyhow!("durability probe synced_paths mutex poisoned"))?
            .clone();
        let synced_parent_targets = self
            .synced_parent_targets
            .lock()
            .map_err(|_| anyhow::anyhow!("durability probe synced_parent_targets mutex poisoned"))?
            .clone();
        Ok(test_support::DurabilityProbeSnapshot {
            synced_paths,
            synced_parent_targets,
        })
    }
}

#[cfg(test)]
static ACTIVE_OPTIMIZATION_PROBE: std::sync::Mutex<Option<Arc<OptimizationProbeState>>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
static ACTIVE_DURABILITY_PROBE: std::sync::Mutex<Option<Arc<DurabilityProbeState>>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
fn with_optimization_probe<F>(record: F)
where
    F: FnOnce(&OptimizationProbeState),
{
    let probe = ACTIVE_OPTIMIZATION_PROBE
        .lock()
        .ok()
        .and_then(|guard| guard.clone());
    if let Some(probe) = probe {
        record(probe.as_ref());
    }
}

#[cfg(test)]
fn record_safe_mmap_success() {
    with_optimization_probe(|probe| {
        probe.safe_mmap_successes.fetch_add(1, Ordering::Relaxed);
    });
}

#[cfg(test)]
pub(crate) fn record_sender_mmap_read_hit() {
    with_optimization_probe(|probe| {
        probe.sender_mmap_read_hits.fetch_add(1, Ordering::Relaxed);
    });
}

#[cfg(not(test))]
pub(crate) fn record_sender_mmap_read_hit() {}

#[cfg(test)]
fn record_staged_seed_invocation() {
    with_optimization_probe(|probe| {
        probe
            .staged_seed_invocations
            .fetch_add(1, Ordering::Relaxed);
    });
}

#[cfg(test)]
fn record_staged_clone_attempt() {
    with_optimization_probe(|probe| {
        probe.staged_clone_attempts.fetch_add(1, Ordering::Relaxed);
    });
}

#[cfg(test)]
fn record_staged_clone_success() {
    with_optimization_probe(|probe| {
        probe.staged_clone_successes.fetch_add(1, Ordering::Relaxed);
    });
}

#[cfg(test)]
fn record_staged_copy_fallback() {
    with_optimization_probe(|probe| {
        probe.staged_copy_fallbacks.fetch_add(1, Ordering::Relaxed);
    });
}

#[cfg(test)]
fn with_durability_probe<F>(record: F)
where
    F: FnOnce(&DurabilityProbeState),
{
    let probe = ACTIVE_DURABILITY_PROBE
        .lock()
        .ok()
        .and_then(|guard| guard.clone());
    if let Some(probe) = probe {
        record(probe.as_ref());
    }
}

#[cfg(test)]
fn record_synced_path(path: &Path) {
    let path = path.to_path_buf();
    with_durability_probe(|probe| {
        if let Ok(mut paths) = probe.synced_paths.lock() {
            paths.push(path);
        }
    });
}

#[cfg(test)]
fn record_synced_parent_target(path: &Path) {
    let path = path.to_path_buf();
    with_durability_probe(|probe| {
        if let Ok(mut paths) = probe.synced_parent_targets.lock() {
            paths.push(path);
        }
    });
}

/// Hidden testing hooks for observing optimization paths without changing runtime behavior.
#[cfg(test)]
#[doc(hidden)]
pub mod test_support {
    use super::{
        ACTIVE_DURABILITY_PROBE, ACTIVE_OPTIMIZATION_PROBE, Arc, DurabilityProbeState,
        OptimizationProbeState, PathBuf,
    };

    /// Snapshot of optimization events recorded while a probe is active.
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub struct OptimizationProbeSnapshot {
        /// Number of successful `safe_mmap` creations.
        pub safe_mmap_successes: u64,
        /// Number of sender-side block reads served from an mmap.
        pub sender_mmap_read_hits: u64,
        /// Number of staged-file preparations seeded from an existing destination.
        pub staged_seed_invocations: u64,
        /// Number of filesystem clone attempts made while seeding a staged file.
        pub staged_clone_attempts: u64,
        /// Number of successful filesystem clones while seeding a staged file.
        pub staged_clone_successes: u64,
        /// Number of byte-copy fallbacks used after staged-file clone was unavailable.
        pub staged_copy_fallbacks: u64,
    }

    /// Snapshot of durability sync operations recorded while a probe is active.
    #[derive(Clone, Debug, Default, Eq, PartialEq)]
    pub struct DurabilityProbeSnapshot {
        /// Paths opened and synced directly.
        pub synced_paths: Vec<PathBuf>,
        /// Target paths whose parent directories were synced.
        pub synced_parent_targets: Vec<PathBuf>,
    }

    /// Guard that records optimization events during a scoped test.
    pub struct OptimizationProbe {
        state: Arc<OptimizationProbeState>,
    }

    impl OptimizationProbe {
        /// Start recording optimization events until this guard is dropped.
        ///
        /// # Errors
        ///
        /// Returns an error if another optimization probe is already active in
        /// the current process.
        pub fn start() -> anyhow::Result<Self> {
            let state = Arc::new(OptimizationProbeState::new());
            let mut active = ACTIVE_OPTIMIZATION_PROBE
                .lock()
                .map_err(|_| anyhow::anyhow!("optimization probe mutex poisoned"))?;
            if active.is_some() {
                anyhow::bail!("optimization probe already active");
            }
            *active = Some(Arc::clone(&state));
            Ok(Self { state })
        }

        /// Return the counters collected by this probe so far.
        #[must_use]
        pub fn snapshot(&self) -> OptimizationProbeSnapshot {
            self.state.snapshot()
        }
    }

    impl Drop for OptimizationProbe {
        fn drop(&mut self) {
            if let Ok(mut active) = ACTIVE_OPTIMIZATION_PROBE.lock() {
                *active = None;
            }
        }
    }

    /// Guard that records durability sync operations during a scoped test.
    pub struct DurabilityProbe {
        state: Arc<DurabilityProbeState>,
    }

    impl DurabilityProbe {
        /// Start recording durability sync operations until this guard is dropped.
        ///
        /// # Errors
        ///
        /// Returns an error if another durability probe is already active in the
        /// current process.
        pub fn start() -> anyhow::Result<Self> {
            let state = Arc::new(DurabilityProbeState::default());
            let mut active = ACTIVE_DURABILITY_PROBE
                .lock()
                .map_err(|_| anyhow::anyhow!("durability probe mutex poisoned"))?;
            if active.is_some() {
                anyhow::bail!("durability probe already active");
            }
            *active = Some(Arc::clone(&state));
            Ok(Self { state })
        }

        /// Return the durability sync operations collected by this probe so far.
        pub fn snapshot(&self) -> anyhow::Result<DurabilityProbeSnapshot> {
            self.state.snapshot()
        }
    }

    impl Drop for DurabilityProbe {
        fn drop(&mut self) {
            if let Ok(mut active) = ACTIVE_DURABILITY_PROBE.lock() {
                *active = None;
            }
        }
    }
}

#[cfg(not(test))]
fn record_safe_mmap_success() {}

#[cfg(not(test))]
fn record_staged_seed_invocation() {}

#[cfg(all(not(test), target_os = "linux"))]
fn record_staged_clone_attempt() {}

#[cfg(all(not(test), target_os = "linux"))]
fn record_staged_clone_success() {}

#[cfg(not(test))]
fn record_staged_copy_fallback() {}

#[cfg(not(test))]
fn record_synced_path(_path: &Path) {}

#[cfg(not(test))]
fn record_synced_parent_target(_path: &Path) {}

/// Return the number of logical CPU cores, clamped to a safe maximum.
///
/// If the OS fails to report the number of cores, it defaults to 1 (single-threaded)
/// to ensure safety on restricted or legacy systems.
#[must_use]
pub fn clamped_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1)
        .min(MAX_PARALLELISM)
}

fn metadata_mtime_nanos(meta: &std::fs::Metadata) -> Option<u32> {
    u32::try_from(meta.mtime_nsec()).ok()
}

fn scaled_threshold(threshold: f32) -> u128 {
    format!("{:.6}", threshold.clamp(0.0, 1.0))
        .replace('.', "")
        .parse::<u128>()
        .unwrap_or(0)
}

fn block_index(index: usize) -> anyhow::Result<u32> {
    u32::try_from(index).map_err(|e| anyhow::anyhow!(e))
}

fn block_offset(index: usize, block_size: u64) -> anyhow::Result<u64> {
    let block_index = u64::try_from(index).map_err(|e| anyhow::anyhow!(e))?;
    block_index
        .checked_mul(block_size)
        .ok_or_else(|| anyhow::anyhow!("block offset overflow"))
}

fn mmap_range(offset: u64, block_size: usize, len: usize) -> anyhow::Result<Option<Range<usize>>> {
    let start = usize::try_from(offset).map_err(|e| anyhow::anyhow!(e))?;
    if start >= len {
        return Ok(None);
    }

    let end = start.saturating_add(block_size).min(len);
    Ok(Some(start..end))
}

/// Create a copy-on-write memory map that protects against in-place modifications.
///
/// Uses `MAP_PRIVATE` which creates a private copy-on-write mapping. If the underlying
/// file is modified in-place, the process won't receive SIGBUS - it will just
/// see stale data, which is acceptable for read-only comparison and hashing.
/// **Note:** This does not protect against SIGBUS if the underlying file is
/// externally truncated on POSIX systems.
pub(crate) fn safe_mmap(file: &std::fs::File) -> Result<memmap2::Mmap, std::io::Error> {
    let mmap = unsafe { memmap2::MmapOptions::new().map_copy_read_only(file) };
    if mmap.is_ok() {
        record_safe_mmap_success();
    }
    mmap
}

/// A trait to allow abstracting over different progress bar types (e.g. standard vs combined)
pub trait ProgressBarLike: Send + Sync {
    fn inc(&self, delta: u64);
}

impl ProgressBarLike for ProgressBar {
    fn inc(&self, delta: u64) {
        self.inc(delta);
    }
}

/// A combined progress bar that increments two progress bars simultaneously.
pub struct CombinedProgressBar {
    pub main: Arc<ProgressBar>,
    pub sub: Arc<ProgressBar>,
}

impl CombinedProgressBar {
    #[must_use]
    pub fn new(main: Arc<ProgressBar>, sub: Arc<ProgressBar>) -> Self {
        Self { main, sub }
    }
}

impl ProgressBarLike for CombinedProgressBar {
    fn inc(&self, delta: u64) {
        self.main.inc(delta);
        self.sub.inc(delta);
    }
}

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
fn seed_staged_file(staged: &mut std::fs::File, final_path: &Path) -> anyhow::Result<()> {
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
    /// **Concurrency Note:** Staging filenames are unique per process and machine.
    /// If multiple processes on the same machine sync to the same directory,
    /// they are protected by PID and atomic counter. For concurrent processes
    /// on different machines sharing a filesystem (e.g. NFS), collision is prevented
    /// by `create_new(true)` in `prepare()`.
    ///
    /// # Errors
    ///
    /// Returns an error if the destination has no parent directory or file name.
    pub fn new(final_path: &Path) -> anyhow::Result<Self> {
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
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(Self {
                        final_path: final_path.to_path_buf(),
                        staged_path,
                        committed: false,
                    });
                }
                Err(e) => return Err(e.into()),
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
    /// When `seed_from_existing` is true, the current destination contents are
    /// cloned or copied into the staging file before any block updates are applied.
    ///
    /// # Errors
    ///
    /// Returns an error if the staging file cannot be created or initialized.
    pub fn prepare(&self, size: u64, seed_from_existing: bool) -> anyhow::Result<()> {
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
    pub fn commit(&mut self) -> anyhow::Result<()> {
        install_prepared_path(&self.staged_path, &self.final_path)?;
        self.committed = true;
        Ok(())
    }

    /// Remove the staging file if it still exists.
    ///
    /// # Errors
    ///
    /// Returns an error if cleanup fails for reasons other than the file already
    /// being absent.
    pub fn cleanup(&self) -> anyhow::Result<()> {
        match std::fs::remove_file(&self.staged_path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
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

fn mmap_block_matches(
    mmap: &memmap2::Mmap,
    offset: u64,
    block_size: usize,
    expected_hash: u64,
) -> anyhow::Result<bool> {
    let Some(range) = mmap_range(offset, block_size, mmap.len())? else {
        return Ok(false);
    };

    Ok(mmap
        .get(range)
        .is_some_and(|chunk| sync::fast_hash_block(chunk) == expected_hash))
}

fn read_block_matches(
    file: &std::fs::File,
    buffer: &mut [u8],
    offset: u64,
    expected_hash: u64,
) -> anyhow::Result<bool> {
    let bytes_read = file.read_at(buffer, offset)?;
    if bytes_read == 0 {
        return Ok(false);
    }

    let chunk = buffer
        .get(..bytes_read)
        .ok_or_else(|| anyhow::anyhow!("read chunk exceeds buffer"))?;
    Ok(sync::fast_hash_block(chunk) == expected_hash)
}

/// Create a standardized progress bar for pxs
#[must_use]
pub fn create_progress_bar(total_size: u64) -> ProgressBar {
    let pb = ProgressBar::new(total_size);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.green/blue}] {bytes}/{total_bytes} ({percent}%) {binary_bytes_per_sec} {eta_precise} {msg}",
            )
            .unwrap_or_else(|_| ProgressStyle::default_bar())
            .progress_chars(PROGRESS_CHARS)
            .tick_strings(PROGRESS_TICK_STRINGS),
    );
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb
}

/// Get the size of a file
///
/// # Errors
///
/// Returns an error if the metadata cannot be read.
pub async fn get_file_size(path: &Path) -> Result<u64> {
    Ok(fs::metadata(path).await?.len())
}

/// Return true if the file should be skipped based on metadata
///
/// # Errors
///
/// Returns an error if the metadata cannot be read.
pub async fn should_skip_file(src: &Path, dst: &Path, checksum: bool) -> Result<bool> {
    // 1. If destination doesn't exist — don't skip
    if !dst.exists() {
        return Ok(false);
    }

    // 2. Compare sizes
    let src_meta = tokio::fs::metadata(src).await?;
    let dst_meta = tokio::fs::metadata(dst).await?;
    let src_size = src_meta.len();
    let dst_size = dst_meta.len();

    if src_size != dst_size {
        return Ok(false);
    }

    // If checksum is NOT requested, rely on mtime
    if !checksum {
        return Ok(src_meta.mtime() == dst_meta.mtime()
            && metadata_mtime_nanos(&src_meta) == metadata_mtime_nanos(&dst_meta));
    }

    // If checksum is requested and size is same, we still need to check contents
    // This is handled by the caller (sync_changed_blocks)
    Ok(false)
}

/// Return true if full copy is more efficient than block comparison.
///
/// # Errors
///
/// Returns an error if metadata cannot be read.
pub async fn should_use_full_copy(src: &Path, dst: &Path, threshold: f32) -> Result<bool> {
    if !dst.exists() {
        return Ok(true);
    }

    let src_meta = fs::metadata(src).await?;
    should_use_full_copy_meta(src_meta.len(), dst, threshold).await
}

/// Return true if full copy is more efficient than block comparison, using provided source size.
///
/// # Errors
///
/// Returns an error if metadata cannot be read.
pub async fn should_use_full_copy_meta(src_size: u64, dst: &Path, threshold: f32) -> Result<bool> {
    if !dst.exists() {
        return Ok(true);
    }

    if src_size == 0 {
        return Ok(false);
    }

    let dst_meta = fs::metadata(dst).await?;
    let dst_size = dst_meta.len();

    // If file is large and destination exists, prefer delta-sync (resume) even if destination is small
    // because hashing is fast and network/disk-write is slow.
    // Heuristic: if file > 1MB, we use a much more aggressive threshold for full copy.
    let threshold = if src_size > 1024 * 1024 {
        threshold.min(0.1)
    } else {
        threshold
    };

    Ok(is_below_threshold(src_size, dst_size, threshold))
}

/// Compare destination/source size ratio against threshold.
///
/// Returns true if `dst_size / src_size < threshold`, meaning the destination
/// is too small relative to the source and a full copy should be performed.
#[must_use]
pub fn is_below_threshold(src_size: u64, dst_size: u64, threshold: f32) -> bool {
    if src_size == 0 {
        return false;
    }

    u128::from(dst_size) * THRESHOLD_SCALE < u128::from(src_size) * scaled_threshold(threshold)
}

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
        // Ignore other errors (e.g. EOPNOTSUPP on FAT/some NFS) and fallback to standard writes
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
pub fn sync_parent_directory(path: &Path) -> anyhow::Result<()> {
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
pub fn sync_path(path: &Path) -> anyhow::Result<()> {
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
pub fn sync_directory_and_parent(path: &Path) -> anyhow::Result<()> {
    sync_path(path)?;
    sync_parent_directory(path)?;
    Ok(())
}

fn remove_path_if_exists(path: &Path) -> anyhow::Result<()> {
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

fn unique_sibling_path(path: &Path, tag: &str) -> anyhow::Result<PathBuf> {
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

#[derive(Debug)]
struct ReplacedEntry {
    original_path: PathBuf,
    backup_path: PathBuf,
    active: bool,
}

impl ReplacedEntry {
    fn move_aside(path: &Path) -> anyhow::Result<Option<Self>> {
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

    fn restore(&mut self) -> anyhow::Result<()> {
        if !self.active {
            return Ok(());
        }
        remove_path_if_exists(&self.original_path)?;
        std::fs::rename(&self.backup_path, &self.original_path)?;
        self.active = false;
        Ok(())
    }

    fn discard(&mut self) -> anyhow::Result<()> {
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
fn should_fail_after_backup_move() -> anyhow::Result<bool> {
    let mut hook = REPLACEMENT_FAILURE_HOOK
        .lock()
        .map_err(|_| anyhow::anyhow!("replacement failure hook mutex poisoned"))?;
    if *hook {
        *hook = false;
        return Ok(true);
    }
    Ok(false)
}

/// Create a temporary symlink in the same directory as `final_path`.
///
/// The returned path is ready to be installed with [`install_prepared_path`].
///
/// # Errors
///
/// Returns an error if the temporary symlink cannot be created.
pub fn create_prepared_symlink(target: &Path, final_path: &Path) -> anyhow::Result<PathBuf> {
    use std::os::unix::fs::symlink;

    let prepared_path = unique_sibling_path(final_path, "symlink")?;
    if let Err(error) = symlink(target, &prepared_path) {
        let _ = remove_path_if_exists(&prepared_path);
        return Err(error.into());
    }
    Ok(prepared_path)
}

/// Ensure that no existing ancestor of `path` is a symlink.
///
/// # Errors
///
/// Returns an error if any existing ancestor is a symlink or metadata lookup fails.
pub fn ensure_no_symlink_ancestors(path: &Path) -> anyhow::Result<()> {
    let mut current = path.parent();
    while let Some(parent) = current {
        match std::fs::symlink_metadata(parent) {
            Ok(meta) => {
                anyhow::ensure!(
                    !meta.file_type().is_symlink(),
                    "destination path traverses through symlinked parent: {}",
                    parent.display()
                );
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        current = parent.parent();
    }
    Ok(())
}

/// Ensure that no existing ancestor below `root` is a symlink.
///
/// # Errors
///
/// Returns an error if `path` is not under `root`, any existing ancestor below
/// `root` is a symlink, or metadata lookup fails.
pub fn ensure_no_symlink_ancestors_under_root(root: &Path, path: &Path) -> anyhow::Result<()> {
    ensure_no_symlink_ancestors(root)?;
    match std::fs::symlink_metadata(root) {
        Ok(meta) => {
            anyhow::ensure!(
                !meta.file_type().is_symlink(),
                "destination root must not be a symlink: {}",
                root.display()
            );
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    let rel_path = path.strip_prefix(root).map_err(|_| {
        anyhow::anyhow!(
            "destination path {} is not under root {}",
            path.display(),
            root.display()
        )
    })?;

    let mut current = root.to_path_buf();
    let components = rel_path.components().collect::<Vec<_>>();
    for (index, component) in components.iter().enumerate() {
        current.push(component.as_os_str());
        let is_leaf = index + 1 == components.len();
        if is_leaf {
            break;
        }
        match std::fs::symlink_metadata(&current) {
            Ok(meta) => {
                anyhow::ensure!(
                    !meta.file_type().is_symlink(),
                    "destination path escapes root through symlinked parent: {}",
                    current.display()
                );
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => return Err(error.into()),
        }
    }

    Ok(())
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
pub fn install_prepared_path(prepared_path: &Path, final_path: &Path) -> anyhow::Result<()> {
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
pub fn ensure_directory_path(path: &Path) -> anyhow::Result<()> {
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

/// Compute destination block indices that differ from provided source hashes.
///
/// Uses memory-mapped I/O for performance when available, with automatic fallback
/// to buffered reads. The file should not be modified externally during this operation.
///
/// # Errors
///
/// Returns an error if file IO or worker coordination fails.
pub fn compute_requested_blocks(
    full_path: &Path,
    hashes: &[u64],
    block_size: u64,
) -> anyhow::Result<Vec<u32>> {
    if !full_path.exists() {
        return (0..hashes.len()).map(block_index).collect();
    }

    let file = std::fs::File::open(full_path)?;
    let file_len = file.metadata()?.len();
    let block_size_usize = usize::try_from(block_size).map_err(|e| anyhow::anyhow!(e))?;

    // Use copy-on-write mmap to avoid SIGBUS if file is modified during sync.
    // Falls back to pread() if mmap fails.
    let mmap = safe_mmap(&file);

    #[cfg(target_os = "linux")]
    if mmap.is_err() {
        // Hint sequential access for fallback path
        let _ = nix::fcntl::posix_fadvise(
            &file,
            0,
            0,
            nix::fcntl::PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL,
        );
    }

    let concurrency = clamped_parallelism();
    let num_blocks = hashes.len();
    let chunk_size = num_blocks.div_ceil(concurrency);

    let mut requested = std::thread::scope(|scope| -> anyhow::Result<Vec<u32>> {
        let mut workers = Vec::new();

        for (chunk_index, hash_chunk) in hashes.chunks(chunk_size).enumerate() {
            let start_idx = chunk_index * chunk_size;
            let mmap_ref = mmap.as_ref().ok();
            let file_ref = &file;

            workers.push(scope.spawn(move || -> anyhow::Result<Vec<u32>> {
                let mut local_requested = Vec::new();
                let mut buffer = vec![0u8; block_size_usize];

                for (relative_index, &expected_hash) in hash_chunk.iter().enumerate() {
                    let block_idx = start_idx + relative_index;
                    let offset = block_offset(block_idx, block_size)?;

                    if offset >= file_len {
                        local_requested.push(block_index(block_idx)?);
                        continue;
                    }

                    let matched = if let Some(mapped) = mmap_ref {
                        mmap_block_matches(mapped, offset, block_size_usize, expected_hash)?
                    } else {
                        read_block_matches(file_ref, &mut buffer, offset, expected_hash)?
                    };

                    if !matched {
                        local_requested.push(block_index(block_idx)?);
                    }
                }

                Ok(local_requested)
            }));
        }

        let mut requested = Vec::new();
        for worker in workers {
            let worker_result = worker
                .join()
                .map_err(|_| anyhow::anyhow!("hash worker panicked"))?;
            requested.extend(worker_result?);
        }
        Ok(requested)
    })?;
    requested.sort_unstable();
    Ok(requested)
}

/// Calculate block hashes for a file using parallel workers.
///
/// Uses memory-mapped I/O for performance when available, with automatic fallback
/// to buffered reads. The file should not be modified externally during this operation.
///
/// # Errors
///
/// Returns an error if file IO, task scheduling, or conversion fails.
pub(crate) fn calculate_file_hashes_for_open_file(
    file: &std::fs::File,
    block_size: u64,
    mmap: Option<&memmap2::Mmap>,
) -> anyhow::Result<Vec<u64>> {
    let len = file.metadata()?.len();
    let num_blocks = len.div_ceil(block_size);
    let num_blocks_usize = usize::try_from(num_blocks).map_err(|e| anyhow::anyhow!(e))?;
    if num_blocks_usize == 0 {
        return Ok(Vec::new());
    }

    let block_size_usize = usize::try_from(block_size).map_err(|e| anyhow::anyhow!(e))?;

    #[cfg(target_os = "linux")]
    if mmap.is_none() {
        // Hint sequential access for fallback path
        let _ = nix::fcntl::posix_fadvise(
            file,
            0,
            0,
            nix::fcntl::PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL,
        );
    }

    let concurrency = clamped_parallelism();
    let mut hashes = vec![0u64; num_blocks_usize];
    let chunk_size = num_blocks_usize.div_ceil(concurrency);

    std::thread::scope(|scope| -> anyhow::Result<()> {
        let mut workers = Vec::new();

        for start_block in (0..num_blocks_usize).step_by(chunk_size) {
            let end_block = std::cmp::min(start_block + chunk_size, num_blocks_usize);
            let mmap_ref = mmap;
            let file_ref = file;

            workers.push(scope.spawn(move || -> anyhow::Result<(usize, Vec<u64>)> {
                let mut local_hashes = Vec::with_capacity(end_block - start_block);
                let mut buffer = vec![0u8; block_size_usize];

                for block_idx in start_block..end_block {
                    let offset = block_offset(block_idx, block_size)?;
                    if let Some(mapped) = mmap_ref
                        && let Some(range) = mmap_range(offset, block_size_usize, mapped.len())?
                        && let Some(chunk) = mapped.get(range)
                    {
                        local_hashes.push(sync::fast_hash_block(chunk));
                        continue;
                    }

                    let bytes_read = file_ref.read_at(&mut buffer, offset)?;
                    let chunk = buffer
                        .get(..bytes_read)
                        .ok_or_else(|| anyhow::anyhow!("read chunk exceeds buffer"))?;
                    local_hashes.push(sync::fast_hash_block(chunk));
                }

                Ok((start_block, local_hashes))
            }));
        }

        for worker in workers {
            let worker_result = worker
                .join()
                .map_err(|_| anyhow::anyhow!("hash worker panicked"))?;
            let (start_block, local_hashes) = worker_result?;
            let end_block = start_block + local_hashes.len();
            let hash_slice = hashes
                .get_mut(start_block..end_block)
                .ok_or_else(|| anyhow::anyhow!("hash slice out of bounds"))?;
            hash_slice.copy_from_slice(&local_hashes);
        }

        Ok(())
    })?;

    Ok(hashes)
}

/// Calculate block hashes for a file using parallel workers.
///
/// Uses memory-mapped I/O for performance when available, with automatic fallback
/// to buffered reads. The file should not be modified externally during this operation.
///
/// # Errors
///
/// Returns an error if file IO, task scheduling, or conversion fails.
pub async fn calculate_file_hashes(path: &Path, block_size: u64) -> anyhow::Result<Vec<u64>> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&path)?;
        let mmap = safe_mmap(&file);
        calculate_file_hashes_for_open_file(&file, block_size, mmap.as_ref().ok())
    })
    .await?
}

/// Compute BLAKE3 hash of a file for end-to-end verification.
///
/// # Errors
///
/// Returns an error if file IO fails.
pub async fn blake3_file_hash(path: &Path) -> anyhow::Result<[u8; 32]> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut hasher = blake3::Hasher::new();
        hasher.update_mmap_rayon(&path)?;
        Ok(*hasher.finalize().as_bytes())
    })
    .await?
}

#[cfg(test)]
mod tests {
    use super::{
        REPLACEMENT_FAILURE_HOOK, ensure_directory_path, install_prepared_path,
        test_support::{DurabilityProbe, OptimizationProbe},
    };
    use crate::pxs::{
        net::{self, PxsCodec},
        sync::{self, SyncOptions},
    };
    use std::{
        path::PathBuf,
        sync::{Mutex as StdMutex, OnceLock},
        time::Duration,
    };
    use tempfile::tempdir;
    use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle};
    use tokio_util::codec::Framed;

    fn optimization_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn replacement_test_lock() -> &'static StdMutex<()> {
        static LOCK: OnceLock<StdMutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| StdMutex::new(()))
    }

    fn durability_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn arm_replacement_failure_hook() -> anyhow::Result<()> {
        let mut hook = REPLACEMENT_FAILURE_HOOK
            .lock()
            .map_err(|_| anyhow::anyhow!("replacement failure hook mutex poisoned"))?;
        *hook = true;
        Ok(())
    }

    async fn spawn_receiver(
        dst_root: PathBuf,
        fsync: bool,
    ) -> anyhow::Result<(std::net::SocketAddr, JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        let receiver_handle = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let dst_root_clone = dst_root.clone();
                tokio::spawn(async move {
                    let mut framed = Framed::new(stream, PxsCodec);
                    if let Err(error) =
                        net::handle_client(&mut framed, &dst_root_clone, false, fsync).await
                    {
                        eprintln!("Receiver error: {error}");
                    }
                });
            }
        });

        Ok((addr, receiver_handle))
    }

    async fn stop_receiver(receiver_handle: JoinHandle<()>) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        receiver_handle.abort();
    }

    #[tokio::test]
    async fn test_network_transfer_uses_mmap_source_reads() -> anyhow::Result<()> {
        let _guard = optimization_test_lock().lock().await;

        let dir = tempdir()?;
        let src_dir = dir.path().join("src");
        let dst_dir = dir.path().join("dst");
        std::fs::create_dir_all(&src_dir)?;
        std::fs::create_dir_all(&dst_dir)?;

        let file_path = src_dir.join("large.bin");
        let content = (0..(4 * 128 * 1024))
            .map(|index| {
                u8::try_from(index % 251).map_err(|e: std::num::TryFromIntError| anyhow::anyhow!(e))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        std::fs::write(&file_path, &content)?;

        let probe = OptimizationProbe::start()?;
        let (addr, receiver_handle) = spawn_receiver(dst_dir.clone(), false).await?;

        net::run_sender(&addr.to_string(), &src_dir, 0.5, false, false, &[]).await?;
        stop_receiver(receiver_handle).await;

        let snapshot = probe.snapshot();
        assert!(snapshot.safe_mmap_successes > 0);
        assert!(snapshot.sender_mmap_read_hits > 0);
        assert_eq!(std::fs::read(dst_dir.join("large.bin"))?, content);

        Ok(())
    }

    #[tokio::test]
    async fn test_network_delta_seeding_attempts_staged_clone() -> anyhow::Result<()> {
        let _guard = optimization_test_lock().lock().await;

        let dir = tempdir()?;
        let src_dir = dir.path().join("src");
        let dst_dir = dir.path().join("dst");
        std::fs::create_dir_all(&src_dir)?;
        std::fs::create_dir_all(&dst_dir)?;

        let src_file = src_dir.join("delta.bin");
        let dst_file = dst_dir.join("delta.bin");
        let src_content = b"abcdefghZZZZmnop".to_vec();
        let dst_content = b"abcdefghYYYYmnop".to_vec();
        std::fs::write(&src_file, &src_content)?;
        std::fs::write(&dst_file, &dst_content)?;

        filetime::set_file_times(
            &dst_file,
            filetime::FileTime::from_unix_time(1_000_000_000, 0),
            filetime::FileTime::from_unix_time(1_000_000_000, 0),
        )?;

        let probe = OptimizationProbe::start()?;
        let (addr, receiver_handle) = spawn_receiver(dst_dir.clone(), false).await?;

        net::run_sender(&addr.to_string(), &src_dir, 0.5, false, false, &[]).await?;
        stop_receiver(receiver_handle).await;

        let snapshot = probe.snapshot();
        assert!(snapshot.staged_seed_invocations > 0);
        #[cfg(target_os = "linux")]
        assert!(snapshot.staged_clone_attempts > 0);
        assert!(
            snapshot.staged_clone_successes > 0 || snapshot.staged_copy_fallbacks > 0,
            "expected clone success or copy fallback while seeding staged file"
        );
        assert_eq!(std::fs::read(&dst_file)?, src_content);

        Ok(())
    }

    #[tokio::test]
    async fn test_network_checksum_mode_skips_staging_on_match() -> anyhow::Result<()> {
        let _guard = optimization_test_lock().lock().await;

        let dir = tempdir()?;
        let src_dir = dir.path().join("src");
        let dst_dir = dir.path().join("dst");
        std::fs::create_dir_all(&src_dir)?;
        std::fs::create_dir_all(&dst_dir)?;

        let file_path = "match.bin";
        let content = b"perfect match".to_vec();
        std::fs::write(src_dir.join(file_path), &content)?;
        std::fs::write(dst_dir.join(file_path), &content)?;

        let probe = OptimizationProbe::start()?;
        let (addr, receiver_handle) = spawn_receiver(dst_dir.clone(), false).await?;

        // Run sync with --checksum
        net::run_sender(&addr.to_string(), &src_dir, 0.5, true, false, &[]).await?;
        stop_receiver(receiver_handle).await;

        let snapshot = probe.snapshot();
        // VERIFY: No staging file was prepared because the file already matched size/hash
        assert_eq!(
            snapshot.staged_seed_invocations, 0,
            "STAGING REGRESSION: Staging file was prepared even though contents matched!"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_local_directory_fsync_syncs_directories() -> anyhow::Result<()> {
        let _guard = durability_test_lock().lock().await;
        let dir = tempdir()?;
        let src_dir = dir.path().join("src");
        let dst_dir = dir.path().join("dst");
        std::fs::create_dir_all(src_dir.join("nested"))?;

        let probe = DurabilityProbe::start()?;
        let options = SyncOptions::new(0.5, false, false, false, Vec::new(), true, true);
        sync::sync_dir(&src_dir, &dst_dir, &options).await?;

        let snapshot = probe.snapshot()?;
        assert!(
            snapshot.synced_paths.contains(&dst_dir),
            "expected fsync on destination root directory"
        );
        assert!(
            snapshot.synced_paths.contains(&dst_dir.join("nested")),
            "expected fsync on nested destination directory"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_network_symlink_fsync_syncs_parent_directory() -> anyhow::Result<()> {
        let _guard = durability_test_lock().lock().await;
        let dir = tempdir()?;
        let src_dir = dir.path().join("src");
        let dst_dir = dir.path().join("dst");
        std::fs::create_dir_all(&src_dir)?;
        std::fs::create_dir_all(&dst_dir)?;
        std::os::unix::fs::symlink("target", src_dir.join("link"))?;

        let probe = DurabilityProbe::start()?;
        let (addr, receiver_handle) = spawn_receiver(dst_dir.clone(), true).await?;

        net::run_sender(&addr.to_string(), &src_dir, 0.5, false, false, &[]).await?;
        stop_receiver(receiver_handle).await;

        let snapshot = probe.snapshot()?;
        assert!(
            snapshot
                .synced_parent_targets
                .contains(&dst_dir.join("link")),
            "expected symlink install to sync its parent directory"
        );
        Ok(())
    }

    #[test]
    fn test_install_prepared_path_restores_original_directory_on_failure() -> anyhow::Result<()> {
        let _guard = replacement_test_lock()
            .lock()
            .map_err(|_| anyhow::anyhow!("replacement test lock poisoned"))?;
        let dir = tempdir()?;
        let final_path = dir.path().join("entry");
        let prepared_path = dir.path().join("entry.tmp");

        std::fs::create_dir_all(final_path.join("nested"))?;
        std::fs::write(final_path.join("nested/file.txt"), "original")?;
        std::fs::write(&prepared_path, "replacement")?;
        arm_replacement_failure_hook()?;

        let error = match install_prepared_path(&prepared_path, &final_path) {
            Ok(()) => anyhow::bail!("replacement should fail after backup move"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("injected replacement failure"));
        assert!(final_path.is_dir());
        assert_eq!(
            std::fs::read_to_string(final_path.join("nested/file.txt"))?,
            "original"
        );
        assert!(!prepared_path.exists());
        Ok(())
    }

    #[test]
    fn test_ensure_directory_path_restores_conflicting_file_on_failure() -> anyhow::Result<()> {
        let _guard = replacement_test_lock()
            .lock()
            .map_err(|_| anyhow::anyhow!("replacement test lock poisoned"))?;
        let dir = tempdir()?;
        let path = dir.path().join("entry");

        std::fs::write(&path, "original file")?;
        arm_replacement_failure_hook()?;

        let error = match ensure_directory_path(&path) {
            Ok(()) => anyhow::bail!("directory replacement should be rolled back"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("injected replacement failure"));
        assert!(path.is_file());
        assert_eq!(std::fs::read_to_string(&path)?, "original file");
        Ok(())
    }
}
