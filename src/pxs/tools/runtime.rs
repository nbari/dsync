/// The maximum number of concurrent workers allowed across the system.
pub const MAX_PARALLELISM: usize = 64;

/// Return the number of logical CPU cores, clamped to a safe maximum.
///
/// If the OS fails to report the number of cores, it defaults to 1
/// to ensure safety on restricted or legacy systems.
#[must_use]
pub fn clamped_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1)
        .min(MAX_PARALLELISM)
}

/// Return a conservative default worker count for parallel remote large-file transfer.
///
/// SSH and raw TCP worker attachments add extra per-worker overhead, so the
/// default is intentionally bucketed rather than matching CPU count directly.
#[must_use]
pub fn default_large_file_parallel_workers() -> usize {
    match clamped_parallelism() {
        0 | 1 => 1,
        2 | 3 => 2,
        4..=7 => 4,
        _ => 8,
    }
}

/// Return the default number of small files to keep in flight on outbound network sync.
#[must_use]
pub const fn default_network_file_concurrency() -> usize {
    4
}
