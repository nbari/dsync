//! Shared helpers used across local sync and network synchronization.

mod compare;
mod fs;
mod hash;
mod probes;
mod progress;
mod runtime;
mod safety;
mod staging;

#[cfg(test)]
mod tests;

pub use compare::{
    DEFAULT_THRESHOLD, get_file_size, is_below_threshold, should_skip_file, should_use_full_copy,
    should_use_full_copy_meta,
};
pub use fs::{preallocate, sync_directory_and_parent, sync_parent_directory, sync_path};
pub use progress::{CombinedProgressBar, ProgressBarLike, create_progress_bar};
pub use runtime::{
    MAX_PARALLELISM, clamped_parallelism, default_large_file_parallel_workers,
    default_network_file_concurrency,
};
pub use safety::{ensure_no_symlink_ancestors, ensure_no_symlink_ancestors_under_root};
pub use staging::{
    StagedFile, create_prepared_symlink, ensure_directory_path, install_prepared_path,
};

pub use hash::{blake3_file_hash, calculate_file_hashes, compute_requested_blocks};
pub(crate) use hash::{
    calculate_file_hashes_for_open_file, record_sender_mmap_read_hit, safe_mmap,
};

#[cfg(test)]
#[doc(hidden)]
pub use probes::test_support;
