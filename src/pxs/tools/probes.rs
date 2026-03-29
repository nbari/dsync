#[cfg(test)]
use anyhow::Result;
use std::path::Path;

#[cfg(test)]
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

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
    fn snapshot(&self) -> Result<test_support::DurabilityProbeSnapshot> {
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
pub(crate) fn record_safe_mmap_success() {
    with_optimization_probe(|probe| {
        probe.safe_mmap_successes.fetch_add(1, Ordering::Relaxed);
    });
}

#[cfg(not(test))]
pub(crate) fn record_safe_mmap_success() {}

#[cfg(test)]
pub(crate) fn record_sender_mmap_read_hit() {
    with_optimization_probe(|probe| {
        probe.sender_mmap_read_hits.fetch_add(1, Ordering::Relaxed);
    });
}

#[cfg(not(test))]
pub(crate) fn record_sender_mmap_read_hit() {}

#[cfg(test)]
pub(crate) fn record_staged_seed_invocation() {
    with_optimization_probe(|probe| {
        probe
            .staged_seed_invocations
            .fetch_add(1, Ordering::Relaxed);
    });
}

#[cfg(not(test))]
pub(crate) fn record_staged_seed_invocation() {}

#[cfg(test)]
pub(crate) fn record_staged_clone_attempt() {
    with_optimization_probe(|probe| {
        probe.staged_clone_attempts.fetch_add(1, Ordering::Relaxed);
    });
}

#[cfg(all(not(test), target_os = "linux"))]
pub(crate) fn record_staged_clone_attempt() {}

#[cfg(test)]
pub(crate) fn record_staged_clone_success() {
    with_optimization_probe(|probe| {
        probe.staged_clone_successes.fetch_add(1, Ordering::Relaxed);
    });
}

#[cfg(all(not(test), target_os = "linux"))]
pub(crate) fn record_staged_clone_success() {}

#[cfg(test)]
pub(crate) fn record_staged_copy_fallback() {
    with_optimization_probe(|probe| {
        probe.staged_copy_fallbacks.fetch_add(1, Ordering::Relaxed);
    });
}

#[cfg(not(test))]
pub(crate) fn record_staged_copy_fallback() {}

#[cfg(test)]
pub(crate) fn record_synced_path(path: &Path) {
    let path = path.to_path_buf();
    with_durability_probe(|probe| {
        if let Ok(mut paths) = probe.synced_paths.lock() {
            paths.push(path);
        }
    });
}

#[cfg(not(test))]
pub(crate) fn record_synced_path(_path: &Path) {}

#[cfg(test)]
pub(crate) fn record_synced_parent_target(path: &Path) {
    let path = path.to_path_buf();
    with_durability_probe(|probe| {
        if let Ok(mut paths) = probe.synced_parent_targets.lock() {
            paths.push(path);
        }
    });
}

#[cfg(not(test))]
pub(crate) fn record_synced_parent_target(_path: &Path) {}

/// Hidden testing hooks for observing optimization paths without changing runtime behavior.
#[cfg(test)]
#[doc(hidden)]
pub mod test_support {
    use super::{
        ACTIVE_DURABILITY_PROBE, ACTIVE_OPTIMIZATION_PROBE, Arc, DurabilityProbeState,
        OptimizationProbeState, PathBuf,
    };
    use anyhow::Result;

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
        /// Returns an error if another optimization probe is already active in the current process.
        pub fn start() -> Result<Self> {
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
        /// Returns an error if another durability probe is already active in the current process.
        pub fn start() -> Result<Self> {
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
        pub fn snapshot(&self) -> Result<DurabilityProbeSnapshot> {
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
