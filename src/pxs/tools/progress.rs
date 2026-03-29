use indicatif::{ProgressBar, ProgressStyle};
use std::sync::Arc;

const PROGRESS_CHARS: &str = "█▉▊▋▌▍▎▏  ·";
const PROGRESS_TICK_STRINGS: &[&str] = &["◜", "◠", "◝", "◞", "◡", "◟", "·"];

/// A trait to abstract over different progress bar implementations.
pub trait ProgressBarLike: Send + Sync {
    fn inc(&self, delta: u64);
}

impl ProgressBarLike for ProgressBar {
    fn inc(&self, delta: u64) {
        self.inc(delta);
    }
}

/// A progress helper that increments two progress bars simultaneously.
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

/// Create the standard pxs progress bar.
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
