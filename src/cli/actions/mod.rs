pub mod run;

use std::path::PathBuf;

#[derive(Debug)]
pub enum Action {
    Run {
        src: PathBuf,
        dst: PathBuf,
        threshold: f32,
        checksum: bool,
        dry_run: bool,
    },
    Listen {
        addr: String,
        dst: PathBuf,
    },
    Connect {
        addr: String,
        src: PathBuf,
        checksum: bool,
    },
}
