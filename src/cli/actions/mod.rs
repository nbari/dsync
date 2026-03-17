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
        fsync: bool,
        ignores: Vec<String>,
    },
    Listen {
        addr: String,
        dst: PathBuf,
        fsync: bool,
    },
    ListenSender {
        addr: String,
        src: PathBuf,
        threshold: f32,
        checksum: bool,
        ignores: Vec<String>,
    },
    Connect {
        addr: String,
        src: PathBuf,
        threshold: f32,
        checksum: bool,
        remote_path: Option<String>,
        ignores: Vec<String>,
    },
    Pull {
        addr: String,
        dst: PathBuf,
        threshold: f32,
        checksum: bool,
        fsync: bool,
        remote_path: Option<String>,
        ignores: Vec<String>,
    },
    Stdio {
        dst: PathBuf,
        fsync: bool,
    },
    StdioSender {
        src: PathBuf,
        threshold: f32,
        checksum: bool,
        ignores: Vec<String>,
    },
}
