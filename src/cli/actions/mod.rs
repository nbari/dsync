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
        ignores: Vec<String>,
    },
    Listen {
        addr: String,
        dst: PathBuf,
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
        remote_path: Option<String>, // Option because TCP pull might not need it
        ignores: Vec<String>,
    },
    Stdio {
        dst: PathBuf,
    },
    StdioSender {
        src: PathBuf,
        threshold: f32,
        checksum: bool,
        ignores: Vec<String>,
    },
}
