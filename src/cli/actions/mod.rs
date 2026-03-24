pub mod run;

use std::path::PathBuf;

/// Remote endpoint selected by a public push or pull command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteEndpoint {
    /// Raw TCP endpoint such as `host:port`.
    Tcp(String),
    /// SSH endpoint such as `user@host:/path`.
    Ssh { host: String, path: String },
    /// Standard input/output transport for advanced piping.
    Stdio,
}

/// Parsed high-level action selected by the CLI.
#[derive(Debug)]
pub enum Action {
    /// Synchronize a local source path into a local destination path.
    Sync {
        src: PathBuf,
        dst: PathBuf,
        threshold: f32,
        checksum: bool,
        dry_run: bool,
        delete: bool,
        fsync: bool,
        ignores: Vec<String>,
        quiet: bool,
    },
    /// Push a local source path to a remote endpoint.
    Push {
        endpoint: RemoteEndpoint,
        src: PathBuf,
        threshold: f32,
        checksum: bool,
        delete: bool,
        fsync: bool,
        ignores: Vec<String>,
        quiet: bool,
    },
    /// Pull from a remote endpoint into a local destination path.
    Pull {
        endpoint: RemoteEndpoint,
        dst: PathBuf,
        threshold: f32,
        checksum: bool,
        delete: bool,
        fsync: bool,
        ignores: Vec<String>,
        quiet: bool,
    },
    /// Listen for incoming push operations and write them into `dst`.
    Listen {
        addr: String,
        dst: PathBuf,
        fsync: bool,
        quiet: bool,
    },
    /// Serve `src` to remote pull clients.
    Serve {
        addr: String,
        src: PathBuf,
        threshold: f32,
        checksum: bool,
        ignores: Vec<String>,
        quiet: bool,
    },
    /// Internal stdio receiver used by SSH tunneling.
    InternalStdioReceive {
        dst: PathBuf,
        fsync: bool,
        ignores: Vec<String>,
        quiet: bool,
    },
    /// Internal stdio sender used by SSH tunneling.
    InternalStdioSend {
        src: PathBuf,
        threshold: f32,
        checksum: bool,
        delete: bool,
        ignores: Vec<String>,
        quiet: bool,
    },
}
