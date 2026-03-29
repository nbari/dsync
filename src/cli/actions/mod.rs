pub mod run;

use std::path::PathBuf;

/// Source or destination operand selected by the unified public sync command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncOperand {
    /// Local filesystem path.
    Local(PathBuf),
    /// Remote endpoint.
    Remote(RemoteEndpoint),
}

/// Remote endpoint selected by the unified public sync CLI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteEndpoint {
    /// Raw TCP endpoint such as `host:port` or `host:port/path`.
    Tcp { addr: String, path: Option<String> },
    /// SSH endpoint such as `user@host:/path`.
    Ssh { host: String, path: String },
    /// Standard input/output transport for advanced piping.
    Stdio,
}

/// Parsed high-level action selected by the CLI.
#[derive(Debug)]
pub enum Action {
    /// Synchronize between local and remote endpoints using transport-aware dispatch.
    Sync {
        src: SyncOperand,
        dst: SyncOperand,
        threshold: f32,
        checksum: bool,
        dry_run: bool,
        delete: bool,
        fsync: bool,
        large_file_parallel_threshold: u64,
        large_file_parallel_workers: usize,
        network_file_concurrency: usize,
        ignores: Vec<String>,
        quiet: bool,
    },
    /// Listen for incoming sync operations and write them into `dst`.
    Listen {
        addr: String,
        dst: PathBuf,
        fsync: bool,
        quiet: bool,
    },
    /// Serve `src` to remote sync clients.
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
    /// Internal stdio worker used by SSH large-file chunk transfer.
    InternalChunkWrite {
        dst: PathBuf,
        transfer_id: String,
        quiet: bool,
    },
}
