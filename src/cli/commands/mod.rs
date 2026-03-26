use crate::built_info;
use clap::{
    Arg, ArgAction, ColorChoice, Command,
    builder::{
        ValueParser,
        styling::{AnsiColor, Effects, Styles},
    },
};
use std::path::PathBuf;

const LONG_ABOUT: &str = "pxs is a file synchronization tool for the same broad \
    job as rsync: moving data trees efficiently and refreshing existing copies \
    with as little work as possible. It is not a drop-in replacement for rsync. \
    pxs focuses on modern large-data sync workloads and uses Rust performance, \
    parallelism, concurrency, fixed-block chunking, and high-throughput \
    transport to speed up repeated synchronization when those choices help.\n\n\
    EXAMPLES:\n\n\
    1. Local Sync (File or Directory):\n\
       pxs sync /data/old /data/new\n\n\
    2. Network Sync - Receiver (Server 2):\n\
       pxs listen 0.0.0.0:8080 /new/pgdata\n\n\
    3. Network Sync - Sender (Server 1):\n\
       pxs push /old/pgdata 192.168.1.10:8080\n\n\
    4. Pull Mode (SSH):\n\
       pxs pull user@server:/data ./local\n\n\
    5. Force Content Verification (Checksum):\n\
       pxs sync file.bin copy.bin --checksum\n\n\
    SUPPORTED PLATFORMS:\n\
       Linux, macOS, and BSD.\n\
       Windows is not supported.";
const ABOUT: &str =
    "pxs (Parallel X-Sync) - Rust file synchronization for modern large-data workloads.";
const THRESHOLD_LONG_HELP: &str = "Value between 0.1 and 1.0. If the destination file size is less than this percentage of the source, a full rewrite is performed.";
const CHECKSUM_LONG_HELP: &str = "By default, pxs skips files if size and modification time match. Use this to force a block-by-block hash comparison. In network mode, pxs also performs end-to-end BLAKE3 verification after the transfer completes.";
const FSYNC_LONG_HELP: &str =
    "Ensures that file data and metadata are flushed to disk before finishing. Slower but safer.";
const LARGE_FILE_PARALLEL_THRESHOLD_LONG_HELP: &str = "Enable SSH push chunk-parallel transfer for files at or above this size. Accepts raw bytes or binary suffixes such as KiB, MiB, GiB, and TiB. Use 0 to disable.";
const LARGE_FILE_PARALLEL_WORKERS_LONG_HELP: &str = "Number of parallel SSH worker sessions for eligible large files. If omitted, pxs chooses a conservative default from available CPU cores.";

/// Create a path validator that requires the path to exist.
pub fn validator_path_exists() -> ValueParser {
    ValueParser::from(move |s: &str| -> std::result::Result<PathBuf, String> {
        let path = PathBuf::from(s);
        if path.exists() {
            return Ok(path);
        }

        Err(format!("Path does not exist: '{s}'"))
    })
}

/// Create a path validator that requires the destination or its parent to exist.
pub fn validator_parent_exist() -> ValueParser {
    ValueParser::from(move |s: &str| -> std::result::Result<PathBuf, String> {
        let path = PathBuf::from(s);
        if path.exists() && path.is_dir() {
            return Ok(path);
        }

        let parent = if let Some(p) = path.parent() {
            if p.as_os_str().is_empty() {
                PathBuf::from(".")
            } else {
                p.to_path_buf()
            }
        } else {
            PathBuf::from(".")
        };

        if parent.exists() && parent.is_dir() {
            return Ok(path);
        }

        Err(format!(
            "Invalid destination path or parent directory does not exist: '{s}'"
        ))
    })
}

fn cli_styles() -> Styles {
    Styles::styled()
        .header(AnsiColor::Yellow.on_default() | Effects::BOLD)
        .usage(AnsiColor::Green.on_default() | Effects::BOLD)
        .literal(AnsiColor::Blue.on_default() | Effects::BOLD)
        .placeholder(AnsiColor::Green.on_default())
}

fn threshold_parser() -> ValueParser {
    ValueParser::from(|s: &str| {
        let val: f32 = s.parse().map_err(|_| String::from("Invalid float"))?;
        if (0.1..=1.0).contains(&val) {
            Ok(val)
        } else {
            Err(String::from("Threshold must be between 0.1 and 1.0"))
        }
    })
}

fn parse_size_bytes(value: &str) -> std::result::Result<u64, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(String::from("Size must not be empty"));
    }

    let split_index = trimmed
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (digits, suffix) = trimmed.split_at(split_index);
    let base = digits
        .parse::<u64>()
        .map_err(|_| format!("Invalid size value: '{value}'"))?;
    let multiplier = match suffix.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1_u64,
        "k" | "kb" | "kib" => 1024_u64,
        "m" | "mb" | "mib" => 1024_u64.pow(2),
        "g" | "gb" | "gib" => 1024_u64.pow(3),
        "t" | "tb" | "tib" => 1024_u64.pow(4),
        _ => return Err(format!("Unsupported size suffix in '{value}'")),
    };
    base.checked_mul(multiplier)
        .ok_or_else(|| format!("Size is too large: '{value}'"))
}

fn size_bytes_parser() -> ValueParser {
    ValueParser::from(|s: &str| parse_size_bytes(s))
}

fn positive_usize_parser() -> ValueParser {
    ValueParser::from(|s: &str| {
        let value = s
            .parse::<usize>()
            .map_err(|_| String::from("Invalid positive integer"))?;
        if value == 0 {
            Err(String::from("Value must be greater than 0"))
        } else {
            Ok(value)
        }
    })
}

fn long_version() -> &'static str {
    if let Some(git_hash) = built_info::GIT_COMMIT_HASH {
        Box::leak(format!("{} - {}", env!("CARGO_PKG_VERSION"), git_hash).into_boxed_str())
    } else {
        env!("CARGO_PKG_VERSION")
    }
}

fn verbose_arg() -> Arg {
    Arg::new("verbose")
        .short('v')
        .long("verbose")
        .help("Increase verbosity, -vv for debug")
        .global(true)
        .action(ArgAction::Count)
}

fn quiet_arg() -> Arg {
    Arg::new("quiet")
        .short('q')
        .long("quiet")
        .help("Do not show progress bar")
        .global(true)
        .action(ArgAction::SetTrue)
}

fn threshold_arg(hidden: bool) -> Arg {
    Arg::new("threshold")
        .short('t')
        .long("threshold")
        .help("Threshold to determine if a file should be copied")
        .long_help(THRESHOLD_LONG_HELP)
        .value_name("THRESHOLD")
        .value_parser(threshold_parser())
        .default_value("0.5")
        .hide(hidden)
}

fn checksum_arg(hidden: bool) -> Arg {
    Arg::new("checksum")
        .short('c')
        .long("checksum")
        .help("Skip based on checksum, not mod-time & size")
        .long_help(CHECKSUM_LONG_HELP)
        .action(ArgAction::SetTrue)
        .hide(hidden)
}

fn fsync_arg(hidden: bool) -> Arg {
    Arg::new("fsync")
        .short('f')
        .long("fsync")
        .help("Force durable sync of committed files, directories, and symlinks")
        .long_help(FSYNC_LONG_HELP)
        .action(ArgAction::SetTrue)
        .hide(hidden)
}

fn large_file_parallel_threshold_arg() -> Arg {
    Arg::new("large_file_parallel_threshold")
        .long("large-file-parallel-threshold")
        .help("Enable SSH push chunk-parallel transfer at or above SIZE")
        .long_help(LARGE_FILE_PARALLEL_THRESHOLD_LONG_HELP)
        .value_name("SIZE")
        .value_parser(size_bytes_parser())
        .default_value("1GiB")
}

fn large_file_parallel_workers_arg() -> Arg {
    Arg::new("large_file_parallel_workers")
        .long("large-file-parallel-workers")
        .help("Set the number of SSH worker sessions for large-file push")
        .long_help(LARGE_FILE_PARALLEL_WORKERS_LONG_HELP)
        .value_name("N")
        .value_parser(positive_usize_parser())
}

fn dry_run_arg() -> Arg {
    Arg::new("dry_run")
        .short('n')
        .long("dry-run")
        .help("Show what would have been transferred")
        .action(ArgAction::SetTrue)
}

fn delete_arg() -> Arg {
    Arg::new("delete")
        .long("delete")
        .help("Delete extraneous files from destination directories")
        .action(ArgAction::SetTrue)
}

fn ignore_arg(hidden: bool) -> Arg {
    Arg::new("ignore")
        .short('i')
        .long("ignore")
        .help("Ignore files/directories matching this pattern (glob)")
        .action(ArgAction::Append)
        .value_name("PATTERN")
        .hide(hidden)
}

fn exclude_from_arg(hidden: bool) -> Arg {
    Arg::new("exclude_from")
        .short('E')
        .long("exclude-from")
        .help("Read exclude patterns from FILE")
        .value_name("FILE")
        .value_parser(validator_path_exists())
        .hide(hidden)
}

fn src_arg() -> Arg {
    Arg::new("src")
        .help("Path to the source file or directory")
        .value_parser(validator_path_exists())
        .value_name("SRC")
        .required(true)
}

fn dst_arg() -> Arg {
    Arg::new("dst")
        .help("Path to the destination file or directory")
        .value_parser(validator_parent_exist())
        .value_name("DST")
        .required(true)
}

fn endpoint_arg() -> Arg {
    Arg::new("endpoint")
        .help("Remote endpoint as host:port, user@host:/path, or - for stdio")
        .value_name("ENDPOINT")
        .required(true)
}

fn addr_arg() -> Arg {
    Arg::new("addr")
        .help("Listen address such as 0.0.0.0:8080")
        .value_name("ADDR")
        .required(true)
}

fn internal_stdio_args() -> [Arg; 13] {
    [
        Arg::new("stdio")
            .long("stdio")
            .help("Use stdin/stdout for communication (internal use for SSH)")
            .hide(true)
            .action(ArgAction::SetTrue),
        Arg::new("sender")
            .long("sender")
            .help("Run in sender mode (internal use for SSH)")
            .hide(true)
            .action(ArgAction::SetTrue),
        Arg::new("source")
            .long("source")
            .help("Path to the source file or directory")
            .hide(true)
            .value_parser(validator_path_exists())
            .value_name("SRC"),
        Arg::new("destination")
            .long("destination")
            .help("Path to the destination file or directory")
            .hide(true)
            .value_parser(validator_parent_exist())
            .value_name("DST"),
        Arg::new("chunk_writer")
            .long("chunk-writer")
            .help("Run in chunk-writer mode (internal use for SSH large files)")
            .hide(true)
            .action(ArgAction::SetTrue),
        Arg::new("transfer_id")
            .long("transfer-id")
            .help("Receiver-issued transfer id for chunk-writer mode")
            .hide(true)
            .value_name("ID"),
        Arg::new("chunk_path")
            .long("chunk-path")
            .help("Protocol path for the chunk-writer file")
            .hide(true)
            .value_name("PATH"),
        threshold_arg(true),
        checksum_arg(true),
        delete_arg(),
        fsync_arg(true),
        ignore_arg(true),
        exclude_from_arg(true),
    ]
}

fn sync_command() -> Command {
    Command::new("sync")
        .about("Synchronize a local source into a local destination")
        .args([
            src_arg(),
            dst_arg(),
            threshold_arg(false),
            checksum_arg(false),
            fsync_arg(false),
            dry_run_arg(),
            delete_arg(),
            ignore_arg(false),
            exclude_from_arg(false),
        ])
}

fn push_command() -> Command {
    Command::new("push")
        .about("Push a local source to a remote receiver or SSH destination")
        .args([
            src_arg(),
            endpoint_arg(),
            threshold_arg(false),
            checksum_arg(false),
            delete_arg(),
            fsync_arg(false),
            large_file_parallel_threshold_arg(),
            large_file_parallel_workers_arg(),
            ignore_arg(false),
            exclude_from_arg(false),
        ])
}

fn pull_command() -> Command {
    Command::new("pull")
        .about("Pull from a remote serve endpoint or SSH source into a local destination")
        .args([
            endpoint_arg(),
            dst_arg(),
            threshold_arg(false),
            checksum_arg(false),
            delete_arg(),
            fsync_arg(false),
            ignore_arg(false),
            exclude_from_arg(false),
        ])
}

fn listen_command() -> Command {
    Command::new("listen")
        .about("Listen for incoming push operations and write them to a destination")
        .args([addr_arg(), dst_arg(), fsync_arg(false)])
}

fn serve_command() -> Command {
    Command::new("serve")
        .about("Serve a local source for remote pull clients")
        .args([
            addr_arg(),
            src_arg(),
            threshold_arg(false),
            checksum_arg(false),
            ignore_arg(false),
            exclude_from_arg(false),
        ])
}

fn base_command() -> Command {
    Command::new("pxs")
        .about(ABOUT)
        .long_about(LONG_ABOUT)
        .version(env!("CARGO_PKG_VERSION"))
        .long_version(long_version())
        .color(ColorChoice::Auto)
        .styles(cli_styles())
        .arg_required_else_help(true)
        .disable_help_subcommand(true)
        .arg(verbose_arg())
        .arg(quiet_arg())
        .args(internal_stdio_args())
        .subcommands([
            sync_command(),
            push_command(),
            pull_command(),
            listen_command(),
            serve_command(),
        ])
}

/// Build the CLI command definition.
#[must_use]
pub fn new() -> Command {
    base_command()
}

#[cfg(test)]
mod tests {
    use super::new;
    use tempfile::tempdir;

    #[test]
    fn test_verbose_flag_counts_occurrences() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src.txt");
        let dst = dir.path().join("dst.txt");
        std::fs::write(&src, "content")?;
        let src_arg = src.to_string_lossy().to_string();
        let dst_arg = dst.to_string_lossy().to_string();
        let matches = new().try_get_matches_from(["pxs", "sync", &src_arg, &dst_arg, "-vvv"])?;
        assert_eq!(matches.get_count("verbose"), 3);
        Ok(())
    }

    #[test]
    fn test_threshold_rejects_out_of_range_values() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src.txt");
        let dst = dir.path().join("dst.txt");
        std::fs::write(&src, "content")?;
        let src_arg = src.to_string_lossy().to_string();
        let dst_arg = dst.to_string_lossy().to_string();

        let too_small =
            new().try_get_matches_from(["pxs", "sync", &src_arg, &dst_arg, "--threshold", "0.01"]);
        assert!(too_small.is_err());

        let too_large =
            new().try_get_matches_from(["pxs", "sync", &src_arg, &dst_arg, "--threshold", "1.5"]);
        assert!(too_large.is_err());
        Ok(())
    }

    #[test]
    fn test_help_hides_internal_stdio_flags() -> anyhow::Result<()> {
        let mut help = Vec::new();
        new().write_long_help(&mut help)?;
        let help = String::from_utf8(help)?;
        assert!(!help.contains("--stdio"));
        assert!(!help.contains("--source"));
        assert!(help.contains("sync"));
        assert!(help.contains("push"));
        Ok(())
    }

    #[test]
    fn test_internal_stdio_mode_still_parses() -> anyhow::Result<()> {
        let matches =
            new().try_get_matches_from(["pxs", "--stdio", "--destination", ".", "--fsync"])?;
        assert!(matches.get_flag("stdio"));
        assert!(matches.get_flag("fsync"));
        Ok(())
    }
}
