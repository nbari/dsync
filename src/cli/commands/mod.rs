use crate::built_info;
use clap::{
    Arg, ArgAction, ArgGroup, ColorChoice, Command,
    builder::{
        ValueParser,
        styling::{AnsiColor, Effects, Styles},
    },
};
use std::path::PathBuf;

const LONG_ABOUT: &str = "pxs is a multi-threaded file synchronization tool designed to saturate \
    high-speed networks (10GbE+) and utilize multi-core CPUs for parallel hashing. \
    It uses 128KB fixed-block chunking and XxHash64 for extremely fast comparison, \
    making it ideal for large database files like Postgres PG_DATA.\n\n\
    EXAMPLES:\n\n\
    1. Local Sync (File or Directory):\n\
       pxs --source /data/old --destination /data/new\n\n\
    2. Network Sync - Receiver (Server 2):\n\
       pxs --listen 0.0.0.0:8080 --destination /new/pgdata\n\n\
    3. Network Sync - Sender (Server 1):\n\
       pxs --remote 192.168.1.10:8080 --source /old/pgdata\n\n\
    4. Pull Mode (SSH):\n\
       pxs --remote user@server:/data --destination ./local --pull\n\n\
    5. Force Content Verification (Checksum):\n\
       pxs --source file.bin --destination copy.bin --checksum\n\n\
    SUPPORTED PLATFORMS:\n\
       Linux, macOS, and BSD.\n\
       Windows is not supported.";
const ABOUT: &str =
    "pxs (Parallel X-Sync) - High-performance, concurrent file synchronization tool.";
const SOURCE_LONG_HELP: &str =
    "The local path to read data from. When using --remote, this is the data sent to the listener.";
const DESTINATION_LONG_HELP: &str =
    "The local path to write data to. When using --listen, incoming data is stored here.";
const THRESHOLD_LONG_HELP: &str = "Value between 0.1 and 1.0. If the destination file size is less than this percentage of the source, a full rewrite is performed.";
const CHECKSUM_LONG_HELP: &str = "By default, pxs skips files if size and modification time match. Use this to force a block-by-block hash comparison. In network mode, pxs also performs end-to-end BLAKE3 verification after the transfer completes.";
const FSYNC_LONG_HELP: &str =
    "Ensures that file data and metadata are flushed to disk before finishing. Slower but safer.";
const LISTEN_LONG_HELP: &str = "Starts pxs in server mode. If --destination is used, it receives files. If --source and --sender are used, it serves files to clients.";
const REMOTE_LONG_HELP: &str = "Connects to a remote pxs instance. By default, it pushes --source. If --pull is used, it fetches data to --destination.";
const PULL_LONG_HELP: &str = "When used with --remote, it initiates a connection to fetch data from the remote source to the local --destination.";
const MODE_ARGS: [&str; 4] = ["listen", "remote", "source", "destination"];

pub fn validator_path_exists() -> ValueParser {
    ValueParser::from(move |s: &str| -> std::result::Result<PathBuf, String> {
        let path = PathBuf::from(s);
        if path.exists() {
            return Ok(path);
        }

        Err(format!("Path does not exist: '{s}'"))
    })
}

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

fn mode_group() -> ArgGroup {
    ArgGroup::new("mode")
        .args(MODE_ARGS)
        .required(true)
        .multiple(true)
}

fn threshold_parser() -> ValueParser {
    clap::builder::ValueParser::from(|s: &str| {
        let val: f32 = s.parse().map_err(|_| String::from("Invalid float"))?;
        if (0.1..=1.0).contains(&val) {
            Ok(val)
        } else {
            Err(String::from("Threshold must be between 0.1 and 1.0"))
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

fn base_command() -> Command {
    Command::new("pxs")
        .about(ABOUT)
        .long_about(LONG_ABOUT)
        .version(env!("CARGO_PKG_VERSION"))
        .long_version(long_version())
        .color(ColorChoice::Auto)
        .styles(cli_styles())
        .arg_required_else_help(true)
        .group(mode_group())
}

/// Build the CLI command definition.
#[must_use]
pub fn new() -> Command {
    let command = base_command();
    command.args([
        Arg::new("source")
            .short('s')
            .long("source")
            .help("Path to the source file or directory")
            .long_help(SOURCE_LONG_HELP)
            .value_parser(validator_path_exists())
            .value_name("SRC"),
        Arg::new("destination")
            .short('d')
            .long("destination")
            .help("Path to the destination file or directory")
            .long_help(DESTINATION_LONG_HELP)
            .value_parser(validator_parent_exist())
            .value_name("DST"),
        Arg::new("threshold")
            .short('t')
            .long("threshold")
            .help("Threshold to determine if a file should be copied")
            .long_help(THRESHOLD_LONG_HELP)
            .value_name("THRESHOLD")
            .value_parser(threshold_parser())
            .default_value("0.5"),
        Arg::new("verbose")
            .short('v')
            .long("verbose")
            .help("Increase verbosity, -vv for debug")
            .action(ArgAction::Count),
        Arg::new("checksum")
            .short('c')
            .long("checksum")
            .help("Skip based on checksum, not mod-time & size")
            .long_help(CHECKSUM_LONG_HELP)
            .action(ArgAction::SetTrue),
        Arg::new("fsync")
            .short('f')
            .long("fsync")
            .help("Force fsync after writing files")
            .long_help(FSYNC_LONG_HELP)
            .action(ArgAction::SetTrue),
        Arg::new("dry_run")
            .short('n')
            .long("dry-run")
            .help("Show what would have been transferred")
            .action(ArgAction::SetTrue),
        Arg::new("listen")
            .short('l')
            .long("listen")
            .help("Listen on ADDRESS:PORT for incoming or outgoing sync")
            .long_help(LISTEN_LONG_HELP)
            .value_name("ADDR"),
        Arg::new("remote")
            .short('r')
            .long("remote")
            .help("Sync with remote ADDRESS:PORT or SSH path")
            .long_help(REMOTE_LONG_HELP)
            .value_name("ADDR"),
        Arg::new("stdio")
            .long("stdio")
            .help("Use stdin/stdout for communication (internal use for SSH)")
            .action(ArgAction::SetTrue)
            .conflicts_with_all(["listen", "remote"]),
        Arg::new("pull")
            .long("pull")
            .short('P')
            .help("Pull files from a remote source")
            .long_help(PULL_LONG_HELP)
            .action(ArgAction::SetTrue)
            .requires("destination")
            .conflicts_with("source"),
        Arg::new("sender")
            .long("sender")
            .help("Run in sender mode (serves files to clients)")
            .action(ArgAction::SetTrue),
        Arg::new("ignore")
            .short('i')
            .long("ignore")
            .help("Ignore files/directories matching this pattern (glob)")
            .action(ArgAction::Append)
            .value_name("PATTERN"),
        Arg::new("exclude_from")
            .short('E')
            .long("exclude-from")
            .help("Read exclude patterns from FILE")
            .value_name("FILE")
            .value_parser(validator_path_exists()),
    ])
}

#[cfg(test)]
mod tests {
    use super::new;

    #[test]
    fn test_verbose_flag_counts_occurrences() -> anyhow::Result<()> {
        let matches =
            new().try_get_matches_from(["pxs", "--stdio", "--destination", ".", "-vvv"])?;
        assert_eq!(matches.get_count("verbose"), 3);
        Ok(())
    }

    #[test]
    fn test_threshold_rejects_out_of_range_values() {
        let too_small = new().try_get_matches_from([
            "pxs",
            "--stdio",
            "--destination",
            ".",
            "--threshold",
            "0.01",
        ]);
        assert!(too_small.is_err());

        let too_large = new().try_get_matches_from([
            "pxs",
            "--stdio",
            "--destination",
            ".",
            "--threshold",
            "1.5",
        ]);
        assert!(too_large.is_err());
    }

    #[test]
    fn test_stdio_conflicts_with_remote() {
        let matches = new().try_get_matches_from([
            "pxs",
            "--stdio",
            "--remote",
            "127.0.0.1:9000",
            "--destination",
            ".",
        ]);
        assert!(matches.is_err());
    }
}
