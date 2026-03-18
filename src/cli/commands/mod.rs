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

/// Generate extended version string with git info
fn long_version() -> &'static str {
    let version = built_info::PKG_VERSION;
    let git_hash = built_info::GIT_COMMIT_HASH_SHORT.unwrap_or("unknown");
    let git_dirty = if built_info::GIT_DIRTY.unwrap_or(false) {
        "-dirty"
    } else {
        ""
    };

    let version_string = format!("{version} (git: {git_hash}{git_dirty})");
    // Leak the string to get a 'static lifetime - this is fine for CLI version string
    Box::leak(version_string.into_boxed_str())
}

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

fn source_arg() -> Arg {
    Arg::new("source")
        .short('s')
        .long("source")
        .help("Path to the source file or directory")
        .long_help(
            "The local path to read data from. When using --remote, this is the data sent to the listener.",
        )
        .value_parser(validator_path_exists())
        .value_name("SRC")
}

fn destination_arg() -> Arg {
    Arg::new("destination")
        .short('d')
        .long("destination")
        .help("Path to the destination file or directory")
        .long_help(
            "The local path to write data to. When using --listen, incoming data is stored here.",
        )
        .value_parser(validator_parent_exist())
        .value_name("DST")
}

fn mode_group() -> ArgGroup {
    ArgGroup::new("mode")
        .args(["listen", "remote", "source", "destination"])
        .required(true)
        .multiple(true)
}

fn threshold_arg() -> Arg {
    Arg::new("threshold")
        .short('t')
        .long("threshold")
        .help("Threshold to determine if a file should be copied")
        .long_help(
            "Value between 0.1 and 1.0. If the destination file size is less than this percentage of the source, a full rewrite is performed.",
        )
        .value_name("THRESHOLD")
        .value_parser(clap::builder::ValueParser::from(|s: &str| {
            let val: f32 = s.parse().map_err(|_| String::from("Invalid float"))?;
            if (0.1..=1.0).contains(&val) {
                Ok(val)
            } else {
                Err(String::from("Threshold must be between 0.1 and 1.0"))
            }
        }))
        .default_value("0.5")
}

fn verbose_arg() -> Arg {
    Arg::new("verbose")
        .short('v')
        .long("verbose")
        .help("Increase verbosity, -vv for debug")
        .action(ArgAction::Count)
}

fn checksum_arg() -> Arg {
    Arg::new("checksum")
        .short('c')
        .long("checksum")
        .help("Skip based on checksum, not mod-time & size")
        .long_help(
            "By default, pxs skips files if size and modification time match. Use this to force a block-by-block hash comparison. In network mode, pxs also performs end-to-end BLAKE3 verification after the transfer completes.",
        )
        .action(ArgAction::SetTrue)
}

fn fsync_arg() -> Arg {
    Arg::new("fsync")
        .short('f')
        .long("fsync")
        .help("Force fsync after writing files")
        .long_help(
            "Ensures that file data and metadata are flushed to disk before finishing. Slower but safer.",
        )
        .action(ArgAction::SetTrue)
}

fn dry_run_arg() -> Arg {
    Arg::new("dry_run")
        .short('n')
        .long("dry-run")
        .help("Show what would have been transferred")
        .action(ArgAction::SetTrue)
}

fn listen_arg() -> Arg {
    Arg::new("listen")
        .short('l')
        .long("listen")
        .help("Listen on ADDRESS:PORT for incoming or outgoing sync")
        .long_help(
            "Starts pxs in server mode. If --destination is used, it receives files. If --source and --sender are used, it serves files to clients.",
        )
        .value_name("ADDR")
}

fn remote_arg() -> Arg {
    Arg::new("remote")
        .short('r')
        .long("remote")
        .help("Sync with remote ADDRESS:PORT or SSH path")
        .long_help(
            "Connects to a remote pxs instance. By default, it pushes --source. If --pull is used, it fetches data to --destination.",
        )
        .value_name("ADDR")
}

fn stdio_arg() -> Arg {
    Arg::new("stdio")
        .long("stdio")
        .help("Use stdin/stdout for communication (internal use for SSH)")
        .action(ArgAction::SetTrue)
        .conflicts_with_all(["listen", "remote"])
}

fn pull_arg() -> Arg {
    Arg::new("pull")
        .long("pull")
        .short('P')
        .help("Pull files from a remote source")
        .long_help(
            "When used with --remote, it initiates a connection to fetch data from the remote source to the local --destination.",
        )
        .action(ArgAction::SetTrue)
        .requires("destination")
        .conflicts_with("source")
}

fn sender_arg() -> Arg {
    Arg::new("sender")
        .long("sender")
        .help("Run in sender mode (serves files to clients)")
        .action(ArgAction::SetTrue)
}

fn ignore_arg() -> Arg {
    Arg::new("ignore")
        .short('i')
        .long("ignore")
        .help("Ignore files/directories matching this pattern (glob)")
        .action(ArgAction::Append)
        .value_name("PATTERN")
}

fn exclude_from_arg() -> Arg {
    Arg::new("exclude_from")
        .short('E')
        .long("exclude-from")
        .help("Read exclude patterns from FILE")
        .value_name("FILE")
        .value_parser(validator_path_exists())
}

/// Build the CLI command definition.
#[must_use]
pub fn new() -> Command {
    Command::new("pxs")
        .about("pxs (Parallel X-Sync) - High-performance, concurrent file synchronization tool.")
        .long_about(LONG_ABOUT)
        .version(long_version())
        .color(ColorChoice::Auto)
        .styles(cli_styles())
        .arg_required_else_help(true)
        .arg(source_arg())
        .arg(destination_arg())
        .group(mode_group())
        .arg(threshold_arg())
        .arg(verbose_arg())
        .arg(checksum_arg())
        .arg(fsync_arg())
        .arg(dry_run_arg())
        .arg(listen_arg())
        .arg(remote_arg())
        .arg(stdio_arg())
        .arg(pull_arg())
        .arg(sender_arg())
        .arg(ignore_arg())
        .arg(exclude_from_arg())
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
