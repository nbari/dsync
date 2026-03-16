use crate::built_info;
use clap::{
    Arg, ArgAction, ColorChoice, Command,
    builder::{
        ValueParser,
        styling::{AnsiColor, Effects, Styles},
    },
};
use std::path::PathBuf;

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

#[allow(clippy::too_many_lines)]
pub fn new() -> Command {
    let styles = Styles::styled()
        .header(AnsiColor::Yellow.on_default() | Effects::BOLD)
        .usage(AnsiColor::Green.on_default() | Effects::BOLD)
        .literal(AnsiColor::Blue.on_default() | Effects::BOLD)
        .placeholder(AnsiColor::Green.on_default());

    Command::new("pxs")
        .about("pxs (Parallel X-Sync) - High-performance, concurrent file synchronization tool.")
        .long_about(
            "pxs is a multi-threaded file synchronization tool designed to saturate \
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
               pxs --source file.bin --destination copy.bin --checksum"
        )
        .version(long_version())
        .color(ColorChoice::Auto)
        .styles(styles)
        .arg_required_else_help(true)
        .arg(
            Arg::new("source")
                .short('s')
                .long("source")
                .help("Path to the source file or directory")
                .long_help("The local path to read data from. When using --remote, this is the data sent to the listener.")
                .value_parser(validator_path_exists())
                .value_name("SRC"),
        )
        .arg(
            Arg::new("destination")
                .short('d')
                .long("destination")
                .help("Path to the destination file or directory")
                .long_help("The local path to write data to. When using --listen, incoming data is stored here.")
                .value_parser(validator_parent_exist())
                .value_name("DST"),
        )
        .group(
            clap::ArgGroup::new("mode")
                .args(["listen", "remote", "source", "destination"])
                .required(true)
                .multiple(true),
        )
        .arg(
            Arg::new("threshold")
                .short('t')
                .long("threshold")
                .help("Threshold to determine if a file should be copied")
                .long_help("Value between 0.1 and 1.0. If the destination file size is less than this percentage of the source, a full rewrite is performed.")
                .value_name("THRESHOLD")
                .value_parser(clap::builder::ValueParser::from(|s: &str| {
                    let val: f32 = s.parse().map_err(|_| String::from("Invalid float"))?;
                    if (0.1..=1.0).contains(&val) {
                        Ok(val)
                    } else {
                        Err(String::from("Threshold must be between 0.1 and 1.0"))
                    }
                }))
                .default_value("0.5"),
        )
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .help("Increase verbosity, -vv for debug")
                .action(ArgAction::Count),
        )
        .arg(
            Arg::new("checksum")
                .short('c')
                .long("checksum")
                .help("Skip based on checksum, not mod-time & size")
                .long_help("By default, pxs skips files if size and modification time match. Use this to force a block-by-block hash comparison.")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("dry_run")
                .short('n')
                .long("dry-run")
                .help("Show what would have been transferred")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("listen")
                .short('l')
                .long("listen")
                .help("Listen on ADDRESS:PORT for incoming or outgoing sync")
                .long_help("Starts pxs in server mode. If --destination is used, it receives files. If --source and --sender are used, it serves files to clients.")
                .value_name("ADDR"),
        )
        .arg(
            Arg::new("remote")
                .short('r')
                .long("remote")
                .help("Sync with remote ADDRESS:PORT or SSH path")
                .long_help("Connects to a remote pxs instance. By default, it pushes --source. If --pull is used, it fetches data to --destination.")
                .value_name("ADDR"),
        )
        .arg(
            Arg::new("stdio")
                .long("stdio")
                .help("Use stdin/stdout for communication (internal use for SSH)")
                .action(ArgAction::SetTrue)
                .conflicts_with_all(["listen", "remote"]),
        )
        .arg(
            Arg::new("pull")
                .long("pull")
                .short('P')
                .help("Pull files from a remote source")
                .long_help("When used with --remote, it initiates a connection to fetch data from the remote source to the local --destination.")
                .action(ArgAction::SetTrue)
                .requires("destination")
                .conflicts_with("source"),
        )
        .arg(
            Arg::new("sender")
                .long("sender")
                .help("Run in sender mode (serves files to clients)")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("ignore")
                .short('i')
                .long("ignore")
                .help("Ignore files/directories matching this pattern (glob)")
                .action(ArgAction::Append)
                .value_name("PATTERN"),
        )
        .arg(
            Arg::new("exclude_from")
                .short('E')
                .long("exclude-from")
                .help("Read exclude patterns from FILE")
                .value_name("FILE")
                .value_parser(validator_path_exists()),
        )
}
