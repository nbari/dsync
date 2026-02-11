use clap::{
    Arg, ArgAction, ColorChoice, Command,
    builder::{
        ValueParser,
        styling::{AnsiColor, Effects, Styles},
    },
};
use std::{env, path::PathBuf};

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

pub fn new() -> Command {
    let styles = Styles::styled()
        .header(AnsiColor::Yellow.on_default() | Effects::BOLD)
        .usage(AnsiColor::Green.on_default() | Effects::BOLD)
        .literal(AnsiColor::Blue.on_default() | Effects::BOLD)
        .placeholder(AnsiColor::Green.on_default());

    Command::new("dsync")
        .about("High-performance, concurrent synchronization tool for massive data transfers.")
        .long_about(
            "dsync is a multi-threaded file synchronization tool designed to saturate \
            high-speed networks (10GbE+) and utilize multi-core CPUs for hashing. \
            It uses 64KB fixed-block chunking and XxHash64 for extremely fast comparison, \
            making it ideal for large database files like Postgres PG_DATA.\n\n\
            EXAMPLES:\n\n\
            1. Local Sync (File or Directory):\n\
               dsync --source /data/old --destination /data/new\n\n\
            2. Network Sync - Receiver (Server 2):\n\
               dsync --listen 0.0.0.0:8080 --destination /new/pgdata\n\n\
            3. Network Sync - Sender (Server 1):\n\
               dsync --remote 192.168.1.10:8080 --source /old/pgdata\n\n\
            4. Force Content Verification (Checksum):\n\
               dsync --source file.bin --destination copy.bin --checksum"
        )
        .version(env!("CARGO_PKG_VERSION"))
        .color(ColorChoice::Auto)
        .styles(styles)
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
                .long_help("By default, dsync skips files if size and modification time match. Use this to force a block-by-block hash comparison.")
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
                .help("Listen on ADDRESS:PORT for incoming sync")
                .long_help("Starts dsync in server mode. Use with --destination to specify where files should be saved.")
                .value_name("ADDR"),
        )
        .arg(
            Arg::new("remote")
                .short('r')
                .long("remote")
                .help("Sync to remote ADDRESS:PORT instead of local")
                .long_help("Connects to a listening dsync instance. Use with --source to specify which files to send.")
                .value_name("ADDR"),
        )
}
