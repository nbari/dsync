use clap::{
    Arg, ArgAction, ColorChoice, Command,
    builder::{
        ValueParser,
        styling::{AnsiColor, Effects, Styles},
    },
};
use std::{env, fs, path::PathBuf};

pub fn validator_is_file() -> ValueParser {
    ValueParser::from(move |s: &str| -> std::result::Result<PathBuf, String> {
        if let Ok(metadata) = fs::metadata(s) {
            if metadata.is_file() {
                return Ok(PathBuf::from(s));
            }
        }

        Err(format!("Invalid file path of file does not exists: '{s}'"))
    })
}

pub fn validator_parent_exist() -> ValueParser {
    ValueParser::from(move |s: &str| -> std::result::Result<PathBuf, String> {
        let path = PathBuf::from(s);
        if path.exists() && path.is_dir() {
            return Ok(path);
        }

        if let Some(parent) = path.parent() {
            if parent.exists() && parent.is_dir() {
                return Ok(path);
            }
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
        .about("synchronization tool")
        .version(env!("CARGO_PKG_VERSION"))
        .color(ColorChoice::Auto)
        .styles(styles)
        .arg(
            Arg::new("source")
                .short('s')
                .long("source")
                .help("Path to the source file or directory")
                .value_parser(validator_is_file())
                .value_name("SRC")
                .required(true),
        )
        .arg(
            Arg::new("destination")
                .short('d')
                .long("destination")
                .help("Path to the destination file or directory")
                .value_parser(validator_parent_exist())
                .value_name("DST")
                .required(true),
        )
        .arg(
            Arg::new("threshold")
                .short('t')
                .long("threshold")
                .help("Threshold to determine if a file should be copied")
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
}
