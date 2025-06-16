use crate::cli::actions::Action;
use anyhow::Result;
use std::path::PathBuf;

pub fn handler(matches: &clap::ArgMatches) -> Result<Action> {
    Ok(Action::Run {
        src: matches.get_one::<PathBuf>("source").unwrap().to_path_buf(),
        dst: matches
            .get_one::<PathBuf>("destination")
            .unwrap()
            .to_path_buf(),
        threshold: *matches.get_one::<f32>("threshold").unwrap_or(&0.5),
    })
}
