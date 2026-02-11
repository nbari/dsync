use crate::cli::actions::Action;
use std::path::PathBuf;

pub fn handler(matches: &clap::ArgMatches) -> Action {
    let checksum = matches.get_flag("checksum");
    let dry_run = matches.get_flag("dry_run");

    if let Some(addr) = matches.get_one::<String>("listen") {
        let dst = matches
            .get_one::<PathBuf>("destination")
            .cloned()
            .unwrap_or_else(|| PathBuf::from("."));
        return Action::Listen {
            addr: addr.clone(),
            dst,
        };
    }

    if let Some(addr) = matches.get_one::<String>("remote") {
        let src = matches
            .get_one::<PathBuf>("source")
            .cloned()
            .unwrap_or_default();
        return Action::Connect {
            addr: addr.clone(),
            src,
            checksum,
        };
    }

    let src = matches
        .get_one::<PathBuf>("source")
        .cloned()
        .unwrap_or_default();
    let dst = matches
        .get_one::<PathBuf>("destination")
        .cloned()
        .unwrap_or_default();
    let threshold = *matches.get_one::<f32>("threshold").unwrap_or(&0.5);

    Action::Run {
        src,
        dst,
        threshold,
        checksum,
        dry_run,
    }
}
