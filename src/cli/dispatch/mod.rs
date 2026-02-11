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

        // Check if it's an SSH style address: user@host:/path
        if let Some((ssh_addr, path)) = addr.split_once(':') {
            return Action::Connect {
                addr: ssh_addr.to_string(),
                src,
                checksum,
                remote_path: Some(path.to_string()),
            };
        }

        return Action::Connect {
            addr: addr.clone(),
            src,
            checksum,
            remote_path: None,
        };
    }

    if matches.get_flag("stdio") {
        let dst = matches
            .get_one::<PathBuf>("destination")
            .cloned()
            .unwrap_or_else(|| PathBuf::from("."));
        return Action::Stdio { dst };
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
