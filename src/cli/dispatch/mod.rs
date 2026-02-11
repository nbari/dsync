use crate::cli::actions::Action;
use std::path::PathBuf;

pub fn handler(matches: &clap::ArgMatches) -> Action {
    let checksum = matches.get_flag("checksum");
    let dry_run = matches.get_flag("dry_run");
    let mut ignores: Vec<String> = matches
        .get_many::<String>("ignore")
        .unwrap_or_default()
        .cloned()
        .collect();

    // Read additional patterns from file if provided
    if let Some(file_path) = matches.get_one::<PathBuf>("exclude_from")
        && let Ok(content) = std::fs::read_to_string(file_path)
    {
        for line in content.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() && !trimmed.starts_with('#') {
                ignores.push(trimmed.to_string());
            }
        }
    }

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
            let remote_path = if path.is_empty() {
                ".".to_string()
            } else {
                path.to_string()
            };
            return Action::Connect {
                addr: ssh_addr.to_string(),
                src,
                checksum,
                remote_path: Some(remote_path),
                ignores,
            };
        }

        return Action::Connect {
            addr: addr.clone(),
            src,
            checksum,
            remote_path: None,
            ignores,
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
        ignores,
    }
}
