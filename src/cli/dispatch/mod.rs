use crate::cli::actions::Action;
use std::path::PathBuf;

pub fn handler(matches: &clap::ArgMatches) -> Action {
    let threshold = *matches.get_one::<f32>("threshold").unwrap_or(&0.5);
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

        // Check if it's an SSH style address: user@host:path or host:path
        // Heuristic: if it contains a colon and the suffix is NOT a port number, it's SSH.
        // If there's an '@', it's always SSH.
        if let Some((prefix, suffix)) = addr.split_once(':') {
            let is_numeric_port = !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit());
            let has_user = prefix.contains('@');

            if has_user || !is_numeric_port {
                let remote_path = if suffix.is_empty() {
                    ".".to_string()
                } else {
                    suffix.to_string()
                };
                return Action::Connect {
                    addr: prefix.to_string(),
                    src,
                    threshold,
                    checksum,
                    remote_path: Some(remote_path),
                    ignores,
                };
            }
        }

        return Action::Connect {
            addr: addr.clone(),
            src,
            threshold,
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
    Action::Run {
        src,
        dst,
        threshold,
        checksum,
        dry_run,
        ignores,
    }
}
