use crate::cli::actions::Action;
use std::path::PathBuf;

/// Main command dispatcher
#[must_use]
pub fn handler(matches: &clap::ArgMatches) -> Action {
    let threshold = *matches.get_one::<f32>("threshold").unwrap_or(&0.5);
    let checksum = matches.get_flag("checksum");
    let dry_run = matches.get_flag("dry_run");
    let fsync = matches.get_flag("fsync");
    let ignores = parse_ignores(matches);

    if let Some(addr) = matches.get_one::<String>("listen") {
        return handle_listen(addr, matches, threshold, checksum, fsync, ignores);
    }

    if let Some(addr) = matches.get_one::<String>("remote") {
        let pull = matches.get_flag("pull");
        if pull {
            return handle_pull(addr, matches, threshold, checksum, fsync, ignores);
        }
        return handle_remote(addr, matches, threshold, checksum, ignores);
    }

    if matches.get_flag("stdio") {
        if matches.get_flag("sender") {
            let src = get_source(matches);
            return Action::StdioSender {
                src,
                threshold,
                checksum,
                ignores,
            };
        }
        let dst = get_destination(matches);
        return Action::Stdio { dst, fsync };
    }

    let src = get_source(matches);
    let dst = get_destination(matches);
    Action::Run {
        src,
        dst,
        threshold,
        checksum,
        dry_run,
        fsync,
        ignores,
    }
}

fn parse_ignores(matches: &clap::ArgMatches) -> Vec<String> {
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
    ignores
}

fn get_source(matches: &clap::ArgMatches) -> PathBuf {
    matches
        .get_one::<PathBuf>("source")
        .cloned()
        .unwrap_or_default()
}

fn get_destination(matches: &clap::ArgMatches) -> PathBuf {
    matches
        .get_one::<PathBuf>("destination")
        .cloned()
        .unwrap_or_else(|| PathBuf::from("."))
}

fn handle_listen(
    addr: &str,
    matches: &clap::ArgMatches,
    threshold: f32,
    checksum: bool,
    fsync: bool,
    ignores: Vec<String>,
) -> Action {
    if matches.get_flag("sender") {
        let src = get_source(matches);
        return Action::ListenSender {
            addr: addr.to_string(),
            src,
            threshold,
            checksum,
            ignores,
        };
    }
    let dst = get_destination(matches);
    Action::Listen {
        addr: addr.to_string(),
        dst,
        fsync,
    }
}

fn handle_pull(
    addr: &str,
    matches: &clap::ArgMatches,
    threshold: f32,
    checksum: bool,
    fsync: bool,
    ignores: Vec<String>,
) -> Action {
    let dst = get_destination(matches);

    if let Some(ssh_info) = parse_ssh_address(addr) {
        return Action::Pull {
            addr: ssh_info.host,
            dst,
            threshold,
            checksum,
            fsync,
            remote_path: Some(ssh_info.path),
            ignores,
        };
    }

    Action::Pull {
        addr: addr.to_string(),
        dst,
        threshold,
        checksum,
        fsync,
        remote_path: None,
        ignores,
    }
}

fn handle_remote(
    addr: &str,
    matches: &clap::ArgMatches,
    threshold: f32,
    checksum: bool,
    ignores: Vec<String>,
) -> Action {
    let src = get_source(matches);

    if addr == "-" {
        return Action::Connect {
            addr: addr.to_string(),
            src,
            threshold,
            checksum,
            remote_path: None,
            ignores,
        };
    }

    if let Some(ssh_info) = parse_ssh_address(addr) {
        return Action::Connect {
            addr: ssh_info.host,
            src,
            threshold,
            checksum,
            remote_path: Some(ssh_info.path),
            ignores,
        };
    }

    Action::Connect {
        addr: addr.to_string(),
        src,
        threshold,
        checksum,
        remote_path: None,
        ignores,
    }
}

struct SshInfo {
    host: String,
    path: String,
}

fn parse_ssh_address(addr: &str) -> Option<SshInfo> {
    // 1. If there's an '@', it's always SSH.
    // 2. If it contains a colon, check if it's a valid SocketAddr.
    // 3. If it's not a SocketAddr but has a colon, it might be an SSH path.
    let is_ssh = if addr.contains('@') {
        true
    } else if let Some((_prefix, suffix)) = addr.rsplit_once(':') {
        if addr.parse::<std::net::SocketAddr>().is_ok() {
            false
        } else {
            let is_numeric_port = !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit());
            !is_numeric_port
        }
    } else {
        false
    };

    if is_ssh && let Some((prefix, suffix)) = addr.split_once(':') {
        let path = if suffix.is_empty() {
            ".".to_string()
        } else {
            suffix.to_string()
        };
        return Some(SshInfo {
            host: prefix.to_string(),
            path,
        });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::handler;
    use crate::cli::{actions::Action, commands};
    use tempfile::tempdir;

    fn parse_action(args: &[&str]) -> anyhow::Result<Action> {
        let matches = commands::new().try_get_matches_from(args)?;
        Ok(handler(&matches))
    }

    #[test]
    fn test_short_fsync_flag_sets_local_run_action() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src.txt");
        let dst = dir.path().join("dst.txt");
        std::fs::write(&src, "content")?;

        let src_arg = src.to_string_lossy().to_string();
        let dst_arg = dst.to_string_lossy().to_string();
        let action = parse_action(&["pxs", "--source", &src_arg, "--destination", &dst_arg, "-f"])?;

        match action {
            Action::Run { fsync, .. } => assert!(fsync),
            other => anyhow::bail!("expected Action::Run, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_long_fsync_flag_sets_receiver_modes() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let dst = dir.path().join("dst");
        std::fs::create_dir_all(&dst)?;
        let dst_arg = dst.to_string_lossy().to_string();

        let listen = parse_action(&[
            "pxs",
            "--listen",
            "127.0.0.1:9999",
            "--destination",
            &dst_arg,
            "--fsync",
        ])?;
        match listen {
            Action::Listen { fsync, .. } => assert!(fsync),
            other => anyhow::bail!("expected Action::Listen, got {other:?}"),
        }

        let pull = parse_action(&[
            "pxs",
            "--remote",
            "127.0.0.1:9999",
            "--destination",
            &dst_arg,
            "--pull",
            "--fsync",
        ])?;
        match pull {
            Action::Pull { fsync, .. } => assert!(fsync),
            other => anyhow::bail!("expected Action::Pull, got {other:?}"),
        }

        let stdio = parse_action(&["pxs", "--stdio", "--destination", &dst_arg, "--fsync"])?;
        match stdio {
            Action::Stdio { fsync, .. } => assert!(fsync),
            other => anyhow::bail!("expected Action::Stdio, got {other:?}"),
        }

        Ok(())
    }
}
