use crate::cli::actions::{Action, RemoteEndpoint};
use clap::{ArgMatches, parser::ValueSource};
use std::path::PathBuf;

const PUBLIC_USAGE_HINT: &str = "public CLI now uses subcommands. Examples: \
    `pxs sync SRC DST`, `pxs push SRC ENDPOINT`, `pxs pull ENDPOINT DST`, \
    `pxs listen ADDR DST`, `pxs serve ADDR SRC`.";

/// Main command dispatcher.
///
/// # Errors
///
/// Returns an error if the parsed CLI arguments do not form a valid action.
pub fn handler(matches: &ArgMatches) -> anyhow::Result<Action> {
    if matches.get_flag("stdio") {
        return handle_internal_stdio(matches);
    }

    match matches.subcommand() {
        Some(("sync", submatches)) => handle_sync(submatches),
        Some(("push", submatches)) => handle_push(submatches),
        Some(("pull", submatches)) => handle_pull(submatches),
        Some(("listen", submatches)) => handle_listen(submatches),
        Some(("serve", submatches)) => handle_serve(submatches),
        Some((other, _)) => anyhow::bail!("unsupported subcommand: {other}"),
        None => anyhow::bail!("{PUBLIC_USAGE_HINT}"),
    }
}

fn parse_ignores(matches: &ArgMatches) -> Vec<String> {
    let mut ignores: Vec<String> = matches
        .get_many::<String>("ignore")
        .unwrap_or_default()
        .cloned()
        .collect();

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

fn required_path(matches: &ArgMatches, id: &str) -> anyhow::Result<PathBuf> {
    matches
        .get_one::<PathBuf>(id)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("missing required path argument: {id}"))
}

fn required_string(matches: &ArgMatches, id: &str) -> anyhow::Result<String> {
    matches
        .get_one::<String>(id)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("missing required argument: {id}"))
}

fn threshold(matches: &ArgMatches) -> f32 {
    *matches.get_one::<f32>("threshold").unwrap_or(&0.5)
}

fn parse_remote_endpoint(endpoint: &str) -> RemoteEndpoint {
    if endpoint == "-" {
        return RemoteEndpoint::Stdio;
    }

    if let Some(ssh) = parse_ssh_endpoint(endpoint) {
        return RemoteEndpoint::Ssh {
            host: ssh.host,
            path: ssh.path,
        };
    }

    RemoteEndpoint::Tcp(endpoint.to_string())
}

fn handle_sync(matches: &ArgMatches) -> anyhow::Result<Action> {
    Ok(Action::Sync {
        src: required_path(matches, "src")?,
        dst: required_path(matches, "dst")?,
        threshold: threshold(matches),
        checksum: matches.get_flag("checksum"),
        dry_run: matches.get_flag("dry_run"),
        fsync: matches.get_flag("fsync"),
        ignores: parse_ignores(matches),
    })
}

fn handle_push(matches: &ArgMatches) -> anyhow::Result<Action> {
    Ok(Action::Push {
        endpoint: parse_remote_endpoint(&required_string(matches, "endpoint")?),
        src: required_path(matches, "src")?,
        threshold: threshold(matches),
        checksum: matches.get_flag("checksum"),
        ignores: parse_ignores(matches),
    })
}

fn handle_pull(matches: &ArgMatches) -> anyhow::Result<Action> {
    let endpoint_text = required_string(matches, "endpoint")?;
    let endpoint = parse_remote_endpoint(&endpoint_text);
    let threshold = threshold(matches);
    let checksum = matches.get_flag("checksum");
    let ignores = parse_ignores(matches);

    if matches_source_side_pull_flags(matches, checksum, &ignores)
        && matches!(endpoint, RemoteEndpoint::Tcp(_) | RemoteEndpoint::Stdio)
    {
        anyhow::bail!(
            "source-side flags on raw TCP pull are not supported; configure `pxs serve` instead"
        );
    }

    Ok(Action::Pull {
        endpoint,
        dst: required_path(matches, "dst")?,
        threshold,
        checksum,
        fsync: matches.get_flag("fsync"),
        ignores,
    })
}

fn handle_listen(matches: &ArgMatches) -> anyhow::Result<Action> {
    Ok(Action::Listen {
        addr: required_string(matches, "addr")?,
        dst: required_path(matches, "dst")?,
        fsync: matches.get_flag("fsync"),
    })
}

fn handle_serve(matches: &ArgMatches) -> anyhow::Result<Action> {
    Ok(Action::Serve {
        addr: required_string(matches, "addr")?,
        src: required_path(matches, "src")?,
        threshold: threshold(matches),
        checksum: matches.get_flag("checksum"),
        ignores: parse_ignores(matches),
    })
}

fn handle_internal_stdio(matches: &ArgMatches) -> anyhow::Result<Action> {
    let threshold = threshold(matches);
    let checksum = matches.get_flag("checksum");

    if matches.get_flag("sender") {
        return Ok(Action::InternalStdioSend {
            src: required_path(matches, "source")?,
            threshold,
            checksum,
            ignores: parse_ignores(matches),
        });
    }

    Ok(Action::InternalStdioReceive {
        dst: required_path(matches, "destination")?,
        fsync: matches.get_flag("fsync"),
    })
}

fn matches_source_side_pull_flags(
    matches: &ArgMatches,
    checksum: bool,
    ignores: &[String],
) -> bool {
    checksum
        || !ignores.is_empty()
        || matches.value_source("exclude_from").is_some()
        || matches
            .value_source("threshold")
            .is_some_and(|source| source != ValueSource::DefaultValue)
}

struct SshInfo {
    host: String,
    path: String,
}

fn parse_ssh_endpoint(endpoint: &str) -> Option<SshInfo> {
    let is_ssh = if endpoint.contains('@') {
        true
    } else if let Some((_prefix, suffix)) = endpoint.rsplit_once(':') {
        if endpoint.parse::<std::net::SocketAddr>().is_ok() {
            false
        } else {
            let is_numeric_port = !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit());
            !is_numeric_port
        }
    } else {
        false
    };

    if is_ssh && let Some((prefix, suffix)) = endpoint.split_once(':') {
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
    use crate::cli::{
        actions::{Action, RemoteEndpoint},
        commands,
    };
    use tempfile::tempdir;

    fn parse_action(args: &[&str]) -> anyhow::Result<Action> {
        let matches = commands::new().try_get_matches_from(args)?;
        handler(&matches)
    }

    fn assert_threshold(actual: f32, expected: f32) {
        assert!((actual - expected).abs() < f32::EPSILON);
    }

    #[test]
    fn test_sync_action_parses_public_flags() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src.txt");
        let dst = dir.path().join("dst.txt");
        std::fs::write(&src, "content")?;
        let src_arg = src.to_string_lossy().to_string();
        let dst_arg = dst.to_string_lossy().to_string();
        let action = parse_action(&[
            "pxs",
            "sync",
            &src_arg,
            &dst_arg,
            "--threshold",
            "0.25",
            "--checksum",
            "--dry-run",
            "--fsync",
        ])?;

        match action {
            Action::Sync {
                threshold,
                checksum,
                dry_run,
                fsync,
                ..
            } => {
                assert_threshold(threshold, 0.25);
                assert!(checksum);
                assert!(dry_run);
                assert!(fsync);
            }
            other => anyhow::bail!("expected Action::Sync, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_push_ssh_endpoint_parses_remote_path() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src.txt");
        std::fs::write(&src, "content")?;
        let src_arg = src.to_string_lossy().to_string();

        let action = parse_action(&[
            "pxs",
            "push",
            &src_arg,
            "user@example:/srv/data",
            "--checksum",
            "--threshold",
            "0.75",
        ])?;

        match action {
            Action::Push {
                endpoint,
                threshold,
                checksum,
                ..
            } => {
                assert_eq!(
                    endpoint,
                    RemoteEndpoint::Ssh {
                        host: "user@example".to_string(),
                        path: "/srv/data".to_string(),
                    }
                );
                assert_threshold(threshold, 0.75);
                assert!(checksum);
            }
            other => anyhow::bail!("expected Action::Push, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_push_stdio_endpoint_is_preserved_for_manual_piping() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src.txt");
        std::fs::write(&src, "content")?;
        let src_arg = src.to_string_lossy().to_string();

        let action = parse_action(&["pxs", "push", &src_arg, "-"])?;
        match action {
            Action::Push {
                endpoint: RemoteEndpoint::Stdio,
                ..
            } => {}
            other => anyhow::bail!("expected stdio push endpoint, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn test_pull_ssh_endpoint_parses_remote_path_and_flags() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let dst = dir.path().join("dst");
        std::fs::create_dir_all(&dst)?;
        let dst_arg = dst.to_string_lossy().to_string();

        let action = parse_action(&[
            "pxs",
            "pull",
            "user@example:/srv/data",
            &dst_arg,
            "--checksum",
            "--threshold",
            "0.8",
            "--ignore",
            "*.tmp",
            "--fsync",
        ])?;

        match action {
            Action::Pull {
                endpoint,
                threshold,
                checksum,
                fsync,
                ignores,
                ..
            } => {
                assert_eq!(
                    endpoint,
                    RemoteEndpoint::Ssh {
                        host: "user@example".to_string(),
                        path: "/srv/data".to_string(),
                    }
                );
                assert_threshold(threshold, 0.8);
                assert!(checksum);
                assert!(fsync);
                assert_eq!(ignores, vec!["*.tmp"]);
            }
            other => anyhow::bail!("expected Action::Pull, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_pull_tcp_rejects_source_side_flags() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let dst = dir.path().join("dst");
        std::fs::create_dir_all(&dst)?;
        let dst_arg = dst.to_string_lossy().to_string();

        let Err(error) = parse_action(&["pxs", "pull", "127.0.0.1:9999", &dst_arg, "--checksum"])
        else {
            anyhow::bail!("raw TCP pull should reject source-side flags");
        };

        assert!(error.to_string().contains("configure `pxs serve` instead"));
        Ok(())
    }

    #[test]
    fn test_listen_and_serve_parse_expected_actions() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        std::fs::create_dir_all(&src)?;
        std::fs::create_dir_all(&dst)?;
        let src_arg = src.to_string_lossy().to_string();
        let dst_arg = dst.to_string_lossy().to_string();

        let listen = parse_action(&["pxs", "listen", "127.0.0.1:9999", &dst_arg, "--fsync"])?;
        match listen {
            Action::Listen { fsync, .. } => assert!(fsync),
            other => anyhow::bail!("expected Action::Listen, got {other:?}"),
        }

        let serve = parse_action(&[
            "pxs",
            "serve",
            "127.0.0.1:9999",
            &src_arg,
            "--threshold",
            "0.9",
            "--checksum",
            "--ignore",
            "*.wal",
        ])?;
        match serve {
            Action::Serve {
                threshold,
                checksum,
                ignores,
                ..
            } => {
                assert_threshold(threshold, 0.9);
                assert!(checksum);
                assert_eq!(ignores, vec!["*.wal"]);
            }
            other => anyhow::bail!("expected Action::Serve, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_ignore_and_exclude_from_patterns_are_merged() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        std::fs::create_dir_all(&src)?;
        std::fs::create_dir_all(&dst)?;
        let exclude_file = dir.path().join("exclude.txt");
        std::fs::write(&exclude_file, "# comment\n*.log\n\ncache/\n")?;

        let src_arg = src.to_string_lossy().to_string();
        let dst_arg = dst.to_string_lossy().to_string();
        let exclude_arg = exclude_file.to_string_lossy().to_string();
        let action = parse_action(&[
            "pxs",
            "sync",
            &src_arg,
            &dst_arg,
            "--ignore",
            "*.tmp",
            "--exclude-from",
            &exclude_arg,
        ])?;

        match action {
            Action::Sync { ignores, .. } => {
                assert_eq!(ignores, vec!["*.tmp", "*.log", "cache/"]);
            }
            other => anyhow::bail!("expected Action::Sync, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_internal_stdio_sender_parses_hidden_mode() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src.txt");
        std::fs::write(&src, "content")?;
        let src_arg = src.to_string_lossy().to_string();
        let action = parse_action(&[
            "pxs",
            "--stdio",
            "--sender",
            "--source",
            &src_arg,
            "--threshold",
            "0.8",
            "--checksum",
            "--ignore",
            "*.tmp",
        ])?;

        match action {
            Action::InternalStdioSend {
                threshold,
                checksum,
                ignores,
                ..
            } => {
                assert_threshold(threshold, 0.8);
                assert!(checksum);
                assert_eq!(ignores, vec!["*.tmp"]);
            }
            other => anyhow::bail!("expected Action::InternalStdioSend, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_old_flat_public_syntax_is_rejected() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src.txt");
        let dst = dir.path().join("dst.txt");
        std::fs::write(&src, "content")?;
        let src_arg = src.to_string_lossy().to_string();
        let dst_arg = dst.to_string_lossy().to_string();

        let Err(error) = parse_action(&["pxs", "--source", &src_arg, "--destination", &dst_arg])
        else {
            anyhow::bail!("old flat syntax should be rejected");
        };
        assert!(
            error
                .to_string()
                .contains("public CLI now uses subcommands")
        );
        Ok(())
    }
}
