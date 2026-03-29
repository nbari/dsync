use crate::cli::actions::{Action, RemoteEndpoint, SyncOperand};
use crate::pxs::tools::DEFAULT_THRESHOLD;
use anyhow::Result;
use clap::ArgMatches;
use std::path::{Path, PathBuf};

const PUBLIC_USAGE_HINT: &str = "public CLI now uses subcommands. Examples: \
    `pxs sync SRC DST`, `pxs sync user@host:/src DST`, \
    `pxs sync host:port/src DST`, `pxs listen ADDR ROOT`, `pxs serve ADDR ROOT`.";

/// Main command dispatcher.
///
/// # Errors
///
/// Returns an error if the parsed CLI arguments do not form a valid action.
pub fn handler(matches: &ArgMatches) -> Result<Action> {
    let quiet = matches.get_flag("quiet");

    if matches.get_flag("stdio") {
        return handle_internal_stdio(matches, quiet);
    }

    match matches.subcommand() {
        Some(("sync", submatches)) => handle_sync(submatches, quiet),
        Some(("push", submatches)) => handle_push(submatches, quiet),
        Some(("pull", submatches)) => handle_pull(submatches, quiet),
        Some(("listen", submatches)) => handle_listen(submatches, quiet),
        Some(("serve", submatches)) => handle_serve(submatches, quiet),
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

fn required_path(matches: &ArgMatches, id: &str) -> Result<PathBuf> {
    matches
        .get_one::<PathBuf>(id)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("missing required path argument: {id}"))
}

fn required_string(matches: &ArgMatches, id: &str) -> Result<String> {
    matches
        .get_one::<String>(id)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("missing required argument: {id}"))
}

fn threshold(matches: &ArgMatches) -> f32 {
    *matches
        .get_one::<f32>("threshold")
        .unwrap_or(&DEFAULT_THRESHOLD)
}

fn parse_remote_endpoint(endpoint: &str, allow_stdio: bool) -> Result<RemoteEndpoint> {
    if allow_stdio && endpoint == "-" {
        return Ok(RemoteEndpoint::Stdio);
    }

    if let Some(ssh) = parse_ssh_endpoint(endpoint)? {
        return Ok(RemoteEndpoint::Ssh {
            host: ssh.host,
            path: ssh.path,
        });
    }

    if let Some(tcp) = parse_tcp_endpoint(endpoint)? {
        return Ok(RemoteEndpoint::Tcp {
            addr: tcp.addr,
            path: tcp.path,
        });
    }

    anyhow::bail!("unsupported remote endpoint syntax: {endpoint}")
}

fn validate_local_source_operand(path: &Path) -> Result<()> {
    anyhow::ensure!(path.exists(), "Path does not exist: '{}'", path.display());
    Ok(())
}

fn validate_local_destination_operand(path: &Path) -> Result<()> {
    if path.exists() && path.is_dir() {
        return Ok(());
    }

    let parent = if let Some(parent) = path.parent() {
        if parent.as_os_str().is_empty() {
            PathBuf::from(".")
        } else {
            parent.to_path_buf()
        }
    } else {
        PathBuf::from(".")
    };

    anyhow::ensure!(
        parent.exists() && parent.is_dir(),
        "Invalid destination path or parent directory does not exist: '{}'",
        path.display()
    );
    Ok(())
}

fn parse_sync_operand(value: &str) -> Result<SyncOperand> {
    if value.contains('@') {
        return Ok(SyncOperand::Remote(parse_remote_endpoint(value, false)?));
    }

    if value.contains('[') || value.contains(']') {
        return Ok(SyncOperand::Remote(parse_remote_endpoint(value, false)?));
    }

    if parse_tcp_endpoint(value)?.is_some() {
        return Ok(SyncOperand::Remote(parse_remote_endpoint(value, false)?));
    }

    Ok(SyncOperand::Local(PathBuf::from(value)))
}

fn build_sync_action(
    src: SyncOperand,
    dst: SyncOperand,
    matches: &ArgMatches,
    quiet: bool,
) -> Result<Action> {
    let dry_run = matches
        .try_get_one::<bool>("dry_run")
        .ok()
        .flatten()
        .copied()
        .unwrap_or(false);
    let large_file_parallel_threshold = matches
        .try_get_one::<u64>("large_file_parallel_threshold")
        .ok()
        .flatten()
        .copied()
        .unwrap_or(0);
    let large_file_parallel_workers = matches
        .try_get_one::<usize>("large_file_parallel_workers")
        .ok()
        .flatten()
        .copied()
        .unwrap_or(0);
    let network_file_concurrency = matches
        .try_get_one::<usize>("network_file_concurrency")
        .ok()
        .flatten()
        .copied()
        .unwrap_or(0);

    match (&src, &dst) {
        (SyncOperand::Local(src_path), SyncOperand::Local(dst_path)) => {
            validate_local_source_operand(src_path)?;
            validate_local_destination_operand(dst_path)?;
        }
        (SyncOperand::Local(src_path), SyncOperand::Remote(_)) => {
            validate_local_source_operand(src_path)?;
        }
        (SyncOperand::Remote(_), SyncOperand::Local(dst_path)) => {
            validate_local_destination_operand(dst_path)?;
        }
        (SyncOperand::Remote(_), SyncOperand::Remote(_)) => {
            anyhow::bail!("sync supports at most one remote operand per invocation");
        }
    }

    Ok(Action::Sync {
        src,
        dst,
        threshold: threshold(matches),
        checksum: matches.get_flag("checksum"),
        dry_run,
        delete: matches.get_flag("delete"),
        fsync: matches.get_flag("fsync"),
        large_file_parallel_threshold,
        large_file_parallel_workers,
        network_file_concurrency,
        ignores: parse_ignores(matches),
        quiet,
    })
}

fn handle_sync(matches: &ArgMatches, quiet: bool) -> Result<Action> {
    let src_text = required_string(matches, "src")?;
    let dst_text = required_string(matches, "dst")?;
    let src = parse_sync_operand(&src_text)?;
    let dst = parse_sync_operand(&dst_text)?;

    if matches!(src, SyncOperand::Remote(RemoteEndpoint::Stdio))
        || matches!(dst, SyncOperand::Remote(RemoteEndpoint::Stdio))
    {
        anyhow::bail!("stdio is not supported by `sync`; use `push` for manual piping");
    }

    build_sync_action(src, dst, matches, quiet)
}

fn handle_push(matches: &ArgMatches, quiet: bool) -> Result<Action> {
    let src = required_path(matches, "src")?;
    let endpoint = parse_remote_endpoint(&required_string(matches, "endpoint")?, true)?;
    build_sync_action(
        SyncOperand::Local(src),
        SyncOperand::Remote(endpoint),
        matches,
        quiet,
    )
}

fn handle_pull(matches: &ArgMatches, quiet: bool) -> Result<Action> {
    let endpoint_text = required_string(matches, "endpoint")?;
    let endpoint = parse_remote_endpoint(&endpoint_text, false)?;
    let threshold = threshold(matches);
    let checksum = matches.get_flag("checksum");
    let ignores = parse_ignores(matches);

    anyhow::ensure!(
        !matches!(endpoint, RemoteEndpoint::Stdio),
        "stdio is not supported for pull mode"
    );

    let _ = threshold;
    let _ = checksum;
    let _ = ignores;
    build_sync_action(
        SyncOperand::Remote(endpoint),
        SyncOperand::Local(required_path(matches, "dst")?),
        matches,
        quiet,
    )
}

fn handle_listen(matches: &ArgMatches, quiet: bool) -> Result<Action> {
    Ok(Action::Listen {
        addr: required_string(matches, "addr")?,
        dst: required_path(matches, "dst")?,
        fsync: matches.get_flag("fsync"),
        quiet,
    })
}

fn handle_serve(matches: &ArgMatches, quiet: bool) -> Result<Action> {
    Ok(Action::Serve {
        addr: required_string(matches, "addr")?,
        src: required_path(matches, "src")?,
        threshold: threshold(matches),
        checksum: matches.get_flag("checksum"),
        ignores: parse_ignores(matches),
        quiet,
    })
}

fn handle_internal_stdio(matches: &ArgMatches, quiet: bool) -> Result<Action> {
    let threshold = threshold(matches);
    let checksum = matches.get_flag("checksum");
    let quiet = quiet || matches.get_flag("quiet");

    if matches.get_flag("chunk_writer") {
        return Ok(Action::InternalChunkWrite {
            dst: required_path(matches, "destination")?,
            transfer_id: required_string(matches, "transfer_id")?,
            quiet,
        });
    }

    if matches.get_flag("sender") {
        return Ok(Action::InternalStdioSend {
            src: required_path(matches, "source")?,
            threshold,
            checksum,
            delete: matches.get_flag("delete"),
            ignores: parse_ignores(matches),
            quiet,
        });
    }

    Ok(Action::InternalStdioReceive {
        dst: required_path(matches, "destination")?,
        fsync: matches.get_flag("fsync"),
        ignores: parse_ignores(matches),
        quiet,
    })
}

struct SshInfo {
    host: String,
    path: String,
}

struct TcpInfo {
    addr: String,
    path: Option<String>,
}

fn split_endpoint_host_suffix(endpoint: &str) -> Result<Option<(&str, &str)>> {
    let mut bracket_depth = 0_u8;
    let mut first_colon = None;

    for (index, ch) in endpoint.char_indices() {
        match ch {
            '[' => {
                anyhow::ensure!(
                    bracket_depth == 0,
                    "malformed endpoint `{endpoint}`: nested `[` is not allowed"
                );
                bracket_depth = 1;
            }
            ']' => {
                anyhow::ensure!(
                    bracket_depth == 1,
                    "malformed endpoint `{endpoint}`: unexpected `]`"
                );
                bracket_depth = 0;
            }
            ':' if bracket_depth == 0 => {
                if first_colon.is_none() {
                    first_colon = Some(index);
                }
            }
            _ => {}
        }
    }

    anyhow::ensure!(
        bracket_depth == 0,
        "malformed endpoint `{endpoint}`: missing closing `]`"
    );

    if let Some(index) = first_colon {
        let (host, suffix_with_colon) = endpoint.split_at(index);
        let suffix = suffix_with_colon
            .strip_prefix(':')
            .ok_or_else(|| anyhow::anyhow!("missing endpoint separator after host"))?;
        anyhow::ensure!(
            !host.is_empty(),
            "malformed endpoint `{endpoint}`: missing host before `:`"
        );
        return Ok(Some((host, suffix)));
    }

    if endpoint.starts_with('[') || endpoint.contains("@[") || endpoint.contains(']') {
        anyhow::bail!(
            "malformed bracketed endpoint `{endpoint}`: expected `[host]:PORT` or `[host]:PATH`"
        );
    }

    Ok(None)
}

fn parse_ssh_endpoint(endpoint: &str) -> Result<Option<SshInfo>> {
    if !endpoint.contains('@') {
        return Ok(None);
    }
    let (host, suffix) = split_endpoint_host_suffix(endpoint)?
        .ok_or_else(|| anyhow::anyhow!("SSH endpoint must use `HOST:PATH` syntax: {endpoint}"))?;

    let path = if suffix.is_empty() {
        ".".to_string()
    } else {
        suffix.to_string()
    };
    Ok(Some(SshInfo {
        host: host.to_string(),
        path,
    }))
}

fn parse_tcp_endpoint(endpoint: &str) -> Result<Option<TcpInfo>> {
    let Some((host, suffix)) = split_endpoint_host_suffix(endpoint)? else {
        return Ok(None);
    };

    if host.contains('@') {
        return Ok(None);
    }

    let (port_text, path) = if let Some((port, path)) = suffix.split_once('/') {
        (port, Some(format!("/{path}")))
    } else {
        (suffix, None)
    };

    let Ok(port) = port_text.parse::<u16>() else {
        return Ok(None);
    };

    Ok(Some(TcpInfo {
        addr: format!("{host}:{port}"),
        path,
    }))
}

#[cfg(test)]
mod tests {
    use super::handler;
    use crate::cli::{
        actions::{Action, RemoteEndpoint, SyncOperand},
        commands,
    };
    use crate::pxs::tools::DEFAULT_THRESHOLD;
    use anyhow::Result;
    use tempfile::tempdir;

    fn parse_action(args: &[&str]) -> Result<Action> {
        let matches = commands::new().try_get_matches_from(args)?;
        handler(&matches)
    }

    fn assert_threshold(actual: f32, expected: f32) {
        assert!((actual - expected).abs() < f32::EPSILON);
    }

    #[test]
    fn test_sync_action_parses_public_flags() -> Result<()> {
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
            "--delete",
            "--fsync",
        ])?;

        match action {
            Action::Sync {
                threshold,
                checksum,
                dry_run,
                delete,
                fsync,
                ..
            } => {
                assert_threshold(threshold, 0.25);
                assert!(checksum);
                assert!(dry_run);
                assert!(delete);
                assert!(fsync);
            }
            other => anyhow::bail!("expected Action::Sync, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_sync_action_uses_default_threshold() -> Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src.txt");
        let dst = dir.path().join("dst.txt");
        std::fs::write(&src, "content")?;
        let src_arg = src.to_string_lossy().to_string();
        let dst_arg = dst.to_string_lossy().to_string();

        let action = parse_action(&["pxs", "sync", &src_arg, &dst_arg])?;
        match action {
            Action::Sync {
                src: SyncOperand::Local(_),
                dst: SyncOperand::Local(_),
                threshold,
                ..
            } => assert_threshold(threshold, DEFAULT_THRESHOLD),
            other => anyhow::bail!("expected Action::Sync, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_push_ssh_endpoint_parses_remote_path() -> Result<()> {
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
            "--delete",
            "--threshold",
            "0.75",
            "--fsync",
        ])?;

        match action {
            Action::Sync {
                src: SyncOperand::Local(_),
                dst: SyncOperand::Remote(endpoint),
                threshold,
                checksum,
                delete,
                fsync,
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
                assert!(delete);
                assert!(fsync);
            }
            other => anyhow::bail!("expected Action::Sync, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_push_ssh_endpoint_parses_large_file_parallel_flags() -> Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src.txt");
        std::fs::write(&src, "content")?;
        let src_arg = src.to_string_lossy().to_string();

        let action = parse_action(&[
            "pxs",
            "push",
            &src_arg,
            "user@example:/srv/data",
            "--large-file-parallel-threshold",
            "2GiB",
            "--large-file-parallel-workers",
            "4",
        ])?;

        match action {
            Action::Sync {
                large_file_parallel_threshold,
                large_file_parallel_workers,
                ..
            } => {
                assert_eq!(large_file_parallel_threshold, 2 * 1024_u64.pow(3));
                assert_eq!(large_file_parallel_workers, 4);
            }
            other => anyhow::bail!("expected Action::Sync, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_push_tcp_endpoint_preserves_bracketed_ipv6_socket() -> Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src.txt");
        std::fs::write(&src, "content")?;
        let src_arg = src.to_string_lossy().to_string();

        let action = parse_action(&["pxs", "push", &src_arg, "[::1]:7878"])?;

        match action {
            Action::Sync {
                dst: SyncOperand::Remote(RemoteEndpoint::Tcp { addr, path }),
                ..
            } => {
                assert_eq!(addr, "[::1]:7878");
                assert!(path.is_none());
            }
            other => anyhow::bail!("expected TCP push endpoint, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_push_tcp_endpoint_parses_large_file_parallel_flags() -> Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src.txt");
        std::fs::write(&src, "content")?;
        let src_arg = src.to_string_lossy().to_string();

        let action = parse_action(&[
            "pxs",
            "push",
            &src_arg,
            "127.0.0.1:7878",
            "--large-file-parallel-threshold",
            "64MiB",
            "--large-file-parallel-workers",
            "3",
        ])?;

        match action {
            Action::Sync {
                dst: SyncOperand::Remote(RemoteEndpoint::Tcp { addr, path }),
                large_file_parallel_threshold,
                large_file_parallel_workers,
                ..
            } => {
                assert_eq!(addr, "127.0.0.1:7878");
                assert!(path.is_none());
                assert_eq!(large_file_parallel_threshold, 64 * 1024_u64.pow(2));
                assert_eq!(large_file_parallel_workers, 3);
            }
            other => anyhow::bail!("expected TCP sync action, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_sync_action_parses_network_file_concurrency_flag() -> Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src");
        std::fs::create_dir_all(&src)?;
        let src_arg = src.to_string_lossy().to_string();

        let action = parse_action(&[
            "pxs",
            "sync",
            &src_arg,
            "127.0.0.1:7878",
            "--network-file-concurrency",
            "6",
        ])?;

        match action {
            Action::Sync {
                src: SyncOperand::Local(_),
                dst: SyncOperand::Remote(RemoteEndpoint::Tcp { .. }),
                network_file_concurrency,
                ..
            } => assert_eq!(network_file_concurrency, 6),
            other => anyhow::bail!("expected TCP sync action, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_push_stdio_endpoint_is_preserved_for_manual_piping() -> Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src.txt");
        std::fs::write(&src, "content")?;
        let src_arg = src.to_string_lossy().to_string();

        let action = parse_action(&["pxs", "push", &src_arg, "-"])?;
        match action {
            Action::Sync {
                dst: SyncOperand::Remote(RemoteEndpoint::Stdio),
                ..
            } => {}
            other => anyhow::bail!("expected stdio push endpoint, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn test_pull_ssh_endpoint_parses_remote_path_and_flags() -> Result<()> {
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
            "--delete",
            "--fsync",
        ])?;

        match action {
            Action::Sync {
                src: SyncOperand::Remote(endpoint),
                dst: SyncOperand::Local(_),
                threshold,
                checksum,
                delete,
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
                assert!(delete);
                assert!(fsync);
                assert_eq!(ignores, vec!["*.tmp"]);
            }
            other => anyhow::bail!("expected Action::Sync, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_pull_ssh_endpoint_parses_bracketed_ipv6_host() -> Result<()> {
        let dir = tempdir()?;
        let dst = dir.path().join("dst");
        std::fs::create_dir_all(&dst)?;
        let dst_arg = dst.to_string_lossy().to_string();

        let action = parse_action(&["pxs", "pull", "user@[2001:db8::1]:/srv/data", &dst_arg])?;

        match action {
            Action::Sync {
                src: SyncOperand::Remote(endpoint),
                ..
            } => {
                assert_eq!(
                    endpoint,
                    RemoteEndpoint::Ssh {
                        host: "user@[2001:db8::1]".to_string(),
                        path: "/srv/data".to_string(),
                    }
                );
            }
            other => anyhow::bail!("expected Action::Sync, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_push_ssh_endpoint_defaults_empty_remote_path_to_current_directory() -> Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src.txt");
        std::fs::write(&src, "content")?;
        let src_arg = src.to_string_lossy().to_string();

        let action = parse_action(&["pxs", "push", &src_arg, "user@example:"])?;

        match action {
            Action::Sync {
                dst: SyncOperand::Remote(endpoint),
                ..
            } => {
                assert_eq!(
                    endpoint,
                    RemoteEndpoint::Ssh {
                        host: "user@example".to_string(),
                        path: ".".to_string(),
                    }
                );
            }
            other => anyhow::bail!("expected Action::Sync, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_push_ssh_endpoint_with_colons_in_path() -> Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src.txt");
        std::fs::write(&src, "content")?;
        let src_arg = src.to_string_lossy().to_string();

        let action = parse_action(&["pxs", "push", &src_arg, "user@example:path:with:colons"])?;

        match action {
            Action::Sync {
                dst: SyncOperand::Remote(endpoint),
                ..
            } => {
                assert_eq!(
                    endpoint,
                    RemoteEndpoint::Ssh {
                        host: "user@example".to_string(),
                        path: "path:with:colons".to_string(),
                    }
                );
            }
            other => anyhow::bail!("expected Action::Sync, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_pull_tcp_accepts_source_side_flags() -> Result<()> {
        let dir = tempdir()?;
        let dst = dir.path().join("dst");
        std::fs::create_dir_all(&dst)?;
        let dst_arg = dst.to_string_lossy().to_string();

        let action = parse_action(&["pxs", "pull", "127.0.0.1:9999", &dst_arg, "--checksum"])?;
        match action {
            Action::Sync {
                src: SyncOperand::Remote(RemoteEndpoint::Tcp { addr, path }),
                checksum,
                ..
            } => {
                assert_eq!(addr, "127.0.0.1:9999");
                assert!(path.is_none());
                assert!(checksum);
            }
            other => anyhow::bail!("expected Action::Sync, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn test_push_rejects_malformed_bracketed_endpoint() -> Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src.txt");
        std::fs::write(&src, "content")?;
        let src_arg = src.to_string_lossy().to_string();

        let Err(error) = parse_action(&["pxs", "push", &src_arg, "[::1"]) else {
            anyhow::bail!("malformed bracketed endpoint should be rejected");
        };

        assert!(error.to_string().contains("missing closing `]`"));
        Ok(())
    }

    #[test]
    fn test_push_rejects_ssh_endpoint_without_path() -> Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src.txt");
        std::fs::write(&src, "content")?;
        let src_arg = src.to_string_lossy().to_string();

        let Err(error) = parse_action(&["pxs", "push", &src_arg, "user@example"]) else {
            anyhow::bail!("SSH endpoint without path should be rejected");
        };

        assert!(
            error
                .to_string()
                .contains("SSH endpoint must use `HOST:PATH` syntax")
        );
        Ok(())
    }

    #[test]
    fn test_listen_and_serve_parse_expected_actions() -> Result<()> {
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
    fn test_ignore_and_exclude_from_patterns_are_merged() -> Result<()> {
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
    fn test_sync_tcp_endpoint_parses_embedded_remote_path() -> Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        std::fs::create_dir_all(&src)?;
        std::fs::create_dir_all(&dst)?;
        let src_arg = src.to_string_lossy().to_string();
        let dst_arg = dst.to_string_lossy().to_string();

        let action = parse_action(&["pxs", "sync", "backup:7878/snapshots/base", &dst_arg])?;

        match action {
            Action::Sync {
                dst: SyncOperand::Local(_),
                src: SyncOperand::Remote(RemoteEndpoint::Tcp { addr, path }),
                ..
            } => {
                assert_eq!(addr, "backup:7878");
                assert_eq!(path.as_deref(), Some("/snapshots/base"));
            }
            other => anyhow::bail!("expected TCP source endpoint, got {other:?}"),
        }

        let action = parse_action(&["pxs", "sync", &src_arg, "backup:7878/archive/out"])?;

        match action {
            Action::Sync {
                dst: SyncOperand::Remote(RemoteEndpoint::Tcp { addr, path }),
                src: SyncOperand::Local(_),
                ..
            } => {
                assert_eq!(addr, "backup:7878");
                assert_eq!(path.as_deref(), Some("/archive/out"));
            }
            other => anyhow::bail!("expected TCP destination endpoint, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_internal_stdio_sender_parses_hidden_mode() -> Result<()> {
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
            "--delete",
            "--ignore",
            "*.tmp",
        ])?;

        match action {
            Action::InternalStdioSend {
                threshold,
                checksum,
                delete,
                ignores,
                ..
            } => {
                assert_threshold(threshold, 0.8);
                assert!(checksum);
                assert!(delete);
                assert_eq!(ignores, vec!["*.tmp"]);
            }
            other => anyhow::bail!("expected Action::InternalStdioSend, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_internal_stdio_receiver_parses_hidden_ignores() -> Result<()> {
        let dir = tempdir()?;
        let dst = dir.path().join("dst");
        let dst_arg = dst.to_string_lossy().to_string();

        let action = parse_action(&[
            "pxs",
            "--stdio",
            "--destination",
            &dst_arg,
            "--ignore",
            "*.tmp",
        ])?;

        match action {
            Action::InternalStdioReceive { ignores, .. } => {
                assert_eq!(ignores, vec!["*.tmp"]);
            }
            other => anyhow::bail!("expected Action::InternalStdioReceive, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_old_flat_public_syntax_is_rejected() -> Result<()> {
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
