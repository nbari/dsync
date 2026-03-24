use crate::cli::actions::{Action, RemoteEndpoint};
use crate::pxs::{
    net::{RemoteFeatureOptions, RemoteSyncOptions},
    sync, tools,
};
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use tracing::{info, instrument};

enum HandleOutcome {
    PrintCompletion,
    SkipCompletionMessage,
}

fn format_updated_block_percentage(stats: sync::SyncStats) -> String {
    if stats.total_blocks == 0 {
        return String::from("0.00");
    }

    let updated_blocks = u128::from(u64::try_from(stats.updated_blocks).unwrap_or(u64::MAX));
    let total_blocks = u128::from(u64::try_from(stats.total_blocks).unwrap_or(u64::MAX));
    let basis_points = (updated_blocks * 10_000 + (total_blocks / 2)) / total_blocks;
    let whole = basis_points / 100;
    let fractional = basis_points % 100;
    format!("{whole}.{fractional:02}")
}

/// Resolve the effective local destination path for a single-file sync.
///
/// When the requested destination already exists as a directory, local sync
/// should mirror standard copy semantics and place the source file inside that
/// directory instead of replacing it.
fn resolve_local_file_destination(src: &Path, dst: &Path) -> Result<PathBuf> {
    if dst.is_dir() {
        let file_name = src
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("source file has no name: {}", src.display()))?;
        return Ok(dst.join(file_name));
    }

    Ok(dst.to_path_buf())
}

async fn handle_push_action(
    endpoint: &RemoteEndpoint,
    src: &Path,
    threshold: f32,
    checksum: bool,
    delete: bool,
    fsync: bool,
    ignores: &[String],
) -> Result<()> {
    match endpoint {
        RemoteEndpoint::Ssh { host, path } => {
            eprintln!("Connecting via SSH to {host} to sync to {path}");
            crate::pxs::net::run_ssh_sender(
                host,
                src,
                path,
                RemoteSyncOptions {
                    threshold,
                    features: RemoteFeatureOptions {
                        checksum,
                        delete,
                        fsync,
                    },
                    ignores,
                },
            )
            .await?;
        }
        RemoteEndpoint::Stdio => {
            anyhow::ensure!(
                !fsync,
                "--fsync is not supported for `pxs push -`; use a receiver transport that can apply durability on the destination side"
            );
            anyhow::ensure!(
                !delete,
                "--delete is not supported for `pxs push -`; use SSH remote mirror mode instead"
            );
            crate::pxs::net::run_stdio_sender(src, threshold, checksum, false, ignores, false)
                .await?;
        }
        RemoteEndpoint::Tcp(addr) => {
            anyhow::ensure!(
                !delete,
                "--delete is not supported for raw TCP push; use SSH remote mirror mode instead"
            );
            eprintln!(
                "Connecting to {addr} to sync from {} (checksum: {checksum})",
                src.display()
            );
            crate::pxs::net::run_sender(addr, src, threshold, checksum, fsync, ignores).await?;
        }
    }

    Ok(())
}

async fn handle_pull_action(
    endpoint: &RemoteEndpoint,
    dst: &Path,
    threshold: f32,
    checksum: bool,
    delete: bool,
    fsync: bool,
    ignores: &[String],
) -> Result<()> {
    match endpoint {
        RemoteEndpoint::Ssh { host, path } => {
            eprintln!("Pulling via SSH from {host}:{path} to {}", dst.display());
            crate::pxs::net::run_ssh_receiver(
                host,
                dst,
                path,
                RemoteSyncOptions {
                    threshold,
                    features: RemoteFeatureOptions {
                        checksum,
                        delete,
                        fsync,
                    },
                    ignores,
                },
            )
            .await?;
        }
        RemoteEndpoint::Stdio => {
            anyhow::bail!("stdio is not supported for pull mode");
        }
        RemoteEndpoint::Tcp(addr) => {
            anyhow::ensure!(
                !delete,
                "--delete is not supported for raw TCP pull; use SSH remote mirror mode instead"
            );
            eprintln!("Connecting to {addr} to pull to {}", dst.display());
            crate::pxs::net::run_pull_client(addr, dst, fsync).await?;
        }
    }

    Ok(())
}

async fn handle_listen_action(addr: &str, dst: &Path, fsync: bool, quiet: bool) -> Result<()> {
    if !quiet {
        eprintln!("Listening on {addr} for incoming sync to {}", dst.display());
    }
    crate::pxs::net::run_receiver(addr, dst, fsync).await
}

async fn handle_serve_action(
    addr: &str,
    src: &Path,
    threshold: f32,
    checksum: bool,
    ignores: &[String],
    quiet: bool,
) -> Result<()> {
    if !quiet {
        eprintln!("Serving {} on {addr} (checksum: {checksum})", src.display());
    }
    crate::pxs::net::run_sender_listener(addr, src, threshold, checksum, ignores).await
}

async fn handle_internal_stdio_receive_action(
    dst: &Path,
    fsync: bool,
    ignores: &[String],
    quiet: bool,
) -> Result<()> {
    crate::pxs::net::run_stdio_receiver(dst, fsync, ignores, quiet).await
}

async fn handle_internal_stdio_send_action(
    src: &Path,
    threshold: f32,
    checksum: bool,
    delete: bool,
    ignores: &[String],
    quiet: bool,
) -> Result<()> {
    crate::pxs::net::run_stdio_sender(src, threshold, checksum, delete, ignores, quiet).await
}

async fn handle_push_cli_action(
    endpoint: &RemoteEndpoint,
    src: &Path,
    threshold: f32,
    checksum: bool,
    delete: bool,
    fsync: bool,
    ignores: &[String],
) -> Result<HandleOutcome> {
    handle_push_action(endpoint, src, threshold, checksum, delete, fsync, ignores).await?;
    Ok(HandleOutcome::PrintCompletion)
}

async fn handle_pull_cli_action(
    endpoint: &RemoteEndpoint,
    dst: &Path,
    threshold: f32,
    checksum: bool,
    delete: bool,
    fsync: bool,
    ignores: &[String],
) -> Result<HandleOutcome> {
    handle_pull_action(endpoint, dst, threshold, checksum, delete, fsync, ignores).await?;
    Ok(HandleOutcome::PrintCompletion)
}

async fn handle_listen_cli_action(
    addr: &str,
    dst: &Path,
    fsync: bool,
    quiet: bool,
) -> Result<HandleOutcome> {
    handle_listen_action(addr, dst, fsync, quiet).await?;
    Ok(HandleOutcome::PrintCompletion)
}

async fn handle_serve_cli_action(
    addr: &str,
    src: &Path,
    threshold: f32,
    checksum: bool,
    ignores: &[String],
    quiet: bool,
) -> Result<HandleOutcome> {
    handle_serve_action(addr, src, threshold, checksum, ignores, quiet).await?;
    Ok(HandleOutcome::PrintCompletion)
}

async fn handle_internal_receive_cli_action(
    dst: &Path,
    fsync: bool,
    ignores: &[String],
    quiet: bool,
) -> Result<HandleOutcome> {
    handle_internal_stdio_receive_action(dst, fsync, ignores, quiet).await?;
    Ok(HandleOutcome::PrintCompletion)
}

async fn handle_internal_send_cli_action(
    src: &Path,
    threshold: f32,
    checksum: bool,
    delete: bool,
    ignores: &[String],
    quiet: bool,
) -> Result<HandleOutcome> {
    handle_internal_stdio_send_action(src, threshold, checksum, delete, ignores, quiet).await?;
    Ok(HandleOutcome::PrintCompletion)
}

async fn handle_local_sync(
    src: &Path,
    dst: &Path,
    options: sync::SyncOptions,
) -> Result<HandleOutcome> {
    if !options.quiet {
        info!(
            "src: {:?}, dst: {:?}, threshold: {:?}, checksum: {}, dry_run: {}, delete: {}, fsync: {}, ignores: {:?}",
            &src,
            &dst,
            options.threshold,
            options.checksum,
            options.dry_run,
            options.delete,
            options.fsync,
            options.ignores
        );
    }

    let src_meta = tokio::fs::metadata(src)
        .await
        .with_context(|| format!("failed to read source metadata for `{}`", src.display()))?;

    if src_meta.is_dir() {
        if !options.quiet {
            eprintln!(
                "Syncing directory from {} to {}",
                src.display(),
                dst.display()
            );
        }
        let stats = sync::sync_dir(src, dst, &options).await?;
        if !options.quiet {
            let percentage = format_updated_block_percentage(stats);
            eprintln!(
                "Summary: {}/{} blocks updated ({percentage}%)",
                stats.updated_blocks, stats.total_blocks
            );
        }
        return Ok(HandleOutcome::PrintCompletion);
    }

    anyhow::ensure!(
        !options.delete,
        "--delete is only supported when syncing directories"
    );

    let dst = resolve_local_file_destination(src, dst)?;
    tools::ensure_no_symlink_ancestors(&dst)?;

    if tools::should_skip_file(src, &dst, options.checksum).await? {
        if !options.quiet {
            eprintln!("File {} is already up to date.", src.display());
        }
        return Ok(HandleOutcome::SkipCompletionMessage);
    }

    if options.dry_run {
        if !options.quiet {
            let src_size = tools::get_file_size(src).await?;
            eprintln!("(dry-run) sync file: {} ({src_size} bytes)", src.display());
        }
        return Ok(HandleOutcome::SkipCompletionMessage);
    }

    if !options.quiet {
        eprintln!(
            "Syncing changed blocks from {} to {}",
            src.display(),
            dst.display()
        );
    }
    let full_copy = tools::should_use_full_copy(src, &dst, options.threshold).await?;
    let stats =
        sync::sync_changed_blocks(src, &dst, full_copy, options.fsync, options.quiet).await?;

    if !options.quiet {
        let percentage = format_updated_block_percentage(stats);
        eprintln!(
            "Summary: {}/{} blocks updated ({percentage}%)",
            stats.updated_blocks, stats.total_blocks
        );
    }

    Ok(HandleOutcome::PrintCompletion)
}

/// Handle the action
///
/// # Errors
///
/// Returns an error if synchronization fails.
#[instrument(skip(action))]
pub async fn handle(action: Action) -> Result<()> {
    let is_quiet: bool;
    let outcome = match action {
        Action::Sync {
            src,
            dst,
            threshold,
            checksum,
            dry_run,
            delete,
            fsync,
            ignores,
            quiet,
        } => {
            is_quiet = quiet;
            let options =
                sync::SyncOptions::new(threshold, checksum, dry_run, delete, ignores, fsync, quiet);
            handle_local_sync(&src, &dst, options).await?
        }
        Action::Push {
            endpoint,
            src,
            threshold,
            checksum,
            delete,
            fsync,
            ignores,
            quiet,
        } => {
            is_quiet = quiet;
            handle_push_cli_action(
                &endpoint, &src, threshold, checksum, delete, fsync, &ignores,
            )
            .await?
        }
        Action::Pull {
            endpoint,
            dst,
            threshold,
            checksum,
            delete,
            fsync,
            ignores,
            quiet,
        } => {
            is_quiet = quiet;
            handle_pull_cli_action(
                &endpoint, &dst, threshold, checksum, delete, fsync, &ignores,
            )
            .await?
        }
        Action::Listen {
            addr,
            dst,
            fsync,
            quiet,
        } => {
            is_quiet = quiet;
            handle_listen_cli_action(&addr, &dst, fsync, quiet).await?
        }
        Action::Serve {
            addr,
            src,
            threshold,
            checksum,
            ignores,
            quiet,
        } => {
            is_quiet = quiet;
            handle_serve_cli_action(&addr, &src, threshold, checksum, &ignores, quiet).await?
        }
        Action::InternalStdioReceive {
            dst,
            fsync,
            ignores,
            quiet,
        } => {
            is_quiet = quiet;
            handle_internal_receive_cli_action(&dst, fsync, &ignores, quiet).await?
        }
        Action::InternalStdioSend {
            src,
            threshold,
            checksum,
            delete,
            ignores,
            quiet,
        } => {
            is_quiet = quiet;
            handle_internal_send_cli_action(&src, threshold, checksum, delete, &ignores, quiet)
                .await?
        }
    };

    if matches!(outcome, HandleOutcome::PrintCompletion) && !is_quiet {
        eprintln!("Synchronization complete");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{handle, resolve_local_file_destination};
    use crate::cli::actions::Action;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_resolve_local_file_destination_into_existing_directory() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("source.bin");
        let dst_dir = dir.path().join("dest");
        fs::write(&src, "content")?;
        fs::create_dir_all(&dst_dir)?;

        let resolved = resolve_local_file_destination(&src, &dst_dir)?;

        assert_eq!(resolved, dst_dir.join("source.bin"));
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_local_sync_copies_file_into_existing_directory() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("source.txt");
        let dst_dir = dir.path().join("dest");
        fs::write(&src, "payload")?;
        fs::create_dir_all(&dst_dir)?;
        fs::write(dst_dir.join("stale.txt"), "stale")?;

        handle(Action::Sync {
            src: src.clone(),
            dst: dst_dir.clone(),
            threshold: 0.5,
            checksum: false,
            dry_run: false,
            delete: false,
            fsync: false,
            ignores: Vec::new(),
            quiet: false,
        })
        .await?;

        assert!(dst_dir.is_dir());
        assert_eq!(fs::read_to_string(dst_dir.join("source.txt"))?, "payload");
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_local_sync_replaces_file_destination_with_directory() -> anyhow::Result<()>
    {
        let dir = tempdir()?;
        let src_dir = dir.path().join("src");
        let dst_file = dir.path().join("dest.txt");
        fs::create_dir_all(src_dir.join("nested"))?;
        fs::write(src_dir.join("nested/file.txt"), "payload")?;
        fs::write(&dst_file, "existing file")?;

        handle(Action::Sync {
            src: src_dir,
            dst: dst_file.clone(),
            threshold: 0.5,
            checksum: false,
            dry_run: false,
            delete: false,
            fsync: false,
            ignores: Vec::new(),
            quiet: false,
        })
        .await?;

        assert!(dst_file.is_dir());
        assert_eq!(
            fs::read_to_string(dst_file.join("nested/file.txt"))?,
            "payload"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_local_sync_rejects_delete_for_single_file() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("source.txt");
        let dst = dir.path().join("dest.txt");
        fs::write(&src, "payload")?;

        let error = match handle(Action::Sync {
            src,
            dst,
            threshold: 0.5,
            checksum: false,
            dry_run: false,
            delete: true,
            fsync: false,
            ignores: Vec::new(),
            quiet: false,
        })
        .await
        {
            Ok(()) => anyhow::bail!("single-file sync should reject --delete"),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("--delete is only supported when syncing directories")
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_push_stdio_rejects_fsync() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("source.txt");
        fs::write(&src, "payload")?;

        let error = match handle(Action::Push {
            endpoint: crate::cli::actions::RemoteEndpoint::Stdio,
            src,
            threshold: 0.5,
            checksum: false,
            delete: false,
            fsync: true,
            ignores: Vec::new(),
            quiet: false,
        })
        .await
        {
            Ok(()) => anyhow::bail!("stdio push should reject --fsync"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("--fsync is not supported"));
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_push_stdio_rejects_delete() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let src_dir = dir.path().join("source");
        fs::create_dir_all(&src_dir)?;

        let error = match handle(Action::Push {
            endpoint: crate::cli::actions::RemoteEndpoint::Stdio,
            src: src_dir,
            threshold: 0.5,
            checksum: false,
            delete: true,
            fsync: false,
            ignores: Vec::new(),
            quiet: false,
        })
        .await
        {
            Ok(()) => anyhow::bail!("stdio push should reject --delete"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("--delete is not supported"));
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_tcp_pull_rejects_delete() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let dst = dir.path().join("dst");
        fs::create_dir_all(&dst)?;

        let error = match handle(Action::Pull {
            endpoint: crate::cli::actions::RemoteEndpoint::Tcp(String::from("127.0.0.1:9999")),
            dst,
            threshold: 0.5,
            checksum: false,
            delete: true,
            fsync: false,
            ignores: Vec::new(),
            quiet: false,
        })
        .await
        {
            Ok(()) => anyhow::bail!("raw TCP pull should reject --delete"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("--delete is not supported"));
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_local_sync_rejects_symlinked_parent_destination() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("source.txt");
        let dst_root = dir.path().join("dest");
        let external_dir = tempdir()?;
        let protected_path = external_dir.path().join("payload.txt");
        fs::write(&src, "new payload")?;
        fs::create_dir_all(&dst_root)?;
        fs::write(&protected_path, "protected")?;
        std::os::unix::fs::symlink(external_dir.path(), dst_root.join("escape"))?;

        let error = match handle(Action::Sync {
            src,
            dst: dst_root.join("escape/payload.txt"),
            threshold: 0.5,
            checksum: false,
            dry_run: false,
            delete: false,
            fsync: false,
            ignores: Vec::new(),
            quiet: false,
        })
        .await
        {
            Ok(()) => anyhow::bail!("local sync should reject symlinked parent destinations"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("symlinked parent"));
        assert_eq!(fs::read_to_string(&protected_path)?, "protected");
        Ok(())
    }
}
