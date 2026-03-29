use crate::cli::actions::{Action, RemoteEndpoint, SyncOperand};
use crate::pxs::{
    net::{LargeFileParallelOptions, RemoteFeatureOptions, RemoteSyncOptions},
    sync, tools,
};
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use tracing::{debug, info, instrument};

enum HandleOutcome {
    PrintCompletion,
    SkipCompletionMessage,
}

#[derive(Clone, Copy)]
struct PushCommandOptions {
    threshold: f32,
    checksum: bool,
    delete: bool,
    fsync: bool,
    large_file_parallel_threshold: u64,
    large_file_parallel_workers: usize,
    network_file_concurrency: usize,
}

fn remote_large_file_parallel_options(
    options: PushCommandOptions,
) -> Option<LargeFileParallelOptions> {
    if options.large_file_parallel_threshold == 0 {
        return None;
    }

    Some(LargeFileParallelOptions {
        threshold_bytes: options.large_file_parallel_threshold,
        worker_count: if options.large_file_parallel_workers == 0 {
            tools::default_large_file_parallel_workers()
        } else {
            options.large_file_parallel_workers
        },
    })
}

fn remote_network_file_concurrency(options: PushCommandOptions) -> usize {
    if options.network_file_concurrency == 0 {
        tools::default_network_file_concurrency()
    } else {
        options.network_file_concurrency
    }
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
    options: PushCommandOptions,
    ignores: &[String],
) -> Result<()> {
    match endpoint {
        RemoteEndpoint::Ssh { host, path } => {
            info!("Connecting via SSH to {host} to sync to {path}");
            let large_file_parallel = remote_large_file_parallel_options(options);
            crate::pxs::net::run_ssh_sender(
                host,
                src,
                path,
                RemoteSyncOptions {
                    path: None,
                    threshold: options.threshold,
                    features: RemoteFeatureOptions {
                        checksum: options.checksum,
                        delete: options.delete,
                        fsync: options.fsync,
                    },
                    large_file_parallel,
                    network_file_concurrency: remote_network_file_concurrency(options),
                    ignores,
                },
            )
            .await?;
        }
        RemoteEndpoint::Stdio => {
            anyhow::ensure!(
                !options.fsync,
                "--fsync is not supported for `pxs push -`; use a receiver transport that can apply durability on the destination side"
            );
            anyhow::ensure!(
                !options.delete,
                "--delete is not supported for `pxs push -`; use SSH remote mirror mode instead"
            );
            crate::pxs::net::run_stdio_sender(
                src,
                options.threshold,
                options.checksum,
                false,
                ignores,
                false,
            )
            .await?;
        }
        RemoteEndpoint::Tcp { addr, path } => {
            info!(
                "Connecting to {addr} to sync {} into the remote destination (checksum: {})",
                src.display(),
                options.checksum
            );
            crate::pxs::net::run_sender_with_features(
                addr,
                src,
                RemoteSyncOptions {
                    path: path.as_deref(),
                    threshold: options.threshold,
                    features: RemoteFeatureOptions {
                        checksum: options.checksum,
                        delete: options.delete,
                        fsync: options.fsync,
                    },
                    large_file_parallel: remote_large_file_parallel_options(options),
                    network_file_concurrency: remote_network_file_concurrency(options),
                    ignores,
                },
            )
            .await?;
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
            info!("Syncing via SSH from {host}:{path} into {}", dst.display());
            crate::pxs::net::run_ssh_receiver(
                host,
                dst,
                path,
                RemoteSyncOptions {
                    path: None,
                    threshold,
                    features: RemoteFeatureOptions {
                        checksum,
                        delete,
                        fsync,
                    },
                    large_file_parallel: None,
                    network_file_concurrency: 1,
                    ignores,
                },
            )
            .await?;
        }
        RemoteEndpoint::Stdio => {
            anyhow::bail!("stdio is not supported for pull mode");
        }
        RemoteEndpoint::Tcp { addr, path } => {
            info!("Connecting to {addr} to sync into {}", dst.display());
            crate::pxs::net::run_pull_client_with_options(
                addr,
                dst,
                RemoteSyncOptions {
                    path: path.as_deref(),
                    threshold,
                    features: RemoteFeatureOptions {
                        checksum,
                        delete,
                        fsync,
                    },
                    large_file_parallel: None,
                    network_file_concurrency: 1,
                    ignores,
                },
            )
            .await?;
        }
    }

    Ok(())
}

async fn handle_listen_action(addr: &str, dst: &Path, fsync: bool, quiet: bool) -> Result<()> {
    if !quiet {
        info!("Listening on {addr} for incoming sync to {}", dst.display());
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
        info!("Serving {} on {addr} (checksum: {checksum})", src.display());
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

async fn handle_internal_chunk_write_action(
    dst: &Path,
    transfer_id: &str,
    quiet: bool,
) -> Result<()> {
    crate::pxs::net::run_stdio_chunk_writer(dst, transfer_id, quiet).await
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

async fn handle_internal_chunk_write_cli_action(
    dst: &Path,
    transfer_id: &str,
    quiet: bool,
) -> Result<HandleOutcome> {
    handle_internal_chunk_write_action(dst, transfer_id, quiet).await?;
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

async fn handle_sync_operands(
    src: &SyncOperand,
    dst: &SyncOperand,
    options: sync::SyncOptions,
    push_options: PushCommandOptions,
) -> Result<HandleOutcome> {
    match (src, dst) {
        (SyncOperand::Local(src_path), SyncOperand::Local(dst_path)) => {
            handle_local_sync(src_path, dst_path, options).await
        }
        (SyncOperand::Local(src_path), SyncOperand::Remote(endpoint)) => {
            anyhow::ensure!(
                !options.dry_run,
                "--dry-run is only supported for local-to-local sync"
            );
            handle_push_action(endpoint, src_path, push_options, options.ignores.as_slice())
                .await?;
            Ok(HandleOutcome::PrintCompletion)
        }
        (SyncOperand::Remote(endpoint), SyncOperand::Local(dst_path)) => {
            anyhow::ensure!(
                !options.dry_run,
                "--dry-run is only supported for local-to-local sync"
            );
            handle_pull_action(
                endpoint,
                dst_path,
                options.threshold,
                options.checksum,
                options.delete,
                options.fsync,
                options.ignores.as_slice(),
            )
            .await?;
            Ok(HandleOutcome::PrintCompletion)
        }
        (SyncOperand::Remote(_), SyncOperand::Remote(_)) => {
            anyhow::bail!("sync supports at most one remote operand per invocation")
        }
    }
}

/// Handle the action
///
/// # Errors
///
/// Returns an error if synchronization fails.
#[instrument(skip(action))]
pub async fn handle(action: Action) -> Result<()> {
    debug!("Dispatching action: {action:?}");
    let (is_quiet, outcome) = dispatch_action(action).await?;

    if matches!(outcome, HandleOutcome::PrintCompletion) && !is_quiet {
        info!("Synchronization complete");
    }

    Ok(())
}

async fn dispatch_action(action: Action) -> Result<(bool, HandleOutcome)> {
    match action {
        Action::Sync {
            src,
            dst,
            threshold,
            checksum,
            dry_run,
            delete,
            fsync,
            large_file_parallel_threshold,
            large_file_parallel_workers,
            network_file_concurrency,
            ignores,
            quiet,
        } => {
            dispatch_sync_variant(
                src,
                dst,
                sync::SyncOptions::new(threshold, checksum, dry_run, delete, ignores, fsync, quiet),
                PushCommandOptions {
                    threshold,
                    checksum,
                    delete,
                    fsync,
                    large_file_parallel_threshold,
                    large_file_parallel_workers,
                    network_file_concurrency,
                },
            )
            .await
        }
        other => dispatch_remote_or_internal_action(other).await,
    }
}

async fn dispatch_sync_variant(
    src: SyncOperand,
    dst: SyncOperand,
    options: sync::SyncOptions,
    push_options: PushCommandOptions,
) -> Result<(bool, HandleOutcome)> {
    Ok((
        options.quiet,
        handle_sync_operands(&src, &dst, options, push_options).await?,
    ))
}

async fn dispatch_remote_or_internal_action(action: Action) -> Result<(bool, HandleOutcome)> {
    match action {
        Action::Listen {
            addr,
            dst,
            fsync,
            quiet,
        } => dispatch_listen_variant(addr, dst, fsync, quiet).await,
        Action::Serve {
            addr,
            src,
            threshold,
            checksum,
            ignores,
            quiet,
        } => dispatch_serve_variant(addr, src, threshold, checksum, ignores, quiet).await,
        Action::InternalStdioReceive {
            dst,
            fsync,
            ignores,
            quiet,
        } => dispatch_internal_receive_variant(dst, fsync, ignores, quiet).await,
        Action::InternalStdioSend {
            src,
            threshold,
            checksum,
            delete,
            ignores,
            quiet,
        } => dispatch_internal_send_variant(src, threshold, checksum, delete, ignores, quiet).await,
        Action::InternalChunkWrite {
            dst,
            transfer_id,
            quiet,
        } => dispatch_internal_chunk_write_variant(dst, transfer_id, quiet).await,
        Action::Sync { .. } => unreachable!("sync action handled by dispatch_action"),
    }
}

async fn dispatch_listen_variant(
    addr: String,
    dst: PathBuf,
    fsync: bool,
    quiet: bool,
) -> Result<(bool, HandleOutcome)> {
    Ok((
        quiet,
        handle_listen_cli_action(&addr, &dst, fsync, quiet).await?,
    ))
}

async fn dispatch_serve_variant(
    addr: String,
    src: PathBuf,
    threshold: f32,
    checksum: bool,
    ignores: Vec<String>,
    quiet: bool,
) -> Result<(bool, HandleOutcome)> {
    Ok((
        quiet,
        handle_serve_cli_action(&addr, &src, threshold, checksum, &ignores, quiet).await?,
    ))
}

async fn dispatch_internal_receive_variant(
    dst: PathBuf,
    fsync: bool,
    ignores: Vec<String>,
    quiet: bool,
) -> Result<(bool, HandleOutcome)> {
    Ok((
        quiet,
        handle_internal_receive_cli_action(&dst, fsync, &ignores, quiet).await?,
    ))
}

async fn dispatch_internal_send_variant(
    src: PathBuf,
    threshold: f32,
    checksum: bool,
    delete: bool,
    ignores: Vec<String>,
    quiet: bool,
) -> Result<(bool, HandleOutcome)> {
    Ok((
        quiet,
        handle_internal_send_cli_action(&src, threshold, checksum, delete, &ignores, quiet).await?,
    ))
}

async fn dispatch_internal_chunk_write_variant(
    dst: PathBuf,
    transfer_id: String,
    quiet: bool,
) -> Result<(bool, HandleOutcome)> {
    Ok((
        quiet,
        handle_internal_chunk_write_cli_action(&dst, &transfer_id, quiet).await?,
    ))
}

#[cfg(test)]
mod tests {
    use super::{handle, resolve_local_file_destination};
    use crate::cli::actions::{Action, RemoteEndpoint, SyncOperand};
    use anyhow::Result;
    use std::fs;
    use tempfile::tempdir;

    fn local_sync_action(src: std::path::PathBuf, dst: std::path::PathBuf) -> Action {
        Action::Sync {
            src: SyncOperand::Local(src),
            dst: SyncOperand::Local(dst),
            threshold: 0.5,
            checksum: false,
            dry_run: false,
            delete: false,
            fsync: false,
            large_file_parallel_threshold: 0,
            large_file_parallel_workers: 0,
            network_file_concurrency: 0,
            ignores: Vec::new(),
            quiet: false,
        }
    }

    #[test]
    fn test_resolve_local_file_destination_into_existing_directory() -> Result<()> {
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
    async fn test_handle_local_sync_copies_file_into_existing_directory() -> Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("source.txt");
        let dst_dir = dir.path().join("dest");
        fs::write(&src, "payload")?;
        fs::create_dir_all(&dst_dir)?;
        fs::write(dst_dir.join("stale.txt"), "stale")?;

        handle(local_sync_action(src.clone(), dst_dir.clone())).await?;

        assert!(dst_dir.is_dir());
        assert_eq!(fs::read_to_string(dst_dir.join("source.txt"))?, "payload");
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_local_sync_replaces_file_destination_with_directory() -> Result<()> {
        let dir = tempdir()?;
        let src_dir = dir.path().join("src");
        let dst_file = dir.path().join("dest.txt");
        fs::create_dir_all(src_dir.join("nested"))?;
        fs::write(src_dir.join("nested/file.txt"), "payload")?;
        fs::write(&dst_file, "existing file")?;

        handle(local_sync_action(src_dir, dst_file.clone())).await?;

        assert!(dst_file.is_dir());
        assert_eq!(
            fs::read_to_string(dst_file.join("nested/file.txt"))?,
            "payload"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_local_sync_rejects_delete_for_single_file() -> Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("source.txt");
        let dst = dir.path().join("dest.txt");
        fs::write(&src, "payload")?;

        let error = match handle(Action::Sync {
            src: SyncOperand::Local(src),
            dst: SyncOperand::Local(dst),
            threshold: 0.5,
            checksum: false,
            dry_run: false,
            delete: true,
            fsync: false,
            large_file_parallel_threshold: 0,
            large_file_parallel_workers: 0,
            network_file_concurrency: 0,
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
    async fn test_handle_push_stdio_rejects_fsync() -> Result<()> {
        let dir = tempdir()?;
        let src = dir.path().join("source.txt");
        fs::write(&src, "payload")?;

        let error = match handle(Action::Sync {
            src: SyncOperand::Local(src),
            dst: SyncOperand::Remote(RemoteEndpoint::Stdio),
            threshold: 0.5,
            checksum: false,
            dry_run: false,
            delete: false,
            fsync: true,
            large_file_parallel_threshold: 0,
            large_file_parallel_workers: 0,
            network_file_concurrency: 0,
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
    async fn test_handle_push_stdio_rejects_delete() -> Result<()> {
        let dir = tempdir()?;
        let src_dir = dir.path().join("source");
        fs::create_dir_all(&src_dir)?;

        let error = match handle(Action::Sync {
            src: SyncOperand::Local(src_dir),
            dst: SyncOperand::Remote(RemoteEndpoint::Stdio),
            threshold: 0.5,
            checksum: false,
            dry_run: false,
            delete: true,
            fsync: false,
            large_file_parallel_threshold: 0,
            large_file_parallel_workers: 0,
            network_file_concurrency: 0,
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
    async fn test_handle_tcp_pull_allows_delete_request() -> Result<()> {
        let dir = tempdir()?;
        let dst = dir.path().join("dst");
        fs::create_dir_all(&dst)?;

        let error = match handle(Action::Sync {
            src: SyncOperand::Remote(RemoteEndpoint::Tcp {
                addr: String::from("127.0.0.1:9999"),
                path: None,
            }),
            dst: SyncOperand::Local(dst),
            threshold: 0.5,
            checksum: false,
            dry_run: false,
            delete: true,
            fsync: false,
            large_file_parallel_threshold: 0,
            large_file_parallel_workers: 0,
            network_file_concurrency: 0,
            ignores: Vec::new(),
            quiet: false,
        })
        .await
        {
            Ok(()) => anyhow::bail!("raw TCP pull should attempt the remote session"),
            Err(error) => error,
        };

        assert!(
            error.to_string().contains("connect")
                || error.to_string().contains("Connection")
                || error.to_string().contains("refused")
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_local_sync_rejects_symlinked_parent_destination() -> Result<()> {
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
            src: SyncOperand::Local(src),
            dst: SyncOperand::Local(dst_root.join("escape/payload.txt")),
            threshold: 0.5,
            checksum: false,
            dry_run: false,
            delete: false,
            fsync: false,
            large_file_parallel_threshold: 0,
            large_file_parallel_workers: 0,
            network_file_concurrency: 0,
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
