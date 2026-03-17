use crate::cli::actions::Action;
use crate::pxs::{sync, tools};
use anyhow::Result;
use std::path::Path;
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

async fn handle_connect_action(
    addr: &str,
    src: &Path,
    threshold: f32,
    checksum: bool,
    remote_path: Option<&str>,
    ignores: &[String],
) -> Result<()> {
    if let Some(path) = remote_path {
        eprintln!("Connecting via SSH to {addr} to sync to {path}");
        crate::pxs::net::run_ssh_sender(addr, src, path, threshold, checksum, ignores).await?;
    } else if addr == "-" {
        crate::pxs::net::run_stdio_sender(src, threshold, checksum, ignores).await?;
    } else {
        eprintln!(
            "Connecting to {addr} to sync from {} (checksum: {checksum})",
            src.display()
        );
        crate::pxs::net::run_sender(addr, src, threshold, checksum, ignores).await?;
    }

    Ok(())
}

async fn handle_pull_action(
    addr: &str,
    dst: &Path,
    threshold: f32,
    checksum: bool,
    fsync: bool,
    remote_path: Option<&str>,
    ignores: &[String],
) -> Result<()> {
    if let Some(path) = remote_path {
        eprintln!("Pulling via SSH from {addr}:{path} to {}", dst.display());
        crate::pxs::net::run_ssh_receiver(addr, dst, path, threshold, checksum, fsync, ignores)
            .await?;
    } else {
        eprintln!("Connecting to {addr} to pull to {}", dst.display());
        crate::pxs::net::run_pull_client(addr, dst, fsync).await?;
    }

    Ok(())
}

async fn handle_local_run(
    src: &Path,
    dst: &Path,
    threshold: f32,
    checksum: bool,
    dry_run: bool,
    fsync: bool,
    ignores: &[String],
) -> Result<HandleOutcome> {
    info!(
        "src: {:?}, dst: {:?}, threshold: {:?}, checksum: {checksum}, dry_run: {dry_run}, fsync: {fsync}, ignores: {:?}",
        &src, &dst, threshold, ignores
    );

    let src_meta = tokio::fs::metadata(src).await?;

    if src_meta.is_dir() {
        eprintln!(
            "Syncing directory from {} to {}",
            src.display(),
            dst.display()
        );
        sync::sync_dir(src, dst, threshold, checksum, dry_run, ignores, fsync).await?;
        return Ok(HandleOutcome::PrintCompletion);
    }

    if tools::should_skip_file(src, dst, checksum).await? {
        eprintln!("File {} is already up to date.", src.display());
        return Ok(HandleOutcome::SkipCompletionMessage);
    }

    if dry_run {
        let src_size = tools::get_file_size(src).await?;
        eprintln!("(dry-run) sync file: {} ({src_size} bytes)", src.display());
        return Ok(HandleOutcome::SkipCompletionMessage);
    }

    eprintln!(
        "Syncing changed blocks from {} to {}",
        src.display(),
        dst.display()
    );
    let full_copy = tools::should_use_full_copy(src, dst, threshold).await?;
    let stats = sync::sync_changed_blocks(src, dst, full_copy, fsync).await?;
    let percentage = format_updated_block_percentage(stats);
    eprintln!(
        "Summary: {}/{} blocks updated ({percentage}%)",
        stats.updated_blocks, stats.total_blocks
    );

    Ok(HandleOutcome::PrintCompletion)
}

/// Handle the action
///
/// # Errors
///
/// Returns an error if synchronization fails.
#[instrument(skip(action))]
pub async fn handle(action: Action) -> Result<()> {
    let outcome = match action {
        Action::Listen { addr, dst, fsync } => {
            eprintln!("Listening on {addr} for incoming sync to {}", dst.display());
            crate::pxs::net::run_receiver(&addr, &dst, fsync).await?;
            HandleOutcome::PrintCompletion
        }
        Action::ListenSender {
            addr,
            src,
            threshold,
            checksum,
            ignores,
        } => {
            eprintln!(
                "Listening on {addr} to serve {} (checksum: {checksum})",
                src.display()
            );
            crate::pxs::net::run_sender_listener(&addr, &src, threshold, checksum, &ignores)
                .await?;
            HandleOutcome::PrintCompletion
        }
        Action::Connect {
            addr,
            src,
            threshold,
            checksum,
            remote_path,
            ignores,
        } => {
            handle_connect_action(
                &addr,
                &src,
                threshold,
                checksum,
                remote_path.as_deref(),
                &ignores,
            )
            .await?;
            HandleOutcome::PrintCompletion
        }
        Action::Pull {
            addr,
            dst,
            threshold,
            checksum,
            fsync,
            remote_path,
            ignores,
        } => {
            handle_pull_action(
                &addr,
                &dst,
                threshold,
                checksum,
                fsync,
                remote_path.as_deref(),
                &ignores,
            )
            .await?;
            HandleOutcome::PrintCompletion
        }

        Action::Stdio { dst, fsync } => {
            crate::pxs::net::run_stdio_receiver(&dst, fsync).await?;
            HandleOutcome::PrintCompletion
        }
        Action::StdioSender {
            src,
            threshold,
            checksum,
            ignores,
        } => {
            crate::pxs::net::run_stdio_sender(&src, threshold, checksum, &ignores).await?;
            HandleOutcome::PrintCompletion
        }
        Action::Run {
            src,
            dst,
            threshold,
            checksum,
            dry_run,
            fsync,
            ignores,
        } => handle_local_run(&src, &dst, threshold, checksum, dry_run, fsync, &ignores).await?,
    };

    if matches!(outcome, HandleOutcome::PrintCompletion) {
        eprintln!("Synchronization complete");
    }

    Ok(())
}
