use crate::cli::actions::Action;
use crate::dsync::{sync, tools};
use anyhow::Result;
use tracing::{info, instrument};

/// Handle the action
///
/// # Errors
///
/// Returns an error if synchronization fails.
#[instrument(skip(action))]
pub async fn handle(action: Action) -> Result<()> {
    match action {
        Action::Listen { addr, dst } => {
            println!("Listening on {addr} for incoming sync to {}", dst.display());
            crate::dsync::net::run_receiver(&addr, &dst).await?;
        }
        Action::Connect {
            addr,
            src,
            checksum,
            remote_path,
            ignores,
        } => {
            if let Some(path) = remote_path {
                println!("Connecting via SSH to {addr} to sync to {path}");
                crate::dsync::net::run_ssh_sender(&addr, &src, &path, checksum, &ignores).await?;
            } else if addr == "-" {
                crate::dsync::net::run_stdio_sender(&src, checksum, &ignores).await?;
            } else {
                println!(
                    "Connecting to {addr} to sync from {} (checksum: {checksum})",
                    src.display()
                );
                crate::dsync::net::run_sender(&addr, &src, checksum, &ignores).await?;
            }
        }
        Action::Stdio { dst } => {
            crate::dsync::net::run_stdio_receiver(&dst).await?;
        }
        Action::Run {
            src,
            dst,
            threshold,
            checksum,
            dry_run,
            ignores,
        } => {
            info!(
                "src: {:?}, dst: {:?}, threshold: {:?}, checksum: {checksum}, dry_run: {dry_run}, ignores: {:?}",
                &src, &dst, threshold, ignores
            );

            let src_meta = tokio::fs::metadata(&src).await?;

            if src_meta.is_dir() {
                println!(
                    "Syncing directory from {} to {}",
                    src.display(),
                    dst.display()
                );
                sync::sync_dir(&src, &dst, threshold, checksum, dry_run, &ignores).await?;
            } else {
                if tools::should_skip_file(&src, &dst, checksum).await? {
                    println!("File {} is already up to date.", src.display());
                    return Ok(());
                }

                if dry_run {
                    let src_size = tools::get_file_size(&src).await?;
                    println!("(dry-run) sync file: {} ({src_size} bytes)", src.display());
                    return Ok(());
                }

                println!(
                    "Syncing changed blocks from {} to {}",
                    src.display(),
                    dst.display()
                );
                let stats = sync::sync_changed_blocks(&src, &dst, false).await?;

                #[allow(clippy::cast_precision_loss)]
                let percentage = (stats.updated_blocks as f64 / stats.total_blocks as f64) * 100.0;
                println!(
                    "Summary: {}/{} blocks updated ({percentage:.2}%)",
                    stats.updated_blocks, stats.total_blocks
                );
            }
        }
    }

    println!("Synchronization complete");

    Ok(())
}
