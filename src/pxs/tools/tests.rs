use super::{
    compute_requested_blocks, default_large_file_parallel_workers,
    default_network_file_concurrency, ensure_directory_path, install_prepared_path,
    probes::test_support::{DurabilityProbe, OptimizationProbe},
    staging::arm_replacement_failure_hook,
};
use crate::pxs::{
    net::{self, PxsCodec},
    sync::{self, SyncOptions},
};
use anyhow::Result;
use std::{
    path::PathBuf,
    sync::{Mutex as StdMutex, OnceLock},
    time::Duration,
};
use tempfile::tempdir;
use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle};
use tokio_util::codec::Framed;

fn optimization_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn replacement_test_lock() -> &'static StdMutex<()> {
    static LOCK: OnceLock<StdMutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| StdMutex::new(()))
}

fn durability_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

async fn spawn_receiver(
    dst_root: PathBuf,
    fsync: bool,
) -> Result<(std::net::SocketAddr, JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let receiver_handle = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let dst_root_clone = dst_root.clone();
            tokio::spawn(async move {
                let mut framed = Framed::new(stream, PxsCodec);
                if let Err(error) =
                    net::handle_client(&mut framed, &dst_root_clone, false, fsync).await
                {
                    eprintln!("Receiver error: {error}");
                }
            });
        }
    });

    Ok((addr, receiver_handle))
}

async fn stop_receiver(receiver_handle: JoinHandle<()>) {
    tokio::time::sleep(Duration::from_millis(100)).await;
    receiver_handle.abort();
}

#[tokio::test]
async fn test_network_transfer_uses_mmap_source_reads() -> Result<()> {
    let _guard = optimization_test_lock().lock().await;

    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(&src_dir)?;
    std::fs::create_dir_all(&dst_dir)?;

    let file_path = src_dir.join("large.bin");
    let content = (0..(4 * 128 * 1024))
        .map(|index| {
            u8::try_from(index % 251)
                .map_err(|error: std::num::TryFromIntError| anyhow::anyhow!(error))
        })
        .collect::<Result<Vec<_>>>()?;
    std::fs::write(&file_path, &content)?;

    let probe = OptimizationProbe::start()?;
    let (addr, receiver_handle) = spawn_receiver(dst_dir.clone(), false).await?;

    net::run_sender(&addr.to_string(), &src_dir, 0.5, false, false, &[]).await?;
    stop_receiver(receiver_handle).await;

    let snapshot = probe.snapshot();
    assert!(snapshot.safe_mmap_successes > 0);
    assert!(snapshot.sender_mmap_read_hits > 0);
    assert_eq!(std::fs::read(dst_dir.join("large.bin"))?, content);

    Ok(())
}

#[tokio::test]
async fn test_network_delta_seeding_attempts_staged_clone() -> Result<()> {
    let _guard = optimization_test_lock().lock().await;

    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(&src_dir)?;
    std::fs::create_dir_all(&dst_dir)?;

    let src_file = src_dir.join("delta.bin");
    let dst_file = dst_dir.join("delta.bin");
    let src_content = b"abcdefghZZZZmnop".to_vec();
    let dst_content = b"abcdefghYYYYmnop".to_vec();
    std::fs::write(&src_file, &src_content)?;
    std::fs::write(&dst_file, &dst_content)?;

    filetime::set_file_times(
        &dst_file,
        filetime::FileTime::from_unix_time(1_000_000_000, 0),
        filetime::FileTime::from_unix_time(1_000_000_000, 0),
    )?;

    let probe = OptimizationProbe::start()?;
    let (addr, receiver_handle) = spawn_receiver(dst_dir.clone(), false).await?;

    net::run_sender(&addr.to_string(), &src_dir, 0.5, false, false, &[]).await?;
    stop_receiver(receiver_handle).await;

    let snapshot = probe.snapshot();
    assert!(snapshot.staged_seed_invocations > 0);
    #[cfg(target_os = "linux")]
    assert!(snapshot.staged_clone_attempts > 0);
    assert!(
        snapshot.staged_clone_successes > 0 || snapshot.staged_copy_fallbacks > 0,
        "expected clone success or copy fallback while seeding staged file"
    );
    assert_eq!(std::fs::read(&dst_file)?, src_content);

    Ok(())
}

#[tokio::test]
async fn test_network_checksum_mode_skips_staging_on_match() -> Result<()> {
    let _guard = optimization_test_lock().lock().await;

    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(&src_dir)?;
    std::fs::create_dir_all(&dst_dir)?;

    let file_path = "match.bin";
    let content = b"perfect match".to_vec();
    std::fs::write(src_dir.join(file_path), &content)?;
    std::fs::write(dst_dir.join(file_path), &content)?;

    let probe = OptimizationProbe::start()?;
    let (addr, receiver_handle) = spawn_receiver(dst_dir.clone(), false).await?;

    net::run_sender(&addr.to_string(), &src_dir, 0.5, true, false, &[]).await?;
    stop_receiver(receiver_handle).await;

    let snapshot = probe.snapshot();
    assert_eq!(
        snapshot.staged_seed_invocations, 0,
        "STAGING REGRESSION: Staging file was prepared even though contents matched!"
    );

    Ok(())
}

#[tokio::test]
async fn test_local_directory_fsync_syncs_directories() -> Result<()> {
    let _guard = durability_test_lock().lock().await;
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(src_dir.join("nested"))?;

    let probe = DurabilityProbe::start()?;
    let options = SyncOptions::new(0.5, false, false, false, Vec::new(), true, true);
    sync::sync_dir(&src_dir, &dst_dir, &options).await?;

    let snapshot = probe.snapshot()?;
    assert!(
        snapshot.synced_paths.contains(&dst_dir),
        "expected fsync on destination root directory"
    );
    assert!(
        snapshot.synced_paths.contains(&dst_dir.join("nested")),
        "expected fsync on nested destination directory"
    );
    Ok(())
}

#[tokio::test]
async fn test_local_delete_fsync_syncs_deleted_entry_parents() -> Result<()> {
    let _guard = durability_test_lock().lock().await;
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(&src_dir)?;
    std::fs::create_dir_all(dst_dir.join("stale/nested"))?;
    std::fs::write(src_dir.join("keep.txt"), "keep")?;
    std::fs::write(dst_dir.join("keep.txt"), "keep")?;
    std::fs::write(dst_dir.join("stale/nested/file.txt"), "stale")?;

    let probe = DurabilityProbe::start()?;
    let options = SyncOptions::new(0.1, false, false, true, Vec::new(), true, true);
    sync::sync_dir(&src_dir, &dst_dir, &options).await?;

    let snapshot = probe.snapshot()?;
    assert!(
        snapshot
            .synced_parent_targets
            .contains(&dst_dir.join("stale/nested/file.txt"))
            || snapshot
                .synced_parent_targets
                .contains(&dst_dir.join("stale/nested"))
            || snapshot
                .synced_parent_targets
                .contains(&dst_dir.join("stale")),
        "expected delete path to sync at least one removed entry parent"
    );
    Ok(())
}

#[tokio::test]
async fn test_network_symlink_fsync_syncs_parent_directory() -> Result<()> {
    let _guard = durability_test_lock().lock().await;
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(&src_dir)?;
    std::fs::create_dir_all(&dst_dir)?;
    std::os::unix::fs::symlink("target", src_dir.join("link"))?;

    let probe = DurabilityProbe::start()?;
    let (addr, receiver_handle) = spawn_receiver(dst_dir.clone(), true).await?;

    net::run_sender(&addr.to_string(), &src_dir, 0.5, false, false, &[]).await?;
    stop_receiver(receiver_handle).await;

    let snapshot = probe.snapshot()?;
    assert!(
        snapshot
            .synced_parent_targets
            .contains(&dst_dir.join("link")),
        "expected symlink install to sync its parent directory"
    );
    Ok(())
}

#[test]
fn test_install_prepared_path_restores_original_directory_on_failure() -> Result<()> {
    let _guard = replacement_test_lock()
        .lock()
        .map_err(|_| anyhow::anyhow!("replacement test lock poisoned"))?;
    let dir = tempdir()?;
    let final_path = dir.path().join("entry");
    let prepared_path = dir.path().join("entry.tmp");

    std::fs::create_dir_all(final_path.join("nested"))?;
    std::fs::write(final_path.join("nested/file.txt"), "original")?;
    std::fs::write(&prepared_path, "replacement")?;
    arm_replacement_failure_hook()?;

    let error = match install_prepared_path(&prepared_path, &final_path) {
        Ok(()) => anyhow::bail!("replacement should fail after backup move"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("injected replacement failure"));
    assert!(final_path.is_dir());
    assert_eq!(
        std::fs::read_to_string(final_path.join("nested/file.txt"))?,
        "original"
    );
    assert!(!prepared_path.exists());
    Ok(())
}

#[test]
fn test_ensure_directory_path_restores_conflicting_file_on_failure() -> Result<()> {
    let _guard = replacement_test_lock()
        .lock()
        .map_err(|_| anyhow::anyhow!("replacement test lock poisoned"))?;
    let dir = tempdir()?;
    let path = dir.path().join("entry");

    std::fs::write(&path, "original file")?;
    arm_replacement_failure_hook()?;

    let error = match ensure_directory_path(&path) {
        Ok(()) => anyhow::bail!("directory replacement should be rolled back"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("injected replacement failure"));
    assert!(path.is_file());
    assert_eq!(std::fs::read_to_string(&path)?, "original file");
    Ok(())
}

#[test]
fn test_default_large_file_parallel_workers_is_conservative() {
    let workers = default_large_file_parallel_workers();
    assert!(
        (1..=8).contains(&workers),
        "expected conservative SSH worker default between 1 and 8, got {workers}"
    );
}

#[test]
fn test_default_network_file_concurrency_is_bounded() {
    let concurrency = default_network_file_concurrency();
    assert_eq!(concurrency, default_large_file_parallel_workers());
    assert!(
        (1..=8).contains(&concurrency),
        "expected conservative outbound network file concurrency between 1 and 8, got {concurrency}"
    );
}

#[test]
fn test_compute_requested_blocks_handles_empty_hash_list() -> Result<()> {
    let dir = tempdir()?;
    let file_path = dir.path().join("existing.bin");
    std::fs::write(&file_path, b"existing payload")?;

    let requested = compute_requested_blocks(&file_path, &[], 128 * 1024)?;
    assert!(requested.is_empty());
    Ok(())
}
