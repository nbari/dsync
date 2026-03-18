use bytes::BytesMut;
use futures_util::{SinkExt, StreamExt};
use indicatif::ProgressBar;
use pxs::pxs::net::{self, Block, FileMetadata, Message};
use pxs::pxs::tools;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::codec::{Decoder, Encoder, Framed};

async fn spawn_receiver(
    dst_root: PathBuf,
) -> anyhow::Result<(std::net::SocketAddr, JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let receiver_handle = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let dst_root_clone = dst_root.clone();
            tokio::spawn(async move {
                let mut framed = Framed::new(stream, net::PxsCodec);
                if let Err(e) = net::handle_client(&mut framed, &dst_root_clone, false, false).await
                {
                    eprintln!("Receiver error: {e}");
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

fn make_patterned_bytes(len: usize, step: usize, offset: usize) -> anyhow::Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(len);
    for index in 0..len {
        let value = (index.wrapping_mul(step).wrapping_add(offset)) % 251;
        bytes.push(u8::try_from(value).map_err(|e| anyhow::anyhow!(e))?);
    }
    Ok(bytes)
}

#[test]
fn test_protocol_serialization() -> anyhow::Result<()> {
    let metadata = FileMetadata {
        size: 1024 * 1024 * 1024,
        mtime: 1_739_276_543,
        mtime_nsec: 0,
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    };
    let msg = Message::SyncFile {
        path: "/var/lib/postgresql/data/base/1/12345".to_string(),
        metadata,
        threshold: 0.5,
        checksum: true,
    };

    let bytes = net::serialize_message(&msg)?;
    let decoded = net::deserialize_message(&bytes)?;

    if let Message::SyncFile {
        path, metadata: m, ..
    } = decoded
    {
        assert_eq!(path, "/var/lib/postgresql/data/base/1/12345");
        assert_eq!(m.size, 1024 * 1024 * 1024);
    } else {
        anyhow::bail!("Decoded message type mismatch");
    }
    Ok(())
}

#[test]
fn test_codec_uses_pxs_magic() -> anyhow::Result<()> {
    let msg = Message::EndOfFile {
        path: String::from("test.bin"),
    };
    let encoded = net::serialize_message(&msg)?;

    let mut codec = net::PxsCodec;
    let mut frame = BytesMut::new();
    codec.encode(encoded, &mut frame)?;

    assert_eq!(frame.get(..4), Some(&b"PXS1"[..]));

    let decoded = codec
        .decode(&mut frame)?
        .ok_or_else(|| anyhow::anyhow!("missing decoded frame"))?;
    let decoded = net::deserialize_message(&decoded)?;

    match decoded {
        Message::EndOfFile { path } => assert_eq!(path, "test.bin"),
        other => anyhow::bail!("expected EndOfFile, got {other:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn test_full_network_sync_simulation() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(&src_dir)?;
    std::fs::create_dir_all(&dst_dir)?;

    // Create a 128KB file (2 blocks)
    let file_path = src_dir.join("test.bin");
    let content = (0..128 * 1024)
        .map(|i| {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let val = (i % 256) as u8;
            val
        })
        .collect::<Vec<_>>();
    std::fs::write(&file_path, &content)?;

    let (addr, receiver_handle) = spawn_receiver(dst_dir.clone()).await?;

    // Run sender
    net::run_sender(&addr.to_string(), &src_dir, 0.5, true, &[]).await?;
    stop_receiver(receiver_handle).await;

    // Verify
    let dst_file_path = dst_dir.join("test.bin");
    assert!(dst_file_path.exists());
    let dst_content = std::fs::read(dst_file_path)?;
    assert_eq!(content, dst_content);

    Ok(())
}

#[tokio::test]
async fn test_network_sync_full_copy_spans_multiple_batches() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(&src_dir)?;
    std::fs::create_dir_all(&dst_dir)?;

    let file_path = src_dir.join("large.bin");
    let content = make_patterned_bytes(18 * 1024 * 1024 + 17, 17, 3)?;
    std::fs::write(&file_path, &content)?;

    let (addr, receiver_handle) = spawn_receiver(dst_dir.clone()).await?;

    net::run_sender(&addr.to_string(), &src_dir, 0.5, false, &[]).await?;
    stop_receiver(receiver_handle).await;

    let dst_content = std::fs::read(dst_dir.join("large.bin"))?;
    assert_eq!(dst_content, content);
    Ok(())
}

#[test]
fn test_block_serialization() -> anyhow::Result<()> {
    let block = Block {
        offset: 5000,
        data: vec![1, 2, 255, 4, 5],
    };

    let msg = Message::ApplyBlocks {
        path: "test.bin".to_string(),
        blocks: vec![block],
    };

    let bytes = net::serialize_message(&msg)?;
    let decoded = net::deserialize_message(&bytes)?;

    if let Message::ApplyBlocks { blocks, .. } = decoded {
        assert_eq!(blocks.len(), 1);
        let first = blocks.first().ok_or_else(|| anyhow::anyhow!("no blocks"))?;
        assert_eq!(first.offset, 5000);
        assert_eq!(first.data, vec![1, 2, 255, 4, 5]);
    } else {
        anyhow::bail!("Block message mismatch");
    }
    Ok(())
}

#[tokio::test]
async fn test_network_sync_delta_requests_multiple_block_batches() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(&src_dir)?;
    std::fs::create_dir_all(&dst_dir)?;

    let src_file = src_dir.join("delta.bin");
    let dst_file = dst_dir.join("delta.bin");
    let src_content = make_patterned_bytes(18 * 1024 * 1024 + 17, 29, 11)?;
    let dst_content = make_patterned_bytes(src_content.len(), 31, 19)?;
    std::fs::write(&src_file, &src_content)?;
    std::fs::write(&dst_file, &dst_content)?;

    let (addr, receiver_handle) = spawn_receiver(dst_dir.clone()).await?;

    net::run_sender(&addr.to_string(), &src_dir, 0.5, false, &[]).await?;
    stop_receiver(receiver_handle).await;

    let dst_bytes = std::fs::read(&dst_file)?;
    assert_eq!(dst_bytes, src_content);
    Ok(())
}

#[tokio::test]
async fn test_network_sync_truncation() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(&src_dir)?;
    std::fs::create_dir_all(&dst_dir)?;

    let src_file = src_dir.join("test.txt");
    let dst_file = dst_dir.join("test.txt");

    std::fs::write(&src_file, "short")?;
    std::fs::write(
        &dst_file,
        "this is a longer string that should be truncated",
    )?;

    let (addr, receiver_handle) = spawn_receiver(dst_dir.clone()).await?;

    net::run_sender(&addr.to_string(), &src_dir, 0.5, true, &[]).await?;
    stop_receiver(receiver_handle).await;

    let dst_content = std::fs::read_to_string(&dst_file)?;
    assert_eq!(dst_content, "short");
    Ok(())
}

#[tokio::test]
async fn test_network_sync_delta_truncates_without_requesting_blocks() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(&src_dir)?;
    std::fs::create_dir_all(&dst_dir)?;

    let src_file = src_dir.join("test.bin");
    let dst_file = dst_dir.join("test.bin");

    let src_content = (0..(2 * 1024 * 1024))
        .map(|i| {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let val = (i % 251) as u8;
            val
        })
        .collect::<Vec<_>>();
    let mut dst_content = src_content.clone();
    dst_content.extend(std::iter::repeat_n(0xEE, 512 * 1024));

    std::fs::write(&src_file, &src_content)?;
    std::fs::write(&dst_file, &dst_content)?;

    let (addr, receiver_handle) = spawn_receiver(dst_dir.clone()).await?;

    net::run_sender(&addr.to_string(), &src_dir, 0.5, true, &[]).await?;
    stop_receiver(receiver_handle).await;

    let dst_bytes = std::fs::read(&dst_file)?;
    assert_eq!(dst_bytes.len(), src_content.len());
    assert_eq!(dst_bytes, src_content);

    Ok(())
}

#[tokio::test]
async fn test_network_sync_deadlock_skipped_files() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(&src_dir)?;
    std::fs::create_dir_all(&dst_dir)?;

    let file_path = src_dir.join("unchanged.bin");
    let content = vec![1, 2, 3, 4, 5];
    std::fs::write(&file_path, &content)?;

    std::fs::write(dst_dir.join("unchanged.bin"), &content)?;

    let src_meta = std::fs::metadata(&file_path)?;
    filetime::set_file_times(
        dst_dir.join("unchanged.bin"),
        filetime::FileTime::from_last_access_time(&src_meta),
        filetime::FileTime::from_last_modification_time(&src_meta),
    )?;

    let (addr, receiver_handle) = spawn_receiver(dst_dir.clone()).await?;

    let timeout_result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        net::run_sender(&addr.to_string(), &src_dir, 0.5, false, &[]),
    )
    .await;

    assert!(timeout_result.is_ok(), "Sync deadlocked on skipped file!");
    timeout_result??;

    stop_receiver(receiver_handle).await;

    Ok(())
}

#[tokio::test]
async fn test_network_sync_directory_mtime() -> anyhow::Result<()> {
    use std::os::unix::fs::MetadataExt;

    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(&src_dir)?;
    std::fs::create_dir_all(&dst_dir)?;

    let src_subdir = src_dir.join("subdir");
    std::fs::create_dir_all(&src_subdir)?;
    std::fs::write(src_subdir.join("file.txt"), "content")?;

    let old_time = filetime::FileTime::from_unix_time(1_000_000_000, 0);
    filetime::set_file_times(&src_subdir, old_time, old_time)?;

    let (addr, receiver_handle) = spawn_receiver(dst_dir.clone()).await?;

    net::run_sender(&addr.to_string(), &src_dir, 0.5, true, &[]).await?;
    stop_receiver(receiver_handle).await;

    let dst_subdir = dst_dir.join("subdir");
    let dst_meta = std::fs::metadata(&dst_subdir)?;

    assert_eq!(dst_meta.mtime(), 1_000_000_000);

    Ok(())
}

#[tokio::test]
async fn test_network_sync_nested_directory_mtime_order() -> anyhow::Result<()> {
    use std::os::unix::fs::MetadataExt;

    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(&src_dir)?;
    std::fs::create_dir_all(&dst_dir)?;

    let src_parent = src_dir.join("parent");
    let src_child = src_parent.join("child");
    std::fs::create_dir_all(&src_child)?;
    std::fs::write(src_child.join("file.txt"), "content")?;

    let parent_time = filetime::FileTime::from_unix_time(1_000_000_001, 0);
    let child_time = filetime::FileTime::from_unix_time(1_000_000_002, 0);
    filetime::set_file_times(&src_parent, parent_time, parent_time)?;
    filetime::set_file_times(&src_child, child_time, child_time)?;

    let (addr, receiver_handle) = spawn_receiver(dst_dir.clone()).await?;

    net::run_sender(&addr.to_string(), &src_dir, 0.5, true, &[]).await?;
    stop_receiver(receiver_handle).await;

    let dst_parent_meta = std::fs::metadata(dst_dir.join("parent"))?;
    let dst_child_meta = std::fs::metadata(dst_dir.join("parent/child"))?;

    assert_eq!(dst_parent_meta.mtime(), 1_000_000_001);
    assert_eq!(dst_child_meta.mtime(), 1_000_000_002);

    Ok(())
}

#[tokio::test]
async fn test_network_sync_file_replaces_directory() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(&src_dir)?;
    std::fs::create_dir_all(dst_dir.join("entry/nested"))?;
    std::fs::write(dst_dir.join("entry/nested/file.txt"), "stale")?;

    std::fs::write(src_dir.join("entry"), "replacement")?;

    let (addr, receiver_handle) = spawn_receiver(dst_dir.clone()).await?;

    net::run_sender(&addr.to_string(), &src_dir, 0.5, true, &[]).await?;
    stop_receiver(receiver_handle).await;

    let dst_entry = dst_dir.join("entry");
    assert!(dst_entry.is_file());
    assert_eq!(std::fs::read_to_string(&dst_entry)?, "replacement");

    Ok(())
}

#[tokio::test]
async fn test_network_sync_broken_symlink_replaces_directory() -> anyhow::Result<()> {
    use std::os::unix::fs::MetadataExt;

    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(&src_dir)?;
    std::fs::create_dir_all(dst_dir.join("link/nested"))?;
    std::fs::write(dst_dir.join("link/nested/file.txt"), "stale")?;

    let src_link = src_dir.join("link");
    let target = PathBuf::from("missing/target");
    #[cfg(unix)]
    std::os::unix::fs::symlink(&target, &src_link)?;

    let link_time = filetime::FileTime::from_unix_time(1_000_000_003, 0);
    filetime::set_symlink_file_times(&src_link, link_time, link_time)?;

    let (addr, receiver_handle) = spawn_receiver(dst_dir.clone()).await?;

    net::run_sender(&addr.to_string(), &src_dir, 0.5, true, &[]).await?;
    stop_receiver(receiver_handle).await;

    let dst_link = dst_dir.join("link");
    let dst_meta = std::fs::symlink_metadata(&dst_link)?;

    assert!(dst_meta.file_type().is_symlink());
    assert_eq!(std::fs::read_link(&dst_link)?, target);
    assert_eq!(dst_meta.mtime(), 1_000_000_003);

    Ok(())
}

#[tokio::test]
async fn test_sync_remote_file_normalizes_nested_relative_paths() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_root = dir.path().join("src");
    let nested_dir = src_root.join("dir/nested");
    std::fs::create_dir_all(&nested_dir)?;

    let file_path = nested_dir.join("file.txt");
    std::fs::write(&file_path, "content")?;

    let (client, server) = tokio::io::duplex(4096);
    let mut sender_framed = Framed::new(client, net::PxsCodec);
    let mut receiver_framed = Framed::new(server, net::PxsCodec);
    let progress = Arc::new(ProgressBar::hidden());

    let expected_path = String::from("dir/nested/file.txt");
    let expected_path_for_task = expected_path.clone();
    let src_root_for_task = src_root.clone();
    let file_path_for_task = file_path.clone();

    let sender_task = tokio::spawn(async move {
        net::sync_remote_file(
            &mut sender_framed,
            &src_root_for_task,
            &file_path_for_task,
            0.5,
            false,
            progress,
        )
        .await
    });

    let sync_msg = receiver_framed
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("missing sync message"))??;
    let sync_msg = net::deserialize_message(&sync_msg)?;
    match sync_msg {
        Message::SyncFile { path, .. } => assert_eq!(path, expected_path),
        other => anyhow::bail!("expected SyncFile, got {other:?}"),
    }

    receiver_framed
        .send(net::serialize_message(&Message::EndOfFile {
            path: expected_path_for_task.clone(),
        })?)
        .await?;

    let metadata_msg = receiver_framed
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("missing ApplyMetadata message"))??;
    let metadata_msg = net::deserialize_message(&metadata_msg)?;
    match metadata_msg {
        Message::ApplyMetadata { path, .. } => assert_eq!(path, expected_path_for_task),
        other => anyhow::bail!("expected ApplyMetadata, got {other:?}"),
    }

    receiver_framed
        .send(net::serialize_message(&Message::MetadataApplied {
            path: String::from("dir/nested/file.txt"),
        })?)
        .await?;

    sender_task.await??;

    Ok(())
}

#[tokio::test]
async fn test_sync_remote_file_rejects_mismatched_response_path() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_root = dir.path().join("src");
    std::fs::create_dir_all(&src_root)?;

    let file_path = src_root.join("file.txt");
    std::fs::write(&file_path, "content")?;

    let (client, server) = tokio::io::duplex(4096);
    let mut sender_framed = Framed::new(client, net::PxsCodec);
    let mut receiver_framed = Framed::new(server, net::PxsCodec);
    let progress = Arc::new(ProgressBar::hidden());

    let sender_task = tokio::spawn(async move {
        net::sync_remote_file(
            &mut sender_framed,
            &src_root,
            &file_path,
            0.5,
            false,
            progress,
        )
        .await
    });

    let sync_msg = receiver_framed
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("missing sync message"))??;
    let sync_msg = net::deserialize_message(&sync_msg)?;
    match sync_msg {
        Message::SyncFile { .. } => {}
        other => anyhow::bail!("expected SyncFile, got {other:?}"),
    }

    receiver_framed
        .send(net::serialize_message(&Message::RequestFullCopy {
            path: String::from("other.txt"),
        })?)
        .await?;

    let err = match sender_task.await? {
        Ok(()) => anyhow::bail!("expected mismatched path error"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("protocol path mismatch"));

    Ok(())
}

#[tokio::test]
async fn test_handle_client_rejects_unsafe_protocol_paths() -> anyhow::Result<()> {
    let metadata = FileMetadata {
        size: 0,
        mtime: 0,
        mtime_nsec: 0,
        mode: 0o755,
        uid: 0,
        gid: 0,
    };

    for path in ["../escape", "/absolute", "dir\\file"] {
        let dir = tempdir()?;
        let (client, server) = tokio::io::duplex(4096);
        let dst_root = dir.path().to_path_buf();

        let handle = tokio::spawn(async move {
            let mut framed = Framed::new(server, net::PxsCodec);
            net::handle_client(&mut framed, &dst_root, false, false).await
        });

        let mut sender = Framed::new(client, net::PxsCodec);
        sender
            .send(net::serialize_message(&Message::Handshake {
                version: env!("CARGO_PKG_VERSION").to_string(),
            })?)
            .await?;
        let _ = sender.next().await;
        sender
            .send(net::serialize_message(&Message::SyncDir {
                path: path.to_string(),
                metadata,
            })?)
            .await?;
        drop(sender);

        let err = match handle.await? {
            Ok(()) => anyhow::bail!("expected invalid path rejection"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("protocol path"));
    }

    Ok(())
}

#[tokio::test]
async fn test_run_sender_rejects_incompatible_handshake_response() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    std::fs::create_dir_all(&src_dir)?;
    std::fs::write(src_dir.join("file.txt"), "content")?;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut framed = Framed::new(stream, net::PxsCodec);
        let handshake = framed
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("missing handshake"))??;
        match net::deserialize_message(&handshake)? {
            Message::Handshake { .. } => {}
            other => anyhow::bail!("expected handshake, got {other:?}"),
        }
        framed
            .send(net::serialize_message(&Message::Handshake {
                version: String::from("9.9.0"),
            })?)
            .await?;
        Ok::<(), anyhow::Error>(())
    });

    let err = match net::run_sender(&addr.to_string(), &src_dir, 0.5, false, &[]).await {
        Ok(()) => anyhow::bail!("expected incompatible handshake error"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("incompatible peer version"));
    server_task.await??;

    Ok(())
}

#[tokio::test]
async fn test_sender_listener_refreshes_source_tree_per_client() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(&src_dir)?;
    std::fs::create_dir_all(&dst_dir)?;
    std::fs::write(src_dir.join("file.txt"), "version-one")?;

    let probe = TcpListener::bind("127.0.0.1:0").await?;
    let addr = probe.local_addr()?;
    drop(probe);

    let addr_string = addr.to_string();
    let src_dir_clone = src_dir.clone();
    let listener_task = tokio::spawn(async move {
        let ignores = Vec::new();
        let _ = net::run_sender_listener(&addr_string, &src_dir_clone, 0.5, false, &ignores).await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    net::run_pull_client(&addr.to_string(), &dst_dir, false).await?;
    assert_eq!(
        std::fs::read_to_string(dst_dir.join("file.txt"))?,
        "version-one"
    );

    std::fs::write(src_dir.join("file.txt"), "version-two-updated")?;
    std::fs::write(src_dir.join("new.txt"), "fresh-file")?;

    net::run_pull_client(&addr.to_string(), &dst_dir, false).await?;
    assert_eq!(
        std::fs::read_to_string(dst_dir.join("file.txt"))?,
        "version-two-updated"
    );
    assert_eq!(
        std::fs::read_to_string(dst_dir.join("new.txt"))?,
        "fresh-file"
    );

    listener_task.abort();
    let _ = listener_task.await;
    Ok(())
}

#[tokio::test]
async fn test_network_sync_with_checksum_verification() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(&src_dir)?;
    std::fs::create_dir_all(&dst_dir)?;

    let file_path = src_dir.join("test.bin");
    let content = (0..256 * 1024)
        .map(|i| {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let val = (i % 256) as u8;
            val
        })
        .collect::<Vec<_>>();
    std::fs::write(&file_path, &content)?;

    let (addr, receiver_handle) = spawn_receiver(dst_dir.clone()).await?;

    // Run sender with checksum=true to trigger BLAKE3 verification
    net::run_sender(&addr.to_string(), &src_dir, 0.5, true, &[]).await?;
    stop_receiver(receiver_handle).await;

    // Verify file was transferred correctly
    let dst_file_path = dst_dir.join("test.bin");
    assert!(dst_file_path.exists());
    let dst_content = std::fs::read(&dst_file_path)?;
    assert_eq!(content, dst_content);

    // Verify BLAKE3 hashes match
    let src_hash = tools::blake3_file_hash(&file_path).await?;
    let dst_hash = tools::blake3_file_hash(&dst_file_path).await?;
    assert_eq!(src_hash, dst_hash);

    Ok(())
}

#[tokio::test]
async fn test_blake3_file_hash() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let file_path = dir.path().join("test.bin");

    let content = b"hello world";
    std::fs::write(&file_path, content)?;

    let hash = tools::blake3_file_hash(&file_path).await?;

    // Verify against known BLAKE3 hash of "hello world"
    let expected = blake3::hash(content);
    assert_eq!(hash, *expected.as_bytes());

    Ok(())
}

#[tokio::test]
async fn test_partial_file_cleanup_on_error() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(&dst_dir)?;

    let (client, server) = tokio::io::duplex(4096);
    let dst_dir_clone = dst_dir.clone();

    let receiver_task = tokio::spawn(async move {
        let mut framed = Framed::new(server, net::PxsCodec);
        net::handle_client(&mut framed, &dst_dir_clone, false, false).await
    });

    let mut sender = Framed::new(client, net::PxsCodec);

    // Send handshake
    sender
        .send(net::serialize_message(&Message::Handshake {
            version: env!("CARGO_PKG_VERSION").to_string(),
        })?)
        .await?;

    // Wait for handshake response
    let _ = sender.next().await;

    // Send a file sync message
    let metadata = FileMetadata {
        size: 1024,
        mtime: 0,
        mtime_nsec: 0,
        mode: 0o644,
        uid: 0,
        gid: 0,
    };
    sender
        .send(net::serialize_message(&Message::SyncFile {
            path: String::from("partial.bin"),
            metadata,
            threshold: 0.5,
            checksum: false,
        })?)
        .await?;

    // Wait for RequestFullCopy
    let _ = sender.next().await;

    // Send partial blocks (not all data)
    sender
        .send(net::serialize_message(&Message::ApplyBlocks {
            path: String::from("partial.bin"),
            blocks: vec![Block {
                offset: 0,
                data: vec![1, 2, 3, 4],
            }],
        })?)
        .await?;

    // Drop sender to simulate connection failure before completion
    drop(sender);

    // Wait for receiver to finish (with error due to incomplete transfer)
    let _ = receiver_task.await;

    // Verify partial file was cleaned up
    let partial_file = dst_dir.join("partial.bin");
    assert!(
        !partial_file.exists(),
        "Partial file should have been removed"
    );

    Ok(())
}

#[tokio::test]
async fn test_partial_update_failure_preserves_existing_file() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(&dst_dir)?;
    let dst_file = dst_dir.join("partial.bin");
    let original = b"existing destination".to_vec();
    std::fs::write(&dst_file, &original)?;

    let (client, server) = tokio::io::duplex(4096);
    let dst_dir_clone = dst_dir.clone();
    let receiver_task = tokio::spawn(async move {
        let mut framed = Framed::new(server, net::PxsCodec);
        net::handle_client(&mut framed, &dst_dir_clone, false, false).await
    });

    let mut sender = Framed::new(client, net::PxsCodec);
    sender
        .send(net::serialize_message(&Message::Handshake {
            version: env!("CARGO_PKG_VERSION").to_string(),
        })?)
        .await?;
    let _ = sender.next().await;

    let metadata = FileMetadata {
        size: 1024,
        mtime: 0,
        mtime_nsec: 0,
        mode: 0o644,
        uid: 0,
        gid: 0,
    };
    sender
        .send(net::serialize_message(&Message::SyncFile {
            path: String::from("partial.bin"),
            metadata,
            threshold: 0.5,
            checksum: false,
        })?)
        .await?;

    let request = sender
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("missing full copy request"))??;
    match net::deserialize_message(&request)? {
        Message::RequestFullCopy { path } => assert_eq!(path, "partial.bin"),
        other => anyhow::bail!("expected RequestFullCopy, got {other:?}"),
    }

    sender
        .send(net::serialize_message(&Message::ApplyBlocks {
            path: String::from("partial.bin"),
            blocks: vec![Block {
                offset: 0,
                data: vec![9, 8, 7, 6],
            }],
        })?)
        .await?;
    drop(sender);
    let _ = receiver_task.await;

    assert_eq!(std::fs::read(&dst_file)?, original);
    Ok(())
}

#[tokio::test]
async fn test_checksum_mismatch_preserves_existing_file() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(&dst_dir)?;
    let dst_file = dst_dir.join("checksum.bin");
    let original = b"keep me".to_vec();
    std::fs::write(&dst_file, &original)?;

    let (client, server) = tokio::io::duplex(4096);
    let dst_dir_clone = dst_dir.clone();
    let receiver_task = tokio::spawn(async move {
        let mut framed = Framed::new(server, net::PxsCodec);
        net::handle_client(&mut framed, &dst_dir_clone, false, false).await
    });

    let mut sender = Framed::new(client, net::PxsCodec);
    sender
        .send(net::serialize_message(&Message::Handshake {
            version: env!("CARGO_PKG_VERSION").to_string(),
        })?)
        .await?;
    let _ = sender.next().await;

    let source_bytes = *b"fresh payload data";
    let corrupt_bytes = *b"fresh payload FAIL";
    let metadata = FileMetadata {
        size: u64::try_from(source_bytes.len()).map_err(|e| anyhow::anyhow!(e))?,
        mtime: 0,
        mtime_nsec: 0,
        mode: 0o644,
        uid: 0,
        gid: 0,
    };
    sender
        .send(net::serialize_message(&Message::SyncFile {
            path: String::from("checksum.bin"),
            metadata,
            threshold: 0.5,
            checksum: true,
        })?)
        .await?;

    let request = sender
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("missing full copy request"))??;
    match net::deserialize_message(&request)? {
        Message::RequestFullCopy { path } => assert_eq!(path, "checksum.bin"),
        other => anyhow::bail!("expected RequestFullCopy, got {other:?}"),
    }

    sender
        .send(net::serialize_message(&Message::ApplyBlocks {
            path: String::from("checksum.bin"),
            blocks: vec![Block {
                offset: 0,
                data: corrupt_bytes.to_vec(),
            }],
        })?)
        .await?;
    sender
        .send(net::serialize_message(&Message::ApplyMetadata {
            path: String::from("checksum.bin"),
            metadata,
        })?)
        .await?;

    let metadata_applied = sender
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("missing MetadataApplied"))??;
    match net::deserialize_message(&metadata_applied)? {
        Message::MetadataApplied { path } => assert_eq!(path, "checksum.bin"),
        other => anyhow::bail!("expected MetadataApplied, got {other:?}"),
    }

    sender
        .send(net::serialize_message(&Message::VerifyChecksum {
            path: String::from("checksum.bin"),
            hash: *blake3::hash(&source_bytes).as_bytes(),
        })?)
        .await?;

    let verify_response = sender
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("missing checksum response"))??;
    match net::deserialize_message(&verify_response)? {
        Message::ChecksumMismatch { path } => assert_eq!(path, "checksum.bin"),
        other => anyhow::bail!("expected ChecksumMismatch, got {other:?}"),
    }

    drop(sender);
    let result = receiver_task.await?;
    assert!(result.is_ok());
    assert_eq!(std::fs::read(&dst_file)?, original);
    Ok(())
}
