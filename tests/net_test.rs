use dsync::dsync::net::{self, Block, DsyncCodec, FileMetadata, Message};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

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

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let dst_root = dst_dir.clone();
    let receiver_handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.map_err(|e| anyhow::anyhow!(e))?;
        let mut framed = Framed::new(stream, DsyncCodec);
        net::handle_client(&mut framed, &dst_root).await
    });

    // Run sender
    net::run_sender(&addr.to_string(), &src_dir, 0.5, true, &[]).await?;

    receiver_handle.await??;

    // Verify
    let dst_file_path = dst_dir.join("test.bin");
    assert!(dst_file_path.exists());
    let dst_content = std::fs::read(dst_file_path)?;
    assert_eq!(content, dst_content);

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
