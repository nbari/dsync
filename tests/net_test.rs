use bytes::BytesMut;
use futures_util::{SinkExt, StreamExt};
use indicatif::ProgressBar;
use pxs::pxs::net::{self, Block, FileMetadata, Message};
use pxs::pxs::tools;
use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::PermissionsExt;
use std::{path::PathBuf, process::Stdio, sync::Arc, time::Duration};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio::{io::AsyncReadExt, io::AsyncWriteExt};
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

fn spawn_stdio_receiver(
    bin: &str,
    dst_root: &std::path::Path,
    receiver_args: &[&str],
) -> anyhow::Result<tokio::process::Child> {
    let mut receiver = Command::new(bin);
    receiver
        .arg("--quiet")
        .arg("--stdio")
        .arg("--destination")
        .arg(dst_root)
        .args(receiver_args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    receiver.spawn().map_err(Into::into)
}

fn spawn_stdio_sender(
    bin: &str,
    src_root: &std::path::Path,
    sender_args: &[&str],
) -> anyhow::Result<tokio::process::Child> {
    let mut sender = Command::new(bin);
    sender
        .arg("--quiet")
        .arg("--stdio")
        .arg("--sender")
        .arg("--source")
        .arg(src_root)
        .args(sender_args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    sender.spawn().map_err(Into::into)
}

fn create_parallel_transfer_record(
    dst_root: &std::path::Path,
    transfer_id: &str,
    chunk_path: &str,
    staged_path: &std::path::Path,
) -> anyhow::Result<()> {
    let record_dir = dst_root.join(".pxs-transfers").join(transfer_id);
    std::fs::create_dir_all(&record_dir)?;
    std::fs::write(record_dir.join("path"), chunk_path.as_bytes())?;
    std::os::unix::fs::symlink(staged_path, record_dir.join("staged"))?;
    Ok(())
}

fn child_framed(
    child: &mut tokio::process::Child,
) -> anyhow::Result<
    Framed<tokio::io::Join<tokio::process::ChildStdout, tokio::process::ChildStdin>, net::PxsCodec>,
> {
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("missing child stdout"))?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow::anyhow!("missing child stdin"))?;
    Ok(Framed::new(tokio::io::join(stdout, stdin), net::PxsCodec))
}

async fn wait_for_child(
    mut child: tokio::process::Child,
) -> anyhow::Result<(std::process::ExitStatus, String)> {
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow::anyhow!("missing child stderr"))?;
    let stderr_task = tokio::spawn(collect_stdio_stderr(stderr));
    let status = child.wait().await?;
    let stderr_output = String::from_utf8_lossy(&stderr_task.await??).into_owned();
    Ok((status, stderr_output))
}

async fn collect_stdio_stderr(
    stderr: tokio::process::ChildStderr,
) -> Result<Vec<u8>, std::io::Error> {
    let mut stderr = stderr;
    let mut buffer = Vec::new();
    stderr.read_to_end(&mut buffer).await?;
    Ok(buffer)
}

async fn run_stdio_sync(
    src_root: &std::path::Path,
    dst_root: &std::path::Path,
    sender_args: &[&str],
) -> anyhow::Result<()> {
    run_stdio_sync_with_receiver_args(src_root, dst_root, sender_args, &[]).await
}

async fn run_stdio_sync_with_receiver_args(
    src_root: &std::path::Path,
    dst_root: &std::path::Path,
    sender_args: &[&str],
    receiver_args: &[&str],
) -> anyhow::Result<()> {
    let bin = env!("CARGO_BIN_EXE_pxs");
    let mut receiver = spawn_stdio_receiver(bin, dst_root, receiver_args)?;
    let mut sender = spawn_stdio_sender(bin, src_root, sender_args)?;

    let mut receiver_stdin = receiver
        .stdin
        .take()
        .ok_or_else(|| anyhow::anyhow!("missing receiver stdin"))?;
    let mut receiver_stdout = receiver
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("missing receiver stdout"))?;
    let receiver_stderr = receiver
        .stderr
        .take()
        .ok_or_else(|| anyhow::anyhow!("missing receiver stderr"))?;

    let mut sender_stdin = sender
        .stdin
        .take()
        .ok_or_else(|| anyhow::anyhow!("missing sender stdin"))?;
    let mut sender_stdout = sender
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("missing sender stdout"))?;
    let sender_stderr = sender
        .stderr
        .take()
        .ok_or_else(|| anyhow::anyhow!("missing sender stderr"))?;

    let sender_to_receiver = tokio::spawn(async move {
        tokio::io::copy(&mut sender_stdout, &mut receiver_stdin).await?;
        receiver_stdin.shutdown().await?;
        Ok::<(), anyhow::Error>(())
    });
    let receiver_to_sender = tokio::spawn(async move {
        tokio::io::copy(&mut receiver_stdout, &mut sender_stdin).await?;
        sender_stdin.shutdown().await?;
        Ok::<(), anyhow::Error>(())
    });
    let sender_stderr_task = tokio::spawn(collect_stdio_stderr(sender_stderr));
    let receiver_stderr_task = tokio::spawn(collect_stdio_stderr(receiver_stderr));

    let (sender_status, receiver_status, sender_err, receiver_err) =
        tokio::time::timeout(Duration::from_secs(10), async {
            sender_to_receiver.await??;
            receiver_to_sender.await??;
            let sender_status = sender.wait().await?;
            let receiver_status = receiver.wait().await?;
            let sender_err = sender_stderr_task.await??;
            let receiver_err = receiver_stderr_task.await??;
            Ok::<_, anyhow::Error>((sender_status, receiver_status, sender_err, receiver_err))
        })
        .await
        .map_err(|_| anyhow::anyhow!("stdio sync timed out"))??;

    anyhow::ensure!(
        sender_status.success(),
        "stdio sender failed: {}",
        String::from_utf8_lossy(&sender_err)
    );
    anyhow::ensure!(
        receiver_status.success(),
        "stdio receiver failed: {}",
        String::from_utf8_lossy(&receiver_err)
    );
    anyhow::ensure!(
        sender_err.is_empty(),
        "quiet stdio sender emitted stderr: {}",
        String::from_utf8_lossy(&sender_err)
    );
    anyhow::ensure!(
        receiver_err.is_empty(),
        "quiet stdio receiver emitted stderr: {}",
        String::from_utf8_lossy(&receiver_err)
    );

    Ok(())
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

#[test]
fn test_codec_rejects_oversized_frame() -> anyhow::Result<()> {
    let mut frame = BytesMut::new();
    frame.extend_from_slice(b"PXS1");
    frame.extend_from_slice(&u32::MAX.to_be_bytes());

    let mut codec = net::PxsCodec;
    let err = match codec.decode(&mut frame) {
        Ok(Some(_)) => anyhow::bail!("expected oversized frame rejection"),
        Ok(None) => anyhow::bail!("expected oversized frame error"),
        Err(err) => err,
    };

    assert!(err.to_string().contains("Frame size too big"));
    Ok(())
}

#[test]
fn test_codec_returns_none_for_incomplete_payload() -> anyhow::Result<()> {
    let msg = Message::EndOfFile {
        path: String::from("test.bin"),
    };
    let encoded = net::serialize_message(&msg)?;

    let mut codec = net::PxsCodec;
    let mut frame = BytesMut::new();
    codec.encode(encoded, &mut frame)?;
    let truncated_len = frame.len().saturating_sub(1);
    frame.truncate(truncated_len);

    assert!(codec.decode(&mut frame)?.is_none());
    Ok(())
}

#[test]
fn test_codec_resynchronizes_after_garbage_prefix() -> anyhow::Result<()> {
    let msg = Message::EndOfFile {
        path: String::from("test.bin"),
    };
    let encoded = net::serialize_message(&msg)?;

    let mut codec = net::PxsCodec;
    let mut frame = BytesMut::from(&b"garbage"[..]);
    codec.encode(encoded, &mut frame)?;

    let decoded = codec
        .decode(&mut frame)?
        .ok_or_else(|| anyhow::anyhow!("missing decoded frame after resync"))?;
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
    net::run_sender(&addr.to_string(), &src_dir, 0.5, true, false, &[]).await?;
    stop_receiver(receiver_handle).await;

    // Verify
    let dst_file_path = dst_dir.join("test.bin");
    assert!(dst_file_path.exists());
    let dst_content = std::fs::read(dst_file_path)?;
    assert_eq!(content, dst_content);

    Ok(())
}

#[tokio::test]
async fn test_raw_tcp_lz4_transfer_succeeds() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(&src_dir)?;
    std::fs::create_dir_all(&dst_dir)?;

    let file_path = src_dir.join("compressible.bin");
    let content = vec![b'A'; 4 * 128 * 1024];
    std::fs::write(&file_path, &content)?;

    let probe = TcpListener::bind("127.0.0.1:0").await?;
    let addr = probe.local_addr()?;
    drop(probe);

    let addr_string = addr.to_string();
    let dst_dir_clone = dst_dir.clone();
    let receiver_task = tokio::spawn(async move {
        let _ = net::run_receiver(&addr_string, &dst_dir_clone, false).await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    net::run_sender(&addr.to_string(), &src_dir, 0.5, false, false, &[]).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    receiver_task.abort();
    let _ = receiver_task.await;

    assert_eq!(std::fs::read(dst_dir.join("compressible.bin"))?, content);
    Ok(())
}

#[tokio::test]
async fn test_stdio_transport_end_to_end_sync() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(src_dir.join("nested"))?;
    std::fs::create_dir_all(&dst_dir)?;

    let large_content = make_patterned_bytes(2 * 1024 * 1024 + 31, 17, 9)?;
    std::fs::write(src_dir.join("large.bin"), &large_content)?;
    std::fs::write(src_dir.join("nested/keep.txt"), "nested payload")?;
    std::fs::write(src_dir.join("ignored.tmp"), "ignored")?;

    let src_same = src_dir.join("same.bin");
    let dst_same = dst_dir.join("same.bin");
    let src_bytes = b"source payload 123";
    let dst_bytes = b"destin payload 123";
    std::fs::write(&src_same, src_bytes)?;
    std::fs::write(&dst_same, dst_bytes)?;

    let time = filetime::FileTime::from_unix_time(1_700_000_000, 0);
    filetime::set_file_times(&src_same, time, time)?;
    filetime::set_file_times(&dst_same, time, time)?;

    run_stdio_sync(
        &src_dir,
        &dst_dir,
        &["--threshold", "0.5", "--checksum", "--ignore", "*.tmp"],
    )
    .await?;

    assert_eq!(std::fs::read(dst_dir.join("large.bin"))?, large_content);
    assert_eq!(
        std::fs::read_to_string(dst_dir.join("nested/keep.txt"))?,
        "nested payload"
    );
    assert_eq!(std::fs::read(dst_dir.join("same.bin"))?, src_bytes);
    assert!(!dst_dir.join("ignored.tmp").exists());

    Ok(())
}

#[tokio::test]
async fn test_stdio_transport_delete_removes_extraneous_entries() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(src_dir.join("nested"))?;
    std::fs::create_dir_all(dst_dir.join("stale/subdir"))?;

    std::fs::write(src_dir.join("nested/keep.txt"), "payload")?;
    std::fs::write(dst_dir.join("stale.txt"), "remove me")?;
    std::fs::write(dst_dir.join("stale/subdir/file.txt"), "remove me too")?;
    std::os::unix::fs::symlink("missing-target", dst_dir.join("stale-link"))?;

    run_stdio_sync(&src_dir, &dst_dir, &["--delete"]).await?;

    assert_eq!(
        std::fs::read_to_string(dst_dir.join("nested/keep.txt"))?,
        "payload"
    );
    assert!(!dst_dir.join("stale.txt").exists());
    assert!(!dst_dir.join("stale").exists());
    assert!(!dst_dir.join("stale-link").exists());
    Ok(())
}

#[tokio::test]
async fn test_stdio_transport_delete_preserves_ignored_destination_entries() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(&src_dir)?;
    std::fs::create_dir_all(&dst_dir)?;

    std::fs::write(src_dir.join("keep.txt"), "payload")?;
    std::fs::write(dst_dir.join("ignored.tmp"), "preserve me")?;
    std::fs::write(dst_dir.join("stale.txt"), "remove me")?;

    run_stdio_sync_with_receiver_args(
        &src_dir,
        &dst_dir,
        &["--delete", "--ignore", "*.tmp"],
        &["--ignore", "*.tmp"],
    )
    .await?;

    assert_eq!(
        std::fs::read_to_string(dst_dir.join("keep.txt"))?,
        "payload"
    );
    assert_eq!(
        std::fs::read_to_string(dst_dir.join("ignored.tmp"))?,
        "preserve me"
    );
    assert!(!dst_dir.join("stale.txt").exists());
    Ok(())
}

#[tokio::test]
async fn test_stdio_receiver_requests_parallel_full_copy_and_cleans_transfer_record()
-> anyhow::Result<()> {
    let dir = tempdir()?;
    let dst_root = dir.path().join("dst");
    std::fs::create_dir_all(&dst_root)?;

    let bin = env!("CARGO_BIN_EXE_pxs");
    let mut receiver = spawn_stdio_receiver(bin, &dst_root, &[])?;
    let mut framed = child_framed(&mut receiver)?;

    framed
        .send(net::serialize_message(&Message::Handshake {
            version: format!("{}+caps=large-file-parallel", env!("CARGO_PKG_VERSION")),
        })?)
        .await?;

    let handshake = framed
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("missing receiver handshake"))??;
    match net::deserialize_message(&handshake)? {
        Message::Handshake { version } => assert!(version.contains("large-file-parallel")),
        other => anyhow::bail!("expected handshake, got {other:?}"),
    }

    framed
        .send(net::serialize_message(&Message::ParallelTransferConfig {
            threshold_bytes: 1,
            worker_count: 2,
        })?)
        .await?;
    framed
        .send(net::serialize_message(&Message::SyncFile {
            path: String::from("large.bin"),
            metadata: FileMetadata {
                size: 512 * 1024,
                mtime: 1_000_000_000,
                mtime_nsec: 0,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
            threshold: 0.5,
            checksum: false,
        })?)
        .await?;

    let request = framed
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("missing parallel full-copy request"))??;
    let transfer_id = match net::deserialize_message(&request)? {
        Message::RequestParallelFullCopy { path, transfer_id } => {
            assert_eq!(path, "large.bin");
            transfer_id
        }
        other => anyhow::bail!("expected RequestParallelFullCopy, got {other:?}"),
    };

    let transfer_dir = dst_root.join(".pxs-transfers").join(&transfer_id);
    assert!(transfer_dir.exists());

    drop(framed);
    let (status, stderr) = wait_for_child(receiver).await?;
    assert!(!status.success());
    assert!(!transfer_dir.exists());
    assert!(
        stderr.contains("incomplete transfer state")
            || stderr.contains("pending staged file")
            || stderr.contains("Connection closed"),
        "{stderr}"
    );
    Ok(())
}

#[tokio::test]
async fn test_stdio_receiver_requests_parallel_blocks_with_transfer_id() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let dst_root = dir.path().join("dst");
    std::fs::create_dir_all(&dst_root)?;
    std::fs::write(dst_root.join("delta.bin"), b"old payload")?;

    let src_file = dir.path().join("src.bin");
    std::fs::write(&src_file, b"new payload")?;
    let hashes = tools::calculate_file_hashes(&src_file, 128 * 1024).await?;

    let bin = env!("CARGO_BIN_EXE_pxs");
    let mut receiver = spawn_stdio_receiver(bin, &dst_root, &[])?;
    let mut framed = child_framed(&mut receiver)?;

    framed
        .send(net::serialize_message(&Message::Handshake {
            version: format!("{}+caps=large-file-parallel", env!("CARGO_PKG_VERSION")),
        })?)
        .await?;
    let _ = framed
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("missing receiver handshake"))??;

    framed
        .send(net::serialize_message(&Message::ParallelTransferConfig {
            threshold_bytes: 1,
            worker_count: 2,
        })?)
        .await?;
    framed
        .send(net::serialize_message(&Message::SyncFile {
            path: String::from("delta.bin"),
            metadata: FileMetadata {
                size: 11,
                mtime: 1_000_000_000,
                mtime_nsec: 0,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
            threshold: 0.0,
            checksum: true,
        })?)
        .await?;

    let request_hashes = framed
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("missing hash request"))??;
    match net::deserialize_message(&request_hashes)? {
        Message::RequestHashes { path } => assert_eq!(path, "delta.bin"),
        other => anyhow::bail!("expected RequestHashes, got {other:?}"),
    }

    framed
        .send(net::serialize_message(&Message::BlockHashes {
            path: String::from("delta.bin"),
            hashes,
        })?)
        .await?;

    let request = framed
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("missing parallel block request"))??;
    let transfer_id = match net::deserialize_message(&request)? {
        Message::RequestParallelBlocks {
            path,
            transfer_id,
            indices,
        } => {
            assert_eq!(path, "delta.bin");
            assert!(!indices.is_empty());
            transfer_id
        }
        other => anyhow::bail!("expected RequestParallelBlocks, got {other:?}"),
    };

    let transfer_dir = dst_root.join(".pxs-transfers").join(&transfer_id);
    assert!(transfer_dir.exists());

    drop(framed);
    let (status, _stderr) = wait_for_child(receiver).await?;
    assert!(!status.success());
    assert!(!transfer_dir.exists());
    Ok(())
}

#[tokio::test]
async fn test_chunk_writer_rejects_unknown_transfer_id() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let dst_root = dir.path().join("dst");
    std::fs::create_dir_all(&dst_root)?;

    let bin = env!("CARGO_BIN_EXE_pxs");
    let receiver = spawn_stdio_receiver(
        bin,
        &dst_root,
        &[
            "--chunk-writer",
            "--transfer-id",
            "deadbeef",
            "--chunk-path",
            "file.bin",
        ],
    )?;

    let (status, stderr) = wait_for_child(receiver).await?;
    assert!(!status.success());
    assert!(stderr.contains("unknown transfer id"));
    Ok(())
}

#[tokio::test]
async fn test_chunk_writer_rejects_transfer_path_mismatch() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let dst_root = dir.path().join("dst");
    std::fs::create_dir_all(&dst_root)?;
    let staged_path = dst_root.join("file.tmp");
    std::fs::write(&staged_path, [])?;
    create_parallel_transfer_record(&dst_root, "feedface", "file.bin", &staged_path)?;

    let bin = env!("CARGO_BIN_EXE_pxs");
    let receiver = spawn_stdio_receiver(
        bin,
        &dst_root,
        &[
            "--chunk-writer",
            "--transfer-id",
            "feedface",
            "--chunk-path",
            "other.bin",
        ],
    )?;

    let (status, stderr) = wait_for_child(receiver).await?;
    assert!(!status.success());
    assert!(stderr.contains("protocol path mismatch"));
    Ok(())
}

#[tokio::test]
async fn test_chunk_writer_supports_non_utf8_staged_path() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let dst_root = dir.path().join("dst");
    std::fs::create_dir_all(&dst_root)?;

    let staged_name = OsString::from_vec(b"file-\xff.tmp".to_vec());
    let staged_path = dst_root.join(staged_name);
    std::fs::write(&staged_path, [])?;
    create_parallel_transfer_record(&dst_root, "cafebabe", "file.bin", &staged_path)?;

    let bin = env!("CARGO_BIN_EXE_pxs");
    let mut receiver = spawn_stdio_receiver(
        bin,
        &dst_root,
        &[
            "--chunk-writer",
            "--transfer-id",
            "cafebabe",
            "--chunk-path",
            "file.bin",
        ],
    )?;
    let mut framed = child_framed(&mut receiver)?;

    framed
        .send(net::serialize_message(&Message::Handshake {
            version: env!("CARGO_PKG_VERSION").to_string(),
        })?)
        .await?;

    let handshake = if let Some(result) = framed.next().await {
        result?
    } else {
        drop(framed);
        let (_status, stderr) = wait_for_child(receiver).await?;
        anyhow::bail!("missing chunk writer handshake: {stderr}");
    };
    match net::deserialize_message(&handshake)? {
        Message::Handshake { .. } => {}
        other => anyhow::bail!("expected handshake, got {other:?}"),
    }

    framed
        .send(net::serialize_message(&Message::ApplyBlocks {
            path: String::from("file.bin"),
            blocks: vec![Block {
                offset: 0,
                data: b"hello".to_vec(),
            }],
        })?)
        .await?;
    framed
        .send(net::serialize_message(&Message::SyncComplete)?)
        .await?;

    let response = framed
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("missing chunk writer completion response"))??;
    match net::deserialize_message(&response)? {
        Message::SyncCompleteAck => {}
        other => anyhow::bail!("expected SyncCompleteAck, got {other:?}"),
    }

    drop(framed);
    let (status, stderr) = wait_for_child(receiver).await?;
    assert!(status.success(), "{stderr}");
    assert_eq!(std::fs::read(&staged_path)?, b"hello");
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

    net::run_sender(&addr.to_string(), &src_dir, 0.5, false, false, &[]).await?;
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

    net::run_sender(&addr.to_string(), &src_dir, 0.5, false, false, &[]).await?;
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

    net::run_sender(&addr.to_string(), &src_dir, 0.5, true, false, &[]).await?;
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

    net::run_sender(&addr.to_string(), &src_dir, 0.5, true, false, &[]).await?;
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
        net::run_sender(&addr.to_string(), &src_dir, 0.5, false, false, &[]),
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

    net::run_sender(&addr.to_string(), &src_dir, 0.5, true, false, &[]).await?;
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

    net::run_sender(&addr.to_string(), &src_dir, 0.5, true, false, &[]).await?;
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

    net::run_sender(&addr.to_string(), &src_dir, 0.5, true, false, &[]).await?;
    stop_receiver(receiver_handle).await;

    let dst_entry = dst_dir.join("entry");
    assert!(dst_entry.is_file());
    assert_eq!(std::fs::read_to_string(&dst_entry)?, "replacement");

    Ok(())
}

#[tokio::test]
async fn test_network_sync_directory_replaces_file() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(src_dir.join("entry"))?;
    std::fs::create_dir_all(&dst_dir)?;
    std::fs::write(src_dir.join("entry/file.txt"), "replacement")?;
    std::fs::write(dst_dir.join("entry"), "stale")?;

    let (addr, receiver_handle) = spawn_receiver(dst_dir.clone()).await?;

    net::run_sender(&addr.to_string(), &src_dir, 0.5, true, false, &[]).await?;
    stop_receiver(receiver_handle).await;

    let dst_entry = dst_dir.join("entry");
    assert!(dst_entry.is_dir());
    assert_eq!(
        std::fs::read_to_string(dst_entry.join("file.txt"))?,
        "replacement"
    );

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

    net::run_sender(&addr.to_string(), &src_dir, 0.5, true, false, &[]).await?;
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
async fn test_handle_client_rejects_duplicate_handshake() -> anyhow::Result<()> {
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
        .send(net::serialize_message(&Message::Handshake {
            version: env!("CARGO_PKG_VERSION").to_string(),
        })?)
        .await?;
    drop(sender);

    let err = match handle.await? {
        Ok(()) => anyhow::bail!("expected duplicate handshake rejection"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("duplicate handshake"));

    Ok(())
}

#[tokio::test]
async fn test_handle_client_rejects_receiver_side_request_message() -> anyhow::Result<()> {
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
        .send(net::serialize_message(&Message::RequestFullCopy {
            path: String::from("file.bin"),
        })?)
        .await?;
    drop(sender);

    let err = match handle.await? {
        Ok(()) => anyhow::bail!("expected invalid receiver-side request rejection"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("unexpected receiver-side protocol message")
    );

    Ok(())
}

#[tokio::test]
async fn test_handle_client_rejects_orphan_verify_checksum() -> anyhow::Result<()> {
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
        .send(net::serialize_message(&Message::VerifyChecksum {
            path: String::from("missing.bin"),
            hash: *blake3::hash(b"payload").as_bytes(),
        })?)
        .await?;
    drop(sender);

    let err = match handle.await? {
        Ok(()) => anyhow::bail!("expected orphan checksum verification rejection"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("missing active transfer state"),
        "unexpected error: {err}"
    );

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

    let err = match net::run_sender(&addr.to_string(), &src_dir, 0.5, false, false, &[]).await {
        Ok(()) => anyhow::bail!("expected incompatible handshake error"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("incompatible peer version"));
    server_task.await??;

    Ok(())
}

#[tokio::test]
async fn test_run_sender_rejects_invalid_handshake_version_format() -> anyhow::Result<()> {
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
                version: String::from("invalid-version"),
            })?)
            .await?;
        Ok::<(), anyhow::Error>(())
    });

    let err = match net::run_sender(&addr.to_string(), &src_dir, 0.5, false, false, &[]).await {
        Ok(()) => anyhow::bail!("expected invalid handshake format error"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("invalid peer version format"));
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
    net::run_sender(&addr.to_string(), &src_dir, 0.5, true, false, &[]).await?;
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

#[tokio::test]
async fn test_apply_metadata_does_not_follow_symlinks() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let (client, server) = tokio::io::duplex(4096);
    let dst_root = dir.path().to_path_buf();

    // Create a target file OUTSIDE the destination root that we want to protect
    let external_dir = tempdir()?;
    let target_file = external_dir.path().join("protected.txt");
    let original_content = b"PROTECTED CONTENT";
    std::fs::write(&target_file, original_content)?;

    // Create a symlink INSIDE the destination root pointing to the external file
    let symlink_path = dst_root.join("attacker_link");
    tokio::fs::symlink(&target_file, &symlink_path).await?;

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

    // Send ApplyMetadata targeting the symlink but with size 0 (to trigger truncation)
    let metadata = net::FileMetadata {
        size: 0,
        mtime: 1_000_000_000,
        mtime_nsec: 0,
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    };

    sender
        .send(net::serialize_message(&Message::ApplyMetadata {
            path: String::from("attacker_link"),
            metadata,
        })?)
        .await?;

    let response = sender
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("missing metadata response"))??;
    match net::deserialize_message(&response)? {
        Message::Error(msg) => assert!(msg.contains("missing active transfer state")),
        other => anyhow::bail!("expected Error, got {other:?}"),
    }

    drop(sender);
    let result = handle.await?;
    assert!(result.is_err());

    // VERIFY: The target file was NOT truncated
    let final_content = std::fs::read(&target_file)?;
    assert_eq!(
        final_content, original_content,
        "SYMLINK TRAVERSAL DETECTED: Target file was modified!"
    );

    Ok(())
}

#[tokio::test]
async fn test_sync_file_rejects_parent_symlink_traversal() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let (client, server) = tokio::io::duplex(4096);
    let dst_root = dir.path().to_path_buf();
    let external_dir = tempdir()?;
    let protected_path = external_dir.path().join("escaped.txt");
    std::fs::write(&protected_path, "protected")?;
    std::os::unix::fs::symlink(external_dir.path(), dst_root.join("escape"))?;

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
        .send(net::serialize_message(&Message::SyncFile {
            path: String::from("escape/escaped.txt"),
            metadata: FileMetadata {
                size: 7,
                mtime: 1_000_000_000,
                mtime_nsec: 0,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
            threshold: 0.5,
            checksum: false,
        })?)
        .await?;

    let response = sender
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("missing error response"))??;
    match net::deserialize_message(&response)? {
        Message::Error(message) => assert!(message.contains("symlinked parent")),
        other => anyhow::bail!("expected Error, got {other:?}"),
    }

    drop(sender);
    let result = handle.await?;
    assert!(result.is_err());
    assert_eq!(std::fs::read_to_string(&protected_path)?, "protected");
    Ok(())
}

#[tokio::test]
async fn test_sync_file_rejects_symlinked_destination_root() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let (client, server) = tokio::io::duplex(4096);
    let external_dir = tempdir()?;
    let dst_root = dir.path().join("linked-root");
    let protected_path = external_dir.path().join("escaped-root.txt");
    std::fs::write(&protected_path, "protected")?;
    std::os::unix::fs::symlink(external_dir.path(), &dst_root)?;

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
        .send(net::serialize_message(&Message::SyncFile {
            path: String::from("escaped-root.txt"),
            metadata: FileMetadata {
                size: 7,
                mtime: 1_000_000_000,
                mtime_nsec: 0,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
            threshold: 0.5,
            checksum: false,
        })?)
        .await?;

    let response = sender
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("missing error response"))??;
    match net::deserialize_message(&response)? {
        Message::Error(message) => {
            assert!(message.contains("destination root must not be a symlink"));
        }
        other => anyhow::bail!("expected Error, got {other:?}"),
    }

    drop(sender);
    let result = handle.await?;
    assert!(result.is_err());
    assert_eq!(std::fs::read_to_string(&protected_path)?, "protected");
    Ok(())
}

#[tokio::test]
async fn test_sync_replaces_broken_symlink_at_destination() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let (client, server) = tokio::io::duplex(4096);
    let dst_root = dir.path().to_path_buf();

    // Create a broken symlink at the destination path
    let target = std::path::PathBuf::from("missing_target");
    let full_path = dst_root.join("entry");
    tokio::fs::symlink(&target, &full_path).await?;

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

    // Sync a new file at the same location as the broken symlink
    let content = b"new data";
    let metadata = net::FileMetadata {
        size: content.len() as u64,
        mtime: 1_000_000_000,
        mtime_nsec: 0,
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    };

    sender
        .send(net::serialize_message(&Message::SyncFile {
            path: String::from("entry"),
            metadata,
            threshold: 0.5,
            checksum: false,
        })?)
        .await?;

    // Expect RequestFullCopy (because it's replacing a non-file)
    let req = sender
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("missing full copy request"))??;
    match net::deserialize_message(&req)? {
        Message::RequestFullCopy { path } => assert_eq!(path, "entry"),
        other => anyhow::bail!("expected RequestFullCopy, got {other:?}"),
    }

    sender
        .send(net::serialize_message(&Message::ApplyBlocks {
            path: String::from("entry"),
            blocks: vec![net::Block {
                offset: 0,
                data: content.to_vec(),
            }],
        })?)
        .await?;

    sender
        .send(net::serialize_message(&Message::ApplyMetadata {
            path: String::from("entry"),
            metadata,
        })?)
        .await?;
    let _ = sender.next().await; // MetadataApplied

    drop(sender);
    handle.await??;

    // VERIFY: The broken symlink was replaced by the file
    assert!(full_path.is_file());
    assert_eq!(std::fs::read(&full_path)?, content);

    Ok(())
}

#[tokio::test]
async fn test_sync_replaces_broken_symlink_with_new_symlink() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let (client, server) = tokio::io::duplex(4096);
    let dst_root = dir.path().to_path_buf();

    // Create a broken symlink at the destination path
    let broken_target = std::path::PathBuf::from("missing_target");
    let full_path = dst_root.join("entry_link");
    tokio::fs::symlink(&broken_target, &full_path).await?;

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

    // Sync a new symlink at the same location as the broken symlink
    let new_target = String::from("new_valid_target");
    let metadata = net::FileMetadata {
        size: 0,
        mtime: 1_000_000_000,
        mtime_nsec: 0,
        mode: 0o777,
        uid: 1000,
        gid: 1000,
    };

    sender
        .send(net::serialize_message(&Message::SyncSymlink {
            path: String::from("entry_link"),
            target: new_target.clone(),
            metadata,
        })?)
        .await?;

    drop(sender);
    handle.await??;

    // VERIFY: The broken symlink was replaced by the new symlink
    assert!(full_path.is_symlink());
    assert_eq!(
        tokio::fs::read_link(&full_path).await?,
        PathBuf::from(new_target)
    );

    Ok(())
}

#[tokio::test]
async fn test_receiver_reports_permission_denied_error_to_sender() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let (client, server) = tokio::io::duplex(4096);
    let dst_root = dir.path().to_path_buf();

    // Make the destination root read-only to trigger a PermissionDenied error
    let mut perms = std::fs::metadata(&dst_root)?.permissions();
    perms.set_mode(0o555); // rx
    std::fs::set_permissions(&dst_root, perms)?;

    let handle = tokio::spawn(async move {
        let mut framed = Framed::new(server, net::PxsCodec);
        // Error will occur inside handle_client_inner which is wrapped by handle_client_with_transport
        net::handle_client(&mut framed, &dst_root, false, false).await
    });

    let mut sender = Framed::new(client, net::PxsCodec);
    sender
        .send(net::serialize_message(&Message::Handshake {
            version: env!("CARGO_PKG_VERSION").to_string(),
        })?)
        .await?;
    let _ = sender.next().await;

    // Try to sync a file which should fail due to permissions
    let metadata = net::FileMetadata {
        size: 100,
        mtime: 1_000_000_000,
        mtime_nsec: 0,
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    };

    sender
        .send(net::serialize_message(&Message::SyncFile {
            path: String::from("blocked.bin"),
            metadata,
            threshold: 0.5,
            checksum: false,
        })?)
        .await?;

    // Wait for the error response
    let response = sender
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("missing error response"))??;

    match net::deserialize_message(&response)? {
        Message::Error(msg) => {
            // Check that the error message is meaningful (Permission denied)
            assert!(
                msg.to_lowercase().contains("permission denied"),
                "unexpected error message: {msg}"
            );
        }
        other => anyhow::bail!("expected Error message, got {other:?}"),
    }

    drop(sender);
    // The receiver should have returned Err
    let result = handle.await?;
    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_run_sender_forwards_fsync_session_option_for_tcp_push() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    std::fs::create_dir_all(src_dir.join("nested"))?;

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
                version: env!("CARGO_PKG_VERSION").to_string(),
            })?)
            .await?;

        let session_options = framed
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("missing session options"))??;
        match net::deserialize_message(&session_options)? {
            Message::SessionOptions { fsync, delete } => {
                assert!(fsync);
                assert!(!delete);
            }
            other => anyhow::bail!("expected session options, got {other:?}"),
        }

        let sync_dir = framed
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("missing directory task"))??;
        match net::deserialize_message(&sync_dir)? {
            Message::SyncDir { path, .. } => assert_eq!(path, "nested"),
            other => anyhow::bail!("expected SyncDir, got {other:?}"),
        }

        let completion = framed
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("missing completion message"))??;
        match net::deserialize_message(&completion)? {
            Message::SyncComplete => {}
            other => anyhow::bail!("expected SyncComplete, got {other:?}"),
        }

        framed
            .send(net::serialize_message(&Message::SyncCompleteAck)?)
            .await?;
        Ok::<(), anyhow::Error>(())
    });

    net::run_sender(&addr.to_string(), &src_dir, 0.5, false, true, &[]).await?;
    server_task.await??;
    Ok(())
}

#[tokio::test]
async fn test_run_sender_reports_non_file_control_connection_error() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    std::fs::create_dir_all(src_dir.join("blocked"))?;
    std::fs::create_dir_all(&dst_dir)?;

    let mut perms = std::fs::metadata(&dst_dir)?.permissions();
    perms.set_mode(0o555);
    std::fs::set_permissions(&dst_dir, perms)?;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let dst_root = dst_dir.clone();
    let receiver_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut framed = Framed::new(stream, net::PxsCodec);
        net::handle_client(&mut framed, &dst_root, false, false).await
    });

    let error = match net::run_sender(&addr.to_string(), &src_dir, 0.5, false, false, &[]).await {
        Ok(()) => anyhow::bail!("expected non-file control connection failure"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("Remote error on control connection")
    );

    let receiver_result = receiver_task.await?;
    assert!(receiver_result.is_err());
    Ok(())
}

#[tokio::test]
async fn test_handle_client_rejects_premature_sync_complete() -> anyhow::Result<()> {
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
        .send(net::serialize_message(&Message::SyncFile {
            path: String::from("incomplete.bin"),
            metadata: FileMetadata {
                size: 4,
                mtime: 1_000_000_000,
                mtime_nsec: 0,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
            threshold: 0.5,
            checksum: false,
        })?)
        .await?;

    let request = sender
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("missing full-copy request"))??;
    match net::deserialize_message(&request)? {
        Message::RequestFullCopy { path } => assert_eq!(path, "incomplete.bin"),
        other => anyhow::bail!("expected RequestFullCopy, got {other:?}"),
    }

    sender
        .send(net::serialize_message(&Message::SyncComplete)?)
        .await?;

    let response = sender
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("missing completion error"))??;
    match net::deserialize_message(&response)? {
        Message::Error(message) => assert!(message.contains("pending staged file")),
        other => anyhow::bail!("expected Error, got {other:?}"),
    }

    drop(sender);
    let result = handle.await?;
    assert!(result.is_err());
    assert!(!dir.path().join("incomplete.bin").exists());
    Ok(())
}

#[tokio::test]
async fn test_handle_client_recovers_if_destination_disappears_after_request_hashes()
-> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_file = dir.path().join("src.bin");
    let dst_root = dir.path().join("dst");
    std::fs::create_dir_all(&dst_root)?;

    let source_bytes = b"new payload after hash negotiation".to_vec();
    let dst_file = dst_root.join("race.bin");
    std::fs::write(&src_file, &source_bytes)?;
    std::fs::write(&dst_file, b"old payload before file removal")?;

    let hashes = tools::calculate_file_hashes(&src_file, 128 * 1024).await?;
    let metadata = net::FileMetadata {
        size: u64::try_from(source_bytes.len()).map_err(|e| anyhow::anyhow!(e))?,
        mtime: 1_000_000_000,
        mtime_nsec: 0,
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    };

    let (client, server) = tokio::io::duplex(4096);
    let receiver_root = dst_root.clone();
    let handle = tokio::spawn(async move {
        let mut framed = Framed::new(server, net::PxsCodec);
        net::handle_client(&mut framed, &receiver_root, false, false).await
    });

    let mut sender = Framed::new(client, net::PxsCodec);
    sender
        .send(net::serialize_message(&Message::Handshake {
            version: env!("CARGO_PKG_VERSION").to_string(),
        })?)
        .await?;
    let _ = sender.next().await;

    sender
        .send(net::serialize_message(&Message::SyncFile {
            path: String::from("race.bin"),
            metadata,
            threshold: 0.5,
            checksum: true,
        })?)
        .await?;

    let request_hashes = sender
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("missing hash request"))??;
    match net::deserialize_message(&request_hashes)? {
        Message::RequestHashes { path } => assert_eq!(path, "race.bin"),
        other => anyhow::bail!("expected RequestHashes, got {other:?}"),
    }

    std::fs::remove_file(&dst_file)?;

    sender
        .send(net::serialize_message(&Message::BlockHashes {
            path: String::from("race.bin"),
            hashes,
        })?)
        .await?;

    let follow_up = sender
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("missing follow-up request"))??;
    match net::deserialize_message(&follow_up)? {
        Message::RequestFullCopy { path } => assert_eq!(path, "race.bin"),
        other => anyhow::bail!("expected RequestFullCopy fallback, got {other:?}"),
    }

    drop(sender);
    let receiver_result = handle.await?;
    assert!(receiver_result.is_err());
    Ok(())
}
