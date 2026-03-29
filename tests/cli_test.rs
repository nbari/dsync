use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use pxs::pxs::net::{self, Message};
use std::path::Path;
use std::process::{Command, Output};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

fn run_pxs(args: &[&str]) -> Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_pxs"))
        .args(args)
        .output()
        .map_err(Into::into)
}

fn run_pxs_in_dir(dir: &Path, args: &[&str]) -> Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_pxs"))
        .current_dir(dir)
        .args(args)
        .output()
        .map_err(Into::into)
}

fn stderr_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn test_pull_stdio_reports_unsupported_mode() -> Result<()> {
    let dir = tempdir()?;
    let dst_arg = dir.path().join("dst").to_string_lossy().to_string();
    let output = run_pxs(&["pull", "-", &dst_arg])?;

    assert!(!output.status.success());
    assert!(stderr_text(&output).contains("unsupported remote endpoint syntax: -"));
    Ok(())
}

#[test]
fn test_pull_tcp_source_flags_attempts_connection() -> Result<()> {
    let dir = tempdir()?;
    let dst_arg = dir.path().join("dst").to_string_lossy().to_string();
    let output = run_pxs(&["pull", "127.0.0.1:9999", &dst_arg, "--checksum"])?;

    assert!(!output.status.success());
    assert!(stderr_text(&output).contains("Failed to connect"));
    Ok(())
}

#[test]
fn test_pull_tcp_delete_attempts_connection() -> Result<()> {
    let dir = tempdir()?;
    let dst_arg = dir.path().join("dst").to_string_lossy().to_string();
    let output = run_pxs(&["pull", "127.0.0.1:9999", &dst_arg, "--delete"])?;

    assert!(!output.status.success());
    assert!(stderr_text(&output).contains("Failed to connect"));
    Ok(())
}

#[test]
fn test_push_reports_malformed_bracketed_endpoint() -> Result<()> {
    let dir = tempdir()?;
    let src = dir.path().join("src.txt");
    std::fs::write(&src, "content")?;
    let src_arg = src.to_string_lossy().to_string();
    let output = run_pxs(&["push", &src_arg, "[::1"])?;

    assert!(!output.status.success());
    assert!(stderr_text(&output).contains("missing closing `]`"));
    Ok(())
}

#[test]
fn test_push_reports_missing_source_path() -> Result<()> {
    let dir = tempdir()?;
    let src_arg = dir.path().join("missing.txt").to_string_lossy().to_string();
    let output = run_pxs(&["push", &src_arg, "127.0.0.1:9999"])?;

    assert!(!output.status.success());
    assert!(stderr_text(&output).contains("Path does not exist"));
    Ok(())
}

#[test]
fn test_push_stdio_delete_reports_actionable_error() -> Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    std::fs::create_dir_all(&src_dir)?;
    let src_arg = src_dir.to_string_lossy().to_string();
    let output = run_pxs(&["push", &src_arg, "-", "--delete"])?;

    assert!(!output.status.success());
    assert!(stderr_text(&output).contains("--delete is not supported"));
    Ok(())
}

#[test]
fn test_verbose_flag_enables_debug_output() -> Result<()> {
    let dir = tempdir()?;
    let src = dir.path().join("src.txt");
    let dst = dir.path().join("dst.txt");
    std::fs::write(&src, "content")?;
    let src_arg = src.to_string_lossy().to_string();
    let dst_arg = dst.to_string_lossy().to_string();

    let base = run_pxs(&["sync", &src_arg, &dst_arg])?;
    let verbose = run_pxs(&["-vv", "sync", &src_arg, &dst_arg])?;

    assert!(!stderr_text(&base).contains("Dispatching action:"));
    assert!(stderr_text(&verbose).contains("Dispatching action:"));
    Ok(())
}

#[test]
fn test_sync_local_file_end_to_end() -> Result<()> {
    let dir = tempdir()?;
    let src = dir.path().join("src.txt");
    let dst = dir.path().join("dst.txt");
    std::fs::write(&src, "local file payload")?;

    let src_arg = src.to_string_lossy().to_string();
    let dst_arg = dst.to_string_lossy().to_string();
    let output = run_pxs(&["sync", &src_arg, &dst_arg, "--quiet"])?;

    assert!(output.status.success(), "{}", stderr_text(&output));
    assert_eq!(std::fs::read_to_string(&dst)?, "local file payload");
    Ok(())
}

#[test]
fn test_sync_local_directory_delete_end_to_end() -> Result<()> {
    let dir = tempdir()?;
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    std::fs::create_dir_all(src.join("nested"))?;
    std::fs::create_dir_all(dst.join("stale/subdir"))?;
    std::fs::write(src.join("keep.txt"), "fresh")?;
    std::fs::write(src.join("nested/keep.txt"), "nested-fresh")?;
    std::fs::write(dst.join("keep.txt"), "stale-content")?;
    std::fs::write(dst.join("stale/subdir/old.txt"), "obsolete")?;

    let src_arg = src.to_string_lossy().to_string();
    let dst_arg = dst.to_string_lossy().to_string();
    let output = run_pxs(&["sync", &src_arg, &dst_arg, "--delete", "--quiet"])?;

    assert!(output.status.success(), "{}", stderr_text(&output));
    assert_eq!(std::fs::read_to_string(dst.join("keep.txt"))?, "fresh");
    assert_eq!(
        std::fs::read_to_string(dst.join("nested/keep.txt"))?,
        "nested-fresh"
    );
    assert!(!dst.join("stale").exists());
    Ok(())
}

#[test]
fn test_sync_local_source_first_current_directory_end_to_end() -> Result<()> {
    let dir = tempdir()?;
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    std::fs::create_dir_all(src.join("nested"))?;
    std::fs::write(src.join("root.txt"), "root-payload")?;
    std::fs::write(src.join("nested/child.txt"), "child-payload")?;
    std::fs::create_dir_all(&dst)?;

    let dst_arg = dst.to_string_lossy().to_string();
    let output = run_pxs_in_dir(&src, &["sync", ".", &dst_arg, "--quiet"])?;

    assert!(output.status.success(), "{}", stderr_text(&output));
    assert_eq!(
        std::fs::read_to_string(dst.join("root.txt"))?,
        "root-payload"
    );
    assert_eq!(
        std::fs::read_to_string(dst.join("nested/child.txt"))?,
        "child-payload"
    );
    Ok(())
}

#[tokio::test]
async fn test_push_reports_incompatible_peer_version() -> Result<()> {
    let dir = tempdir()?;
    let src = dir.path().join("src.txt");
    std::fs::write(&src, "content")?;
    let src_arg = src.to_string_lossy().to_string();

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut framed = Framed::new(stream, net::PxsCodec);
        let handshake = framed
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("missing client handshake"))??;
        match net::deserialize_message(&handshake)? {
            Message::Handshake { .. } => {}
            other => anyhow::bail!("expected handshake, got {other:?}"),
        }
        framed
            .send(net::serialize_message(&Message::Handshake {
                version: "999.0.0".to_string(),
            })?)
            .await?;
        Ok::<(), anyhow::Error>(())
    });

    let addr_arg = addr.to_string();
    let output =
        tokio::task::spawn_blocking(move || run_pxs(&["push", &src_arg, &addr_arg])).await??;
    server.await??;

    assert!(!output.status.success());
    assert!(stderr_text(&output).contains("incompatible peer version"));
    Ok(())
}
