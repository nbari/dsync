use futures_util::{SinkExt, StreamExt};
use pxs::pxs::net::{self, Message};
use std::process::{Command, Output};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

fn run_pxs(args: &[&str]) -> anyhow::Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_pxs"))
        .args(args)
        .output()
        .map_err(Into::into)
}

fn stderr_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn test_pull_stdio_reports_unsupported_mode() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let dst_arg = dir.path().join("dst").to_string_lossy().to_string();
    let output = run_pxs(&["pull", "-", &dst_arg])?;

    assert!(!output.status.success());
    assert!(stderr_text(&output).contains("stdio is not supported for pull mode"));
    Ok(())
}

#[test]
fn test_pull_tcp_source_flags_reports_actionable_error() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let dst_arg = dir.path().join("dst").to_string_lossy().to_string();
    let output = run_pxs(&["pull", "127.0.0.1:9999", &dst_arg, "--checksum"])?;

    assert!(!output.status.success());
    assert!(stderr_text(&output).contains("configure `pxs serve` instead"));
    Ok(())
}

#[test]
fn test_pull_tcp_delete_reports_actionable_error() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let dst_arg = dir.path().join("dst").to_string_lossy().to_string();
    let output = run_pxs(&["pull", "127.0.0.1:9999", &dst_arg, "--delete"])?;

    assert!(!output.status.success());
    assert!(stderr_text(&output).contains("--delete is not supported"));
    Ok(())
}

#[test]
fn test_push_reports_malformed_bracketed_endpoint() -> anyhow::Result<()> {
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
fn test_push_reports_missing_source_path() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_arg = dir.path().join("missing.txt").to_string_lossy().to_string();
    let output = run_pxs(&["push", &src_arg, "127.0.0.1:9999"])?;

    assert!(!output.status.success());
    assert!(stderr_text(&output).contains("Path does not exist"));
    Ok(())
}

#[test]
fn test_push_stdio_delete_reports_actionable_error() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    std::fs::create_dir_all(&src_dir)?;
    let src_arg = src_dir.to_string_lossy().to_string();
    let output = run_pxs(&["push", &src_arg, "-", "--delete"])?;

    assert!(!output.status.success());
    assert!(stderr_text(&output).contains("--delete is not supported"));
    Ok(())
}

#[tokio::test]
async fn test_push_reports_incompatible_peer_version() -> anyhow::Result<()> {
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
