use super::codec::PxsCodec;
use anyhow::Result;
use std::process::Stdio;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio_util::codec::Framed;

type FramedChildIo = tokio::io::Join<ChildStdout, ChildStdin>;

/// Quote a remote shell argument using single-quote escaping.
fn shell_quote(arg: &str) -> String {
    format!("'{}'", arg.replace('\'', "'\"'\"'"))
}

fn build_remote_command(args: &[String]) -> String {
    args.iter()
        .map(|arg| shell_quote(arg))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Build the remote pxs command used for SSH push mode.
#[must_use]
pub(crate) fn build_ssh_push_command(dst_path: &str, fsync: bool, ignores: &[String]) -> String {
    let mut args = vec![
        "pxs".to_string(),
        "--stdio".to_string(),
        "--quiet".to_string(),
        "--destination".to_string(),
        dst_path.to_string(),
    ];
    if fsync {
        args.push("--fsync".to_string());
    }
    for pattern in ignores {
        args.push("--ignore".to_string());
        args.push(pattern.clone());
    }
    build_remote_command(&args)
}

/// Build the remote pxs command used for SSH chunk-writer worker mode.
#[must_use]
pub(crate) fn build_ssh_chunk_writer_command(dst_path: &str, transfer_id: &str) -> String {
    let args = vec![
        "pxs".to_string(),
        "--stdio".to_string(),
        "--quiet".to_string(),
        "--destination".to_string(),
        dst_path.to_string(),
        "--chunk-writer".to_string(),
        "--transfer-id".to_string(),
        transfer_id.to_string(),
    ];
    build_remote_command(&args)
}

/// Build the remote pxs command used for SSH pull mode.
#[must_use]
pub(crate) fn build_ssh_pull_command(
    src_path: &str,
    threshold: f32,
    checksum: bool,
    delete: bool,
    ignores: &[String],
) -> String {
    let mut args = vec![
        "pxs".to_string(),
        "--stdio".to_string(),
        "--quiet".to_string(),
        "--sender".to_string(),
        "--source".to_string(),
        src_path.to_string(),
        "--threshold".to_string(),
        threshold.to_string(),
    ];
    if checksum {
        args.push("--checksum".to_string());
    }
    if delete {
        args.push("--delete".to_string());
    }
    for pattern in ignores {
        args.push("--ignore".to_string());
        args.push(pattern.clone());
    }
    build_remote_command(&args)
}

/// Build the SSH command used for framed pxs transport.
#[must_use]
pub(crate) fn build_ssh_command(addr: &str, remote_cmd: &str) -> Command {
    let mut cmd = Command::new("ssh");
    cmd.arg("-T")
        .arg("-q")
        .arg("-o")
        .arg("Compression=no")
        .arg("-o")
        .arg("Ciphers=aes128-gcm@openssh.com,chacha20-poly1305@openssh.com,aes128-ctr")
        .arg("-o")
        .arg("IPQoS=throughput")
        .arg(addr)
        .arg(remote_cmd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped());
    cmd
}

pub(crate) struct ChildSession {
    child: Child,
    framed: Option<Framed<FramedChildIo, PxsCodec>>,
}

impl ChildSession {
    pub(crate) fn spawn(mut cmd: Command) -> Result<Self> {
        let mut child = cmd.spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow::anyhow!("stdin failed"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("stdout failed"))?;
        let framed = Framed::new(tokio::io::join(stdout, stdin), PxsCodec);
        Ok(Self {
            child,
            framed: Some(framed),
        })
    }

    pub(crate) fn framed_mut(&mut self) -> Result<&mut Framed<FramedChildIo, PxsCodec>> {
        self.framed
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("framed session already finalized"))
    }

    pub(crate) async fn finish(
        mut self,
        session_result: Result<()>,
        process_label: &str,
    ) -> Result<()> {
        drop(self.framed.take());

        if session_result.is_err() {
            let _ = self.child.start_kill();
        }

        let status = match self.child.wait().await {
            Ok(status) => status,
            Err(wait_error) => {
                return match session_result {
                    Ok(()) => Err(wait_error.into()),
                    Err(err) => Err(err.context(format!(
                        "failed to wait for {process_label} after session error: {wait_error}"
                    ))),
                };
            }
        };

        match session_result {
            Ok(()) => {
                if !status.success() {
                    anyhow::bail!("{process_label} exited with error: {status}");
                }
                Ok(())
            }
            Err(err) => Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ChildSession, build_ssh_chunk_writer_command, build_ssh_command, build_ssh_pull_command,
        build_ssh_push_command, shell_quote,
    };
    use anyhow::Result;
    use std::time::{Duration, Instant};
    use tokio::process::Command;

    #[test]
    fn test_shell_quote_escapes_single_quotes() {
        assert_eq!(shell_quote("a'b c"), "'a'\"'\"'b c'");
    }

    #[test]
    fn test_build_ssh_push_command_quotes_destination_and_ignores() {
        let command = build_ssh_push_command(
            "/tmp/dst path/it's here",
            false,
            &[String::from("*.tmp"), String::from("quote'pattern")],
        );
        assert_eq!(
            command,
            "'pxs' '--stdio' '--quiet' '--destination' '/tmp/dst path/it'\"'\"'s here' '--ignore' '*.tmp' '--ignore' 'quote'\"'\"'pattern'"
        );
    }

    #[test]
    fn test_build_ssh_pull_command_quotes_source_and_preserves_checksum() {
        let command = build_ssh_pull_command(
            "/tmp/src path/it's here",
            0.5,
            true,
            false,
            &[String::from("space name")],
        );
        assert_eq!(
            command,
            "'pxs' '--stdio' '--quiet' '--sender' '--source' '/tmp/src path/it'\"'\"'s here' '--threshold' '0.5' '--checksum' '--ignore' 'space name'"
        );
    }

    #[test]
    fn test_build_ssh_chunk_writer_command_quotes_transfer() {
        let command = build_ssh_chunk_writer_command("/tmp/dst path", "deadbeef-42");
        assert_eq!(
            command,
            "'pxs' '--stdio' '--quiet' '--destination' '/tmp/dst path' '--chunk-writer' '--transfer-id' 'deadbeef-42'"
        );
    }

    #[test]
    fn test_build_ssh_command_sets_expected_transport_options() {
        let cmd = build_ssh_command("user@example", "'pxs' '--stdio' '--quiet'");
        let args = cmd
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        assert_eq!(
            args,
            vec![
                "-T",
                "-q",
                "-o",
                "Compression=no",
                "-o",
                "Ciphers=aes128-gcm@openssh.com,chacha20-poly1305@openssh.com,aes128-ctr",
                "-o",
                "IPQoS=throughput",
                "user@example",
                "'pxs' '--stdio' '--quiet'",
            ]
        );
    }

    #[tokio::test]
    async fn test_child_session_finish_kills_process_on_error() -> Result<()> {
        let mut cmd = Command::new("sh");
        cmd.arg("-c")
            .arg("sleep 60")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped());

        let session = ChildSession::spawn(cmd)?;
        let start = Instant::now();
        let err = match session
            .finish(Err(anyhow::anyhow!("session failed")), "test process")
            .await
        {
            Ok(()) => anyhow::bail!("expected session error"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("session failed"));
        assert!(start.elapsed() < Duration::from_secs(2));
        Ok(())
    }

    #[tokio::test]
    async fn test_child_session_finish_surfaces_nonzero_exit() -> Result<()> {
        let mut cmd = Command::new("sh");
        cmd.arg("-c")
            .arg("exit 7")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped());

        let session = ChildSession::spawn(cmd)?;
        let err = match session.finish(Ok(()), "test process").await {
            Ok(()) => anyhow::bail!("expected non-zero exit error"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("test process exited with error"));
        Ok(())
    }

    #[test]
    fn test_build_ssh_push_command_includes_fsync() {
        let command = build_ssh_push_command("/dst", true, &[]);
        assert!(
            command.contains("'--fsync'"),
            "SSH PUSH REGRESSION: --fsync was not forwarded!"
        );
    }

    #[test]
    fn test_build_ssh_pull_command_includes_delete() {
        let command = build_ssh_pull_command("/src", 0.5, false, true, &[]);
        assert!(
            command.contains("'--delete'"),
            "SSH PULL REGRESSION: --delete was not forwarded!"
        );
    }
}
