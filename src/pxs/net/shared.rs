use super::{
    BLOCK_SIZE, IDLE_TIMEOUT_SECS,
    protocol::{Block, FileMetadata},
};
use futures_util::StreamExt;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

const MAX_RETRIES: u32 = 3;
const INITIAL_BACKOFF_MS: u64 = 500;
const PROTOCOL_MAJOR: &str = env!("CARGO_PKG_VERSION_MAJOR");
const PROTOCOL_MINOR: &str = env!("CARGO_PKG_VERSION_MINOR");

pub(crate) async fn connect_with_retry(addr: &str) -> anyhow::Result<TcpStream> {
    let mut last_error = None;

    for attempt in 0..MAX_RETRIES {
        match TcpStream::connect(addr).await {
            Ok(stream) => return Ok(stream),
            Err(e) => {
                last_error = Some(e);
                if attempt + 1 < MAX_RETRIES {
                    let backoff = Duration::from_millis(INITIAL_BACKOFF_MS << attempt);
                    eprintln!(
                        "Connection to {addr} failed, retrying in {}ms... (attempt {}/{})",
                        backoff.as_millis(),
                        attempt + 1,
                        MAX_RETRIES
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }

    Err(last_error.map_or_else(
        || anyhow::anyhow!("Failed to connect to {addr}"),
        |e| anyhow::anyhow!("Failed to connect to {addr} after {MAX_RETRIES} attempts: {e}"),
    ))
}

fn parse_protocol_version(version: &str) -> Option<(&str, &str)> {
    let mut parts = version.split('.');
    let major = parts.next()?;
    let minor = parts.next()?;
    Some((major, minor))
}

/// Validate that a peer speaks the same protocol generation as the local binary.
///
/// The wire format is tied to the crate version for now, so pre-1.0 releases
/// require a matching major/minor pair.
pub(crate) fn validate_peer_version(version: &str) -> anyhow::Result<()> {
    let Some((major, minor)) = parse_protocol_version(version) else {
        anyhow::bail!("invalid peer version format: {version}");
    };

    if major == PROTOCOL_MAJOR && minor == PROTOCOL_MINOR {
        return Ok(());
    }

    anyhow::bail!(
        "incompatible peer version: local {PROTOCOL_MAJOR}.{PROTOCOL_MINOR} vs remote {version}"
    )
}

/// Read next frame with idle timeout.
pub(crate) async fn recv_with_timeout<T, C>(
    framed: &mut Framed<T, C>,
) -> anyhow::Result<Option<Vec<u8>>>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    C: tokio_util::codec::Decoder<Item = Vec<u8>, Error = anyhow::Error>,
{
    let timeout = Duration::from_secs(IDLE_TIMEOUT_SECS);
    match tokio::time::timeout(timeout, framed.next()).await {
        Ok(Some(Ok(bytes))) => Ok(Some(bytes)),
        Ok(Some(Err(e))) => Err(e),
        Ok(None) => Ok(None),
        Err(_) => Err(anyhow::anyhow!(
            "Connection idle timeout ({IDLE_TIMEOUT_SECS}s)"
        )),
    }
}

pub(crate) fn skipped_bytes(metadata: FileMetadata, requested: &[u32]) -> u64 {
    let mut requested_bytes = 0_u64;
    for &idx in requested {
        let offset = u64::from(idx) * BLOCK_SIZE;
        let bytes = std::cmp::min(BLOCK_SIZE, metadata.size.saturating_sub(offset));
        requested_bytes += bytes;
    }

    metadata.size.saturating_sub(requested_bytes)
}

pub(crate) fn block_bytes(blocks: &[Block]) -> anyhow::Result<u64> {
    blocks.iter().try_fold(0_u64, |total, block| {
        let block_len = u64::try_from(block.data.len()).map_err(|e| anyhow::anyhow!(e))?;
        total
            .checked_add(block_len)
            .ok_or_else(|| anyhow::anyhow!("block byte count overflow"))
    })
}
