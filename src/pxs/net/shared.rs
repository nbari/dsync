use super::{
    BLOCK_SIZE, IDLE_TIMEOUT_SECS,
    protocol::{Block, FileMetadata},
};
use anyhow::Result;
use futures_util::StreamExt;
use std::{io::Cursor, time::Duration};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

const MAX_RETRIES: u32 = 3;
const INITIAL_BACKOFF_MS: u64 = 500;
const PROTOCOL_MAJOR: &str = env!("CARGO_PKG_VERSION_MAJOR");
const PROTOCOL_MINOR: &str = env!("CARGO_PKG_VERSION_MINOR");
const CAPABILITIES_PREFIX: &str = "caps=";
const LZ4_BLOCKS_CAPABILITY: &str = "lz4-blocks";
const ZSTD_BLOCKS_CAPABILITY: &str = "zstd-blocks";
const LARGE_FILE_PARALLEL_CAPABILITY: &str = "large-file-parallel";
const ZSTD_COMPRESSION_LEVEL: i32 = 1;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum BlockCompression {
    #[default]
    None,
    Lz4,
    Zstd,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct TransportFeatures {
    pub(crate) block_compression: BlockCompression,
    pub(crate) large_file_parallel: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct PeerCapabilities {
    lz4_block_messages: bool,
    zstd_block_messages: bool,
    large_file_parallel: bool,
}

pub(crate) async fn connect_with_retry(addr: &str) -> Result<TcpStream> {
    let mut last_error = None;

    for attempt in 0..MAX_RETRIES {
        match TcpStream::connect(addr).await {
            Ok(stream) => return Ok(stream),
            Err(e) => {
                last_error = Some(e);
                if attempt + 1 < MAX_RETRIES {
                    let backoff = Duration::from_millis(INITIAL_BACKOFF_MS << attempt);
                    tracing::warn!(
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
pub(crate) fn validate_peer_version(version: &str) -> Result<()> {
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

fn parse_peer_capabilities(version: &str) -> PeerCapabilities {
    let Some((_, metadata)) = version.split_once('+') else {
        return PeerCapabilities::default();
    };
    let Some(raw_capabilities) = metadata.strip_prefix(CAPABILITIES_PREFIX) else {
        return PeerCapabilities::default();
    };

    let mut features = PeerCapabilities::default();
    for capability in raw_capabilities.split(',') {
        if capability == LZ4_BLOCKS_CAPABILITY {
            features.lz4_block_messages = true;
        } else if capability == ZSTD_BLOCKS_CAPABILITY {
            features.zstd_block_messages = true;
        } else if capability == LARGE_FILE_PARALLEL_CAPABILITY {
            features.large_file_parallel = true;
        }
    }

    features
}

pub(crate) fn local_handshake_version(
    advertise_block_compression: bool,
    advertise_large_file_parallel: bool,
) -> String {
    let mut capabilities = Vec::new();
    if advertise_block_compression {
        capabilities.push(ZSTD_BLOCKS_CAPABILITY);
        capabilities.push(LZ4_BLOCKS_CAPABILITY);
    }
    if advertise_large_file_parallel {
        capabilities.push(LARGE_FILE_PARALLEL_CAPABILITY);
    }
    if !capabilities.is_empty() {
        return format!(
            "{}+{CAPABILITIES_PREFIX}{}",
            env!("CARGO_PKG_VERSION"),
            capabilities.join(",")
        );
    }

    env!("CARGO_PKG_VERSION").to_string()
}

pub(crate) fn negotiate_transport_features(
    peer_version: &str,
    allow_block_compression: bool,
    allow_large_file_parallel: bool,
) -> Result<TransportFeatures> {
    validate_peer_version(peer_version)?;
    let peer_features = parse_peer_capabilities(peer_version);
    Ok(TransportFeatures {
        block_compression: if allow_block_compression {
            if peer_features.zstd_block_messages {
                BlockCompression::Zstd
            } else if peer_features.lz4_block_messages {
                BlockCompression::Lz4
            } else {
                BlockCompression::None
            }
        } else {
            BlockCompression::None
        },
        large_file_parallel: allow_large_file_parallel && peer_features.large_file_parallel,
    })
}

/// Compress a serialized block batch for transport if the chosen codec is enabled.
///
/// # Errors
///
/// Returns an error if compression fails.
pub(crate) fn compress_block_batch_payload(
    payload: &[u8],
    codec: BlockCompression,
) -> Result<Option<Vec<u8>>> {
    let compressed = match codec {
        BlockCompression::None => return Ok(None),
        BlockCompression::Lz4 => lz4_flex::compress_prepend_size(payload),
        BlockCompression::Zstd => zstd::bulk::compress(payload, ZSTD_COMPRESSION_LEVEL)?,
    };
    if compressed.len() < payload.len() {
        Ok(Some(compressed))
    } else {
        Ok(None)
    }
}

/// Decompress a serialized block batch payload using the negotiated transport codec.
///
/// # Errors
///
/// Returns an error if no compression codec was negotiated or decompression fails.
pub(crate) fn decompress_block_batch_payload(
    payload: &[u8],
    codec: BlockCompression,
) -> Result<Vec<u8>> {
    match codec {
        BlockCompression::None => {
            anyhow::bail!("received compressed block batch without negotiated compression support")
        }
        BlockCompression::Lz4 => lz4_flex::decompress_size_prepended(payload)
            .map_err(|error| anyhow::anyhow!("failed to decompress lz4 block batch: {error}")),
        BlockCompression::Zstd => zstd::stream::decode_all(Cursor::new(payload))
            .map_err(|error| anyhow::anyhow!("failed to decompress zstd block batch: {error}")),
    }
}

/// Read next frame with idle timeout.
pub(crate) async fn recv_with_timeout<T, C>(framed: &mut Framed<T, C>) -> Result<Option<Vec<u8>>>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    C: tokio_util::codec::Decoder<Item = Vec<u8>, Error = anyhow::Error>,
{
    recv_with_timeout_for(framed, Duration::from_secs(IDLE_TIMEOUT_SECS)).await
}

async fn recv_with_timeout_for<T, C>(
    framed: &mut Framed<T, C>,
    timeout: Duration,
) -> Result<Option<Vec<u8>>>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    C: tokio_util::codec::Decoder<Item = Vec<u8>, Error = anyhow::Error>,
{
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

pub(crate) fn block_bytes(blocks: &[Block]) -> Result<u64> {
    blocks.iter().try_fold(0_u64, |total, block| {
        let block_len = u64::try_from(block.data.len()).map_err(|e| anyhow::anyhow!(e))?;
        total
            .checked_add(block_len)
            .ok_or_else(|| anyhow::anyhow!("block byte count overflow"))
    })
}

#[cfg(test)]
mod tests {
    use super::{
        BlockCompression, TransportFeatures, compress_block_batch_payload,
        decompress_block_batch_payload, local_handshake_version, negotiate_transport_features,
        recv_with_timeout_for,
    };
    use crate::pxs::net::PxsCodec;
    use anyhow::Result;
    use std::time::Duration;
    use tokio_util::codec::Framed;

    #[tokio::test]
    async fn test_recv_with_timeout_for_errors_on_idle_stream() -> Result<()> {
        let (_writer, reader) = tokio::io::duplex(64);
        let mut framed = Framed::new(reader, PxsCodec);

        let Err(err) = recv_with_timeout_for(&mut framed, Duration::from_millis(10)).await else {
            anyhow::bail!("expected idle timeout");
        };

        assert!(err.to_string().contains("Connection idle timeout"));
        Ok(())
    }

    #[test]
    fn test_local_handshake_version_advertises_block_compression_capabilities() {
        let version = local_handshake_version(true, false);
        assert!(version.contains("+caps="));
        assert!(version.contains("zstd-blocks"));
        assert!(version.contains("lz4-blocks"));
    }

    #[test]
    fn test_negotiate_transport_features_prefers_zstd_when_both_peers_support_it() -> Result<()> {
        let features = negotiate_transport_features(
            &format!("{}+caps=zstd-blocks,lz4-blocks", env!("CARGO_PKG_VERSION")),
            true,
            false,
        )?;
        assert_eq!(
            features,
            TransportFeatures {
                block_compression: BlockCompression::Zstd,
                large_file_parallel: false,
            }
        );
        Ok(())
    }

    #[test]
    fn test_negotiate_transport_features_falls_back_to_lz4_without_peer_zstd() -> Result<()> {
        let features = negotiate_transport_features(
            &format!("{}+caps=lz4-blocks", env!("CARGO_PKG_VERSION")),
            true,
            false,
        )?;
        assert_eq!(
            features,
            TransportFeatures {
                block_compression: BlockCompression::Lz4,
                large_file_parallel: false,
            }
        );
        Ok(())
    }

    #[test]
    fn test_negotiate_transport_features_disables_compression_without_peer_support() -> Result<()> {
        let features = negotiate_transport_features(env!("CARGO_PKG_VERSION"), true, false)?;
        assert_eq!(features, TransportFeatures::default());
        Ok(())
    }

    #[test]
    fn test_negotiate_transport_features_enables_large_file_parallel_when_both_peers_support_it()
    -> Result<()> {
        let features = negotiate_transport_features(
            &format!("{}+caps=large-file-parallel", env!("CARGO_PKG_VERSION")),
            false,
            true,
        )?;
        assert_eq!(
            features,
            TransportFeatures {
                block_compression: BlockCompression::None,
                large_file_parallel: true,
            }
        );
        Ok(())
    }

    #[test]
    fn test_negotiate_transport_features_ignores_unknown_capabilities() -> Result<()> {
        let features = negotiate_transport_features(
            &format!("{}+caps=unknown,zstd-blocks", env!("CARGO_PKG_VERSION")),
            true,
            false,
        )?;
        assert_eq!(
            features,
            TransportFeatures {
                block_compression: BlockCompression::Zstd,
                large_file_parallel: false,
            }
        );
        Ok(())
    }

    #[test]
    fn test_compress_and_decompress_block_batch_payload_with_zstd() -> Result<()> {
        let payload = vec![b'A'; 128 * 1024];
        let compressed = compress_block_batch_payload(&payload, BlockCompression::Zstd)?
            .ok_or_else(|| anyhow::anyhow!("expected compressed payload"))?;
        let decompressed = decompress_block_batch_payload(&compressed, BlockCompression::Zstd)?;
        assert_eq!(decompressed, payload);
        Ok(())
    }

    #[test]
    fn test_compress_and_decompress_block_batch_payload_with_lz4() -> Result<()> {
        let payload = vec![b'A'; 128 * 1024];
        let compressed = compress_block_batch_payload(&payload, BlockCompression::Lz4)?
            .ok_or_else(|| anyhow::anyhow!("expected compressed payload"))?;
        let decompressed = decompress_block_batch_payload(&compressed, BlockCompression::Lz4)?;
        assert_eq!(decompressed, payload);
        Ok(())
    }
}
