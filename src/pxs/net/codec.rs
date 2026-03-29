use super::MAX_FRAME_SIZE;
use anyhow::Result;
use bytes::{Buf, BufMut, BytesMut};
use rkyv::util::AlignedVec;
use tokio_util::codec::{Decoder, Encoder};

// Wire-level frame marker for the pxs protocol.
//
// Frame layout:
// 1. 4-byte magic marker: "PXS1"
// 2. 4-byte big-endian payload length
// 3. Serialized `Message` payload
//
// The trailing "1" acts as a simple protocol-generation tag. If the framing
// format changes in an incompatible way, this marker should change as well.
//
// Important: changing this value is a wire-compatibility break. Old binaries
// using the previous marker will not be able to talk to binaries using this one.
const MAGIC: &[u8; 4] = b"PXS1";

/// Length-prefixed frame codec for the pxs network protocol.
///
/// This codec is intentionally simple:
/// - every frame starts with [`MAGIC`]
/// - payload length is stored as a 4-byte big-endian integer
/// - payload bytes are an `rkyv`-serialized `Message`
///
/// The decoder also attempts to resynchronize if stray bytes appear before a
/// valid frame by scanning forward until it finds the next matching magic marker.
pub struct PxsCodec;

impl Decoder for PxsCodec {
    type Item = Vec<u8>;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            if src.len() < 8 {
                return Ok(None);
            }

            if src.get(0..4) != Some(MAGIC) {
                if let Some(pos) = memchr::memchr(MAGIC[0], src) {
                    src.advance(pos);
                    if src.len() < 4 {
                        return Ok(None);
                    }
                    if src.get(0..4) != Some(MAGIC) {
                        src.advance(1);
                        continue;
                    }
                } else {
                    src.clear();
                    return Ok(None);
                }
            }

            let mut len_bytes = [0_u8; 4];
            if let Some(bytes) = src.get(4..8) {
                len_bytes.copy_from_slice(bytes);
            } else {
                return Ok(None);
            }
            let len = u32::from_be_bytes(len_bytes) as usize;
            if len > MAX_FRAME_SIZE {
                return Err(anyhow::anyhow!("Frame size too big: {len}"));
            }

            if src.len() < 8 + len {
                return Ok(None);
            }
            src.advance(8);
            let data = src.split_to(len).to_vec();
            return Ok(Some(data));
        }
    }
}

impl Encoder<AlignedVec<16>> for PxsCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, item: AlignedVec<16>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if item.len() > MAX_FRAME_SIZE {
            return Err(anyhow::anyhow!("Frame size too big: {}", item.len()));
        }

        dst.reserve(8 + item.len());
        dst.put_slice(MAGIC);
        let item_len = u32::try_from(item.len()).map_err(|e| anyhow::anyhow!(e))?;
        dst.put_u32(item_len);
        dst.put_slice(&item);
        Ok(())
    }
}
