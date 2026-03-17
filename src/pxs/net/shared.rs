use super::{
    BLOCK_SIZE,
    protocol::{Block, FileMetadata},
};

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
