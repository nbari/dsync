use std::hash::Hasher;
use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use twox_hash::XxHash64;

const BLOCK_SIZE: usize = 64 * 1024;

fn fast_hash_block(data: &[u8]) -> u64 {
    let mut hasher = XxHash64::with_seed(0);
    hasher.write(data);
    hasher.finish()
}

pub async fn sync_changed_blocks(src_path: &PathBuf, dst_path: &PathBuf) -> std::io::Result<()> {
    let mut src = File::open(src_path).await?;
    let mut dst = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(dst_path)
        .await?;

    let src_len = src.metadata().await?.len();
    let dst_len = dst.metadata().await?.len();

    if dst_len < src_len {
        dst.set_len(src_len).await?;
    }

    let mut offset = 0u64;
    let mut src_buf = vec![0u8; BLOCK_SIZE];
    let mut dst_buf = vec![0u8; BLOCK_SIZE];

    while offset < src_len {
        let to_read = std::cmp::min(BLOCK_SIZE as u64, src_len - offset) as usize;

        src.seek(SeekFrom::Start(offset)).await?;
        let src_n = src.read(&mut src_buf[..to_read]).await?;

        dst.seek(SeekFrom::Start(offset)).await?;
        let dst_n = dst.read(&mut dst_buf[..to_read]).await?;

        let src_hash = fast_hash_block(&src_buf[..src_n]);
        let dst_hash = fast_hash_block(&dst_buf[..dst_n]);

        if src_hash != dst_hash {
            dst.seek(SeekFrom::Start(offset)).await?;
            dst.write_all(&src_buf[..src_n]).await?;
            println!("Synced block at offset {}", offset);
        }

        offset += src_n as u64;
    }

    if dst_len > src_len {
        dst.set_len(src_len).await?;
        println!("Truncated destination to match source");
    }

    Ok(())
}
