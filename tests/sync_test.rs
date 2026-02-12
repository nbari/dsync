use dsync::dsync::{sync, tools};
use filetime::{FileTime, set_file_times};
use std::{
    fs,
    io::{Seek, SeekFrom, Write},
};
use tempfile::tempdir;

#[tokio::test]
async fn test_incremental_sync_only_writes_changed_blocks() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_path = dir.path().join("source.bin");
    let dst_path = dir.path().join("dest.bin");

    // 1. Create a 10MB source file with known data
    let size = 10 * 1024 * 1024;
    let mut initial_data = vec![0u8; size];
    for (i, byte) in initial_data.iter_mut().enumerate() {
        #[allow(clippy::cast_possible_truncation)]
        let val = (i % 256) as u8;
        *byte = val;
    }
    fs::write(&src_path, &initial_data)?;

    // 2. Initial sync (Full copy)
    let stats = sync::sync_changed_blocks(&src_path, &dst_path, true).await?;
    assert_eq!(stats.updated_blocks, 160); // 10MB / 64KB = 160 blocks

    // Verify initial sync
    let dst_data = fs::read(&dst_path)?;
    assert_eq!(initial_data, dst_data);

    // 3. Modify exactly ONE block (64KB) at a specific aligned offset
    let offset = 1024 * 1024; // 1MB offset
    let block_size = 64 * 1024;
    let mut file = fs::OpenOptions::new().write(true).open(&src_path)?;
    file.seek(SeekFrom::Start(offset))?;
    let new_block_data = vec![0xAAu8; block_size];
    file.write_all(&new_block_data)?;
    drop(file);

    // 4. Perform incremental sync
    let stats = sync::sync_changed_blocks(&src_path, &dst_path, false).await?;
    assert_eq!(stats.updated_blocks, 1); // Only 1 block should be updated
    assert_eq!(stats.total_blocks, 160);

    // 5. Verify content is correct
    let final_src_data = fs::read(&src_path)?;
    let final_dst_data = fs::read(&dst_path)?;
    assert_eq!(final_src_data, final_dst_data);

    let start = usize::try_from(offset).map_err(|e| anyhow::anyhow!(e))?;
    let end = start + block_size;
    let slice = final_dst_data
        .get(start..end)
        .ok_or_else(|| anyhow::anyhow!("slice out of bounds"))?;
    assert_eq!(slice, &new_block_data[..]);
    Ok(())
}

#[tokio::test]
async fn test_sync_dir_recursive() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src_dir");
    let dst_dir = dir.path().join("dst_dir");

    fs::create_dir_all(src_dir.join("subdir"))?;
    fs::write(src_dir.join("file1.txt"), "hello")?;
    fs::write(src_dir.join("subdir/file2.txt"), "world")?;

    sync::sync_dir(&src_dir, &dst_dir, 1.0, false, false, &[]).await?;

    assert!(dst_dir.join("file1.txt").exists());
    assert!(dst_dir.join("subdir/file2.txt").exists());
    assert_eq!(
        fs::read_to_string(dst_dir.join("subdir/file2.txt"))?,
        "world"
    );
    Ok(())
}

#[tokio::test]
async fn test_metadata_skip_logic() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_path = dir.path().join("src.txt");
    let dst_path = dir.path().join("dst.txt");

    fs::write(&src_path, "same content")?;
    fs::write(&dst_path, "same content")?;

    // They might not be equal immediately due to write timing, so we don't assert true here
    // But we can assert that if we MODIFY one, it returns false.
    fs::write(&src_path, "different content")?;
    let skip = tools::should_skip_file(&src_path, &dst_path, false).await?;
    assert!(!skip);
    Ok(())
}

#[tokio::test]
async fn test_metadata_skip_uses_nanosecond_precision() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_path = dir.path().join("src.txt");
    let dst_path = dir.path().join("dst.txt");

    fs::write(&src_path, "same content")?;
    fs::write(&dst_path, "same content")?;

    let src_time = FileTime::from_unix_time(1_700_000_000, 100);
    let dst_time = FileTime::from_unix_time(1_700_000_000, 200);
    set_file_times(&src_path, src_time, src_time)?;
    set_file_times(&dst_path, dst_time, dst_time)?;

    let skip = tools::should_skip_file(&src_path, &dst_path, false).await?;
    assert!(!skip);

    set_file_times(&dst_path, src_time, src_time)?;
    let skip = tools::should_skip_file(&src_path, &dst_path, false).await?;
    assert!(skip);

    Ok(())
}

#[tokio::test]
async fn test_threshold_full_copy_decision() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let src_path = dir.path().join("src.bin");
    let dst_path = dir.path().join("dst.bin");

    fs::write(&src_path, vec![1_u8; 1024 * 1024])?;
    fs::write(&dst_path, vec![2_u8; 64 * 1024])?;

    let full_copy = tools::should_use_full_copy(&src_path, &dst_path, 0.5).await?;
    assert!(full_copy);

    let full_copy = tools::should_use_full_copy(&src_path, &dst_path, 0.01).await?;
    assert!(!full_copy);

    Ok(())
}
