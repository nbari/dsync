use anyhow::Result;
use filetime::{FileTime, set_file_times};
use pxs::pxs::sync::{self, SyncOptions};
use pxs::pxs::tools;
use std::{
    fs,
    io::{Seek, SeekFrom, Write},
    process::Command,
};
use tempfile::tempdir;

#[tokio::test]
async fn test_incremental_sync_only_writes_changed_blocks() -> Result<()> {
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
    let stats = sync::sync_changed_blocks(&src_path, &dst_path, true, false, false).await?;
    assert_eq!(stats.updated_blocks, 80); // 10MB / 128KB = 80 blocks

    // Verify initial sync
    let dst_data = fs::read(&dst_path)?;
    assert_eq!(initial_data, dst_data);

    // 3. Modify exactly ONE block (128KB) at a specific aligned offset
    let offset = 1024 * 1024; // 1MB offset
    let block_size = 128 * 1024;
    let mut file = fs::OpenOptions::new().write(true).open(&src_path)?;
    file.seek(SeekFrom::Start(offset))?;
    let new_block_data = vec![0xAAu8; block_size];
    file.write_all(&new_block_data)?;
    drop(file);

    // 4. Perform incremental sync
    let stats = sync::sync_changed_blocks(&src_path, &dst_path, false, false, false).await?;
    assert_eq!(stats.updated_blocks, 1); // Only 1 block should be updated
    assert_eq!(stats.total_blocks, 80);

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
async fn test_sync_dir_recursive() -> Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src_dir");
    let dst_dir = dir.path().join("dst_dir");

    fs::create_dir_all(src_dir.join("subdir"))?;
    fs::write(src_dir.join("file1.txt"), "hello")?;
    fs::write(src_dir.join("subdir/file2.txt"), "world")?;

    let options = SyncOptions::new(1.0, false, false, false, Vec::new(), false, false);
    sync::sync_dir(&src_dir, &dst_dir, &options).await?;

    assert!(dst_dir.join("file1.txt").exists());
    assert!(dst_dir.join("subdir/file2.txt").exists());
    assert_eq!(
        fs::read_to_string(dst_dir.join("subdir/file2.txt"))?,
        "world"
    );
    Ok(())
}

#[tokio::test]
async fn test_sync_changed_blocks_rejects_symlinked_parent_destination() -> Result<()> {
    let dir = tempdir()?;
    let src_path = dir.path().join("source.txt");
    let dst_root = dir.path().join("dst");
    let external_dir = tempdir()?;
    let protected_path = external_dir.path().join("payload.txt");
    fs::write(&src_path, "payload")?;
    fs::create_dir_all(&dst_root)?;
    fs::write(&protected_path, "protected")?;
    std::os::unix::fs::symlink(external_dir.path(), dst_root.join("escape"))?;

    let Err(error) = sync::sync_changed_blocks(
        &src_path,
        &dst_root.join("escape/payload.txt"),
        true,
        false,
        true,
    )
    .await
    else {
        anyhow::bail!("single-file sync should reject symlinked parent destinations");
    };

    assert!(error.to_string().contains("symlinked parent"));
    assert_eq!(fs::read_to_string(&protected_path)?, "protected");
    Ok(())
}

#[tokio::test]
async fn test_sync_dir_rejects_symlinked_destination_root() -> Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let external_dir = tempdir()?;
    let dst_root = dir.path().join("dst-link");
    let protected_path = external_dir.path().join("payload.txt");
    fs::create_dir_all(&src_dir)?;
    fs::write(src_dir.join("payload.txt"), "payload")?;
    fs::write(&protected_path, "protected")?;
    std::os::unix::fs::symlink(external_dir.path(), &dst_root)?;

    let options = SyncOptions::new(1.0, false, false, false, Vec::new(), false, false);
    let Err(error) = sync::sync_dir(&src_dir, &dst_root, &options).await else {
        anyhow::bail!("directory sync should reject symlinked destination roots");
    };

    assert!(
        error
            .to_string()
            .contains("destination root must not be a symlink")
    );
    assert_eq!(fs::read_to_string(&protected_path)?, "protected");
    Ok(())
}

#[tokio::test]
async fn test_metadata_skip_logic() -> Result<()> {
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
async fn test_metadata_skip_uses_nanosecond_precision() -> Result<()> {
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
async fn test_threshold_full_copy_decision() -> Result<()> {
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

#[tokio::test]
async fn test_threshold_full_copy_decision_above_one_mebibyte_respects_user_value() -> Result<()> {
    let dir = tempdir()?;
    let src_path = dir.path().join("src-large.bin");
    let dst_path = dir.path().join("dst-small.bin");

    fs::write(&src_path, vec![1_u8; 2 * 1024 * 1024])?;
    fs::write(&dst_path, vec![2_u8; 384 * 1024])?;

    let full_copy = tools::should_use_full_copy(&src_path, &dst_path, 0.2).await?;
    assert!(full_copy);

    let full_copy = tools::should_use_full_copy(&src_path, &dst_path, 0.1).await?;
    assert!(!full_copy);

    Ok(())
}

#[tokio::test]
async fn test_symlink_over_directory() -> Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    fs::create_dir_all(&src_dir)?;
    fs::create_dir_all(&dst_dir)?;

    let dst_subdir = dst_dir.join("link");
    fs::create_dir_all(&dst_subdir)?;

    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("target", src_dir.join("link"))?;
    }

    let options = SyncOptions::new(1.0, false, false, false, Vec::new(), false, false);
    sync::sync_dir(&src_dir, &dst_dir, &options).await?;

    let dst_link = dst_dir.join("link");
    assert!(dst_link.is_symlink());
    assert_eq!(
        fs::read_link(&dst_link)?,
        std::path::PathBuf::from("target")
    );

    Ok(())
}

#[tokio::test]
async fn test_file_replaces_directory() -> Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    fs::create_dir_all(&src_dir)?;
    fs::create_dir_all(dst_dir.join("entry/nested"))?;
    fs::write(dst_dir.join("entry/nested/file.txt"), "stale")?;

    fs::write(src_dir.join("entry"), "replacement")?;

    let options = SyncOptions::new(1.0, false, false, false, Vec::new(), false, false);
    sync::sync_dir(&src_dir, &dst_dir, &options).await?;

    let dst_entry = dst_dir.join("entry");
    assert!(dst_entry.is_file());
    assert_eq!(fs::read_to_string(&dst_entry)?, "replacement");

    Ok(())
}

#[tokio::test]
async fn test_directory_replaces_file() -> Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    fs::create_dir_all(&src_dir)?;
    fs::create_dir_all(&dst_dir)?;

    fs::create_dir_all(src_dir.join("subdir"))?;
    fs::write(src_dir.join("subdir/file.txt"), "hello")?;

    fs::write(dst_dir.join("subdir"), "i am a file")?;

    let options = SyncOptions::new(1.0, false, false, false, Vec::new(), false, false);
    sync::sync_dir(&src_dir, &dst_dir, &options).await?;

    assert!(dst_dir.join("subdir").is_dir());
    assert_eq!(
        fs::read_to_string(dst_dir.join("subdir/file.txt"))?,
        "hello"
    );

    Ok(())
}

#[tokio::test]
async fn test_sync_dir_root_is_file() -> Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_root = dir.path().join("dst");

    fs::create_dir_all(&src_dir)?;
    fs::write(src_dir.join("file.txt"), "hello")?;

    fs::write(&dst_root, "i am a file")?;

    let options = SyncOptions::new(1.0, false, false, false, Vec::new(), false, false);
    sync::sync_dir(&src_dir, &dst_root, &options).await?;

    assert!(dst_root.is_dir());
    assert_eq!(fs::read_to_string(dst_root.join("file.txt"))?, "hello");

    Ok(())
}

#[tokio::test]
async fn test_sync_dir_with_delete() -> Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");

    fs::create_dir_all(src_dir.join("kept"))?;
    fs::write(src_dir.join("kept/file.txt"), "content")?;

    fs::create_dir_all(dst_dir.join("extraneous"))?;
    fs::write(dst_dir.join("extraneous/old.txt"), "delete me")?;
    fs::write(dst_dir.join("stale.txt"), "delete me too")?;

    // Run sync WITHOUT delete first - extraneous files should remain
    let options_no_delete = SyncOptions::new(1.0, false, false, false, Vec::new(), false, false);
    sync::sync_dir(&src_dir, &dst_dir, &options_no_delete).await?;
    assert!(dst_dir.join("extraneous/old.txt").exists());
    assert!(dst_dir.join("stale.txt").exists());

    // Run sync WITH delete
    let options_delete = SyncOptions::new(1.0, false, false, true, Vec::new(), false, false);
    sync::sync_dir(&src_dir, &dst_dir, &options_delete).await?;
    assert!(!dst_dir.join("extraneous").exists());
    assert!(!dst_dir.join("stale.txt").exists());
    assert!(dst_dir.join("kept/file.txt").exists());

    Ok(())
}

#[tokio::test]
async fn test_sync_dir_with_delete_and_ignore() -> Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");

    fs::create_dir_all(&src_dir)?;
    fs::create_dir_all(&dst_dir)?;
    fs::write(src_dir.join("file.txt"), "content")?;

    fs::write(dst_dir.join("ignored.tmp"), "do not delete")?;
    fs::write(dst_dir.join("stale.txt"), "delete me")?;

    // Run sync WITH delete and ignore
    let options = SyncOptions::new(
        1.0,
        false,
        false,
        true,
        vec!["*.tmp".to_string()],
        false,
        false,
    );
    sync::sync_dir(&src_dir, &dst_dir, &options).await?;

    assert!(dst_dir.join("ignored.tmp").exists());
    assert!(!dst_dir.join("stale.txt").exists());
    assert!(dst_dir.join("file.txt").exists());

    Ok(())
}

#[tokio::test]
async fn test_sync_dir_with_delete_preserves_final_directory_mtime() -> Result<()> {
    use std::os::unix::fs::MetadataExt;

    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");

    fs::create_dir_all(src_dir.join("sub"))?;
    fs::create_dir_all(dst_dir.join("sub"))?;
    fs::write(src_dir.join("sub/keep.txt"), "keep")?;
    fs::write(dst_dir.join("sub/keep.txt"), "keep")?;
    fs::write(dst_dir.join("sub/stale.txt"), "stale")?;

    let time = FileTime::from_unix_time(1_700_000_100, 0);
    set_file_times(src_dir.join("sub"), time, time)?;
    set_file_times(&src_dir, time, time)?;

    let options = SyncOptions::new(1.0, false, false, true, Vec::new(), false, false);
    sync::sync_dir(&src_dir, &dst_dir, &options).await?;

    let src_meta = fs::metadata(src_dir.join("sub"))?;
    let dst_meta = fs::metadata(dst_dir.join("sub"))?;
    assert_eq!(src_meta.mtime(), dst_meta.mtime());
    assert_eq!(src_meta.mtime_nsec(), dst_meta.mtime_nsec());

    Ok(())
}

#[tokio::test]
async fn test_sync_with_checksum_force() -> Result<()> {
    let dir = tempdir()?;
    let src_path = dir.path().join("src.txt");
    let dst_path = dir.path().join("dst.txt");

    // 1. Create identical mtime/size but different content
    fs::write(&src_path, "source content")?;
    fs::write(&dst_path, "destin content")?; // Same length
    let time = FileTime::from_unix_time(1_700_000_000, 0);
    set_file_times(&src_path, time, time)?;
    set_file_times(&dst_path, time, time)?;

    // 2. Sync WITHOUT checksum - should skip (metadata matches)
    let skip = tools::should_skip_file(&src_path, &dst_path, false).await?;
    assert!(skip);

    // 3. Sync WITH checksum - should detect difference
    // should_skip_file returns false if checksum is true and sizes match
    let skip = tools::should_skip_file(&src_path, &dst_path, true).await?;
    assert!(!skip);

    let stats = sync::sync_changed_blocks(&src_path, &dst_path, false, false, false).await?;
    assert_eq!(stats.updated_blocks, 1);
    assert_eq!(fs::read_to_string(&dst_path)?, "source content");

    Ok(())
}

#[tokio::test]
async fn test_sync_subcommand_with_checksum_updates_matching_metadata_file() -> Result<()> {
    let dir = tempdir()?;
    let src_path = dir.path().join("src.bin");
    let dst_path = dir.path().join("dst.bin");

    let src_bytes = b"source payload 123";
    let dst_bytes = b"destin payload 123"; // same length, different content
    fs::write(&src_path, src_bytes)?;
    fs::write(&dst_path, dst_bytes)?;

    let time = FileTime::from_unix_time(1_700_000_000, 0);
    set_file_times(&src_path, time, time)?;
    set_file_times(&dst_path, time, time)?;

    let output = Command::new(env!("CARGO_BIN_EXE_pxs"))
        .arg("sync")
        .arg(&src_path)
        .arg(&dst_path)
        .arg("--checksum")
        .arg("--quiet")
        .output()?;

    assert!(
        output.status.success(),
        "sync failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let src_hash = tools::blake3_file_hash(&src_path).await?;
    let dst_hash = tools::blake3_file_hash(&dst_path).await?;
    assert_eq!(src_hash, dst_hash);
    assert_eq!(fs::read(&dst_path)?, src_bytes);

    Ok(())
}

#[tokio::test]
async fn test_sync_dry_run_safety() -> Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    fs::create_dir_all(&src_dir)?;
    fs::write(src_dir.join("new_file.txt"), "content")?;

    // Run sync with dry-run
    let options = SyncOptions::new(1.0, false, true, false, Vec::new(), false, false);
    sync::sync_dir(&src_dir, &dst_dir, &options).await?;

    // Destination should NOT be created
    assert!(!dst_dir.exists());

    // Create destination but with a file that would be replaced
    fs::create_dir_all(&dst_dir)?;
    fs::write(dst_dir.join("new_file.txt"), "old content")?;

    sync::sync_dir(&src_dir, &dst_dir, &options).await?;
    // File should NOT be updated
    assert_eq!(
        fs::read_to_string(dst_dir.join("new_file.txt"))?,
        "old content"
    );

    Ok(())
}

#[tokio::test]
async fn test_sync_with_exclude_from_file() -> Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    let exclude_file = dir.path().join("excludes.txt");

    fs::create_dir_all(&src_dir)?;
    fs::write(src_dir.join("keep.txt"), "keep")?;
    fs::write(src_dir.join("skip.log"), "skip")?;
    fs::write(&exclude_file, "*.log\n")?;

    // In a real CLI run, the dispatcher parses the file.
    // Here we simulate that by passing the parsed patterns.
    let patterns = vec!["*.log".to_string()];
    let options = SyncOptions::new(1.0, false, false, false, patterns, false, false);

    sync::sync_dir(&src_dir, &dst_dir, &options).await?;

    assert!(dst_dir.join("keep.txt").exists());
    assert!(!dst_dir.join("skip.log").exists());

    Ok(())
}

#[tokio::test]
async fn test_sync_broken_symlink_at_destination() -> Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    fs::create_dir_all(&src_dir)?;
    fs::create_dir_all(&dst_dir)?;

    // 1. Create a real file in source
    let src_file = src_dir.join("conflict.txt");
    fs::write(&src_file, "new data")?;

    // 2. Create a broken symlink in destination
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("missing_target", dst_dir.join("conflict.txt"))?;
    }

    // 3. Sync - should replace broken symlink with real file
    let options = SyncOptions::new(1.0, false, false, false, Vec::new(), false, false);
    sync::sync_dir(&src_dir, &dst_dir, &options).await?;

    let dst_file = dst_dir.join("conflict.txt");
    assert!(dst_file.is_file());
    assert!(!dst_file.is_symlink());
    assert_eq!(fs::read_to_string(&dst_file)?, "new data");

    Ok(())
}

#[tokio::test]
async fn test_sync_dir_replaces_matching_leaf_symlink_file() -> Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    let other_dir = dir.path().join("other");
    let src_file = src_dir.join("item.txt");
    let target_file = other_dir.join("target.txt");
    let dst_file = dst_dir.join("item.txt");

    fs::create_dir_all(&src_dir)?;
    fs::create_dir_all(&dst_dir)?;
    fs::create_dir_all(&other_dir)?;
    fs::write(&src_file, "hello world")?;
    fs::write(&target_file, "hello world")?;
    std::os::unix::fs::symlink(&target_file, &dst_file)?;
    let time = FileTime::from_unix_time(1_700_000_001, 0);
    set_file_times(&src_file, time, time)?;
    set_file_times(&target_file, time, time)?;

    let options = SyncOptions::new(1.0, false, false, false, Vec::new(), false, false);
    sync::sync_dir(&src_dir, &dst_dir, &options).await?;

    assert!(!fs::symlink_metadata(&dst_file)?.file_type().is_symlink());
    assert_eq!(fs::read_to_string(&dst_file)?, "hello world");
    Ok(())
}

#[tokio::test]
async fn test_sync_source_broken_symlink() -> Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    fs::create_dir_all(&src_dir)?;

    // 1. Create a broken symlink in source
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("non_existent", src_dir.join("broken_link"))?;
    }

    // 2. Sync
    let options = SyncOptions::new(1.0, false, false, false, Vec::new(), false, false);
    sync::sync_dir(&src_dir, &dst_dir, &options).await?;

    // 3. Destination should have the same broken symlink
    let dst_link = dst_dir.join("broken_link");
    assert!(dst_link.is_symlink());
    assert_eq!(
        fs::read_link(&dst_link)?,
        std::path::PathBuf::from("non_existent")
    );

    Ok(())
}

#[tokio::test]
async fn test_sync_single_file_replaces_matching_leaf_symlink() -> Result<()> {
    let dir = tempdir()?;
    let src_file = dir.path().join("source.txt");
    let dst_dir = dir.path().join("dst");
    let other_dir = dir.path().join("other");
    let target_file = other_dir.join("target.txt");
    let dst_file = dst_dir.join("out.txt");

    fs::create_dir_all(&dst_dir)?;
    fs::create_dir_all(&other_dir)?;
    fs::write(&src_file, "hello world")?;
    fs::write(&target_file, "hello world")?;
    std::os::unix::fs::symlink(&target_file, &dst_file)?;
    let time = FileTime::from_unix_time(1_700_000_000, 0);
    set_file_times(&src_file, time, time)?;
    set_file_times(&target_file, time, time)?;

    sync::sync_changed_blocks(&src_file, &dst_file, false, false, true).await?;

    assert!(!fs::symlink_metadata(&dst_file)?.file_type().is_symlink());
    assert_eq!(fs::read_to_string(&dst_file)?, "hello world");
    Ok(())
}

#[tokio::test]
async fn test_sync_delete_preserves_broken_symlink_match() -> Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    fs::create_dir_all(&src_dir)?;
    fs::create_dir_all(&dst_dir)?;

    // 1. Source has a broken symlink
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("away", src_dir.join("item"))?;
    }

    // 2. Destination has a matching file (or symlink)
    fs::write(dst_dir.join("item"), "stale content")?;

    // 3. Sync WITH delete.
    // It should NOT delete 'item' from destination because it exists in source (as a broken symlink)
    // Instead it should REPLACE it with the symlink.
    let options = SyncOptions::new(1.0, false, false, true, Vec::new(), false, false);
    sync::sync_dir(&src_dir, &dst_dir, &options).await?;

    let dst_item = dst_dir.join("item");
    assert!(dst_item.is_symlink());
    assert!(dst_item.exists() || dst_item.is_symlink()); // exists() is false if broken, but metadata works

    Ok(())
}

#[tokio::test]
async fn test_sync_replace_symlink_dir_with_real_dir() -> Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    fs::create_dir_all(&src_dir)?;
    fs::create_dir_all(&dst_dir)?;

    // 1. Source has a real directory
    fs::create_dir_all(src_dir.join("logs"))?;
    fs::write(src_dir.join("logs/current.log"), "data")?;

    // 2. Destination has a symlink where that directory should be
    #[cfg(unix)]
    {
        let other_dir = dir.path().join("other");
        fs::create_dir_all(&other_dir)?;
        std::os::unix::fs::symlink(&other_dir, dst_dir.join("logs"))?;
    }

    // 3. Sync - should remove symlink and create real directory
    let options = SyncOptions::new(1.0, false, false, false, Vec::new(), false, false);
    sync::sync_dir(&src_dir, &dst_dir, &options).await?;

    let dst_logs = dst_dir.join("logs");
    assert!(dst_logs.is_dir());
    assert!(!dst_logs.is_symlink());
    assert!(dst_logs.join("current.log").exists());

    Ok(())
}

#[tokio::test]
async fn test_sync_quiet_mode_smoke() -> Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    fs::create_dir_all(&src_dir)?;
    fs::write(src_dir.join("file.txt"), "content")?;

    // Just verify it runs without error
    let options = SyncOptions::new(1.0, false, false, false, Vec::new(), false, true);
    sync::sync_dir(&src_dir, &dst_dir, &options).await?;

    assert!(dst_dir.join("file.txt").exists());
    Ok(())
}

#[tokio::test]
#[cfg(unix)]
async fn test_sync_skips_fifo_without_failing() -> Result<()> {
    use nix::sys::stat::Mode;
    use nix::unistd::mkfifo;

    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    fs::create_dir_all(&src_dir)?;

    fs::write(src_dir.join("file.txt"), "content")?;
    let fifo_path = src_dir.join("queue.fifo");
    mkfifo(&fifo_path, Mode::S_IRUSR | Mode::S_IWUSR)?;

    let options = SyncOptions::new(1.0, false, false, false, Vec::new(), false, false);
    sync::sync_dir(&src_dir, &dst_dir, &options).await?;

    assert_eq!(fs::read_to_string(dst_dir.join("file.txt"))?, "content");
    assert!(!dst_dir.join("queue.fifo").exists());

    Ok(())
}

#[tokio::test]
#[cfg(unix)]
async fn test_sync_skips_unix_socket_without_failing() -> Result<()> {
    use std::os::unix::net::UnixListener;

    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    fs::create_dir_all(&src_dir)?;

    fs::write(src_dir.join("file.txt"), "content")?;
    let socket_path = src_dir.join("service.sock");
    let _listener = UnixListener::bind(&socket_path)?;

    let options = SyncOptions::new(1.0, false, false, false, Vec::new(), false, false);
    sync::sync_dir(&src_dir, &dst_dir, &options).await?;

    assert_eq!(fs::read_to_string(dst_dir.join("file.txt"))?, "content");
    assert!(!dst_dir.join("service.sock").exists());

    Ok(())
}

#[tokio::test]
#[cfg(target_os = "linux")]
async fn test_sync_non_utf8_filename() -> Result<()> {
    use std::os::unix::ffi::OsStringExt;

    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    fs::create_dir_all(&src_dir)?;

    // Create a filename with invalid UTF-8 (0xFF is not valid UTF-8)
    let bad_filename = std::ffi::OsString::from_vec(vec![0xFF, 0xFE, 0xFD]);
    let src_file = src_dir.join(&bad_filename);
    fs::write(&src_file, "raw bytes")?;

    let options = SyncOptions::new(1.0, false, false, false, Vec::new(), false, false);
    sync::sync_dir(&src_dir, &dst_dir, &options).await?;

    let dst_file = dst_dir.join(&bad_filename);
    assert!(dst_file.exists());
    assert_eq!(fs::read_to_string(&dst_file)?, "raw bytes");

    Ok(())
}

#[tokio::test]
async fn test_sync_staged_file_cleanup_on_error() -> Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    fs::create_dir_all(&src_dir)?;
    fs::create_dir_all(&dst_dir)?;

    let src_file = src_dir.join("broken.bin");
    fs::write(&src_file, "some data")?;

    // Create a scenario where sync fails - e.g. destination parent is suddenly removed or made read-only
    // Actually, a simpler way is to test the drop behavior directly in a small scope
    {
        let dst_path = dst_dir.join("target.bin");
        let staged = pxs::pxs::tools::StagedFile::new(&dst_path)?;
        staged.prepare(10, false)?;
        let staged_path = staged.path().to_path_buf();
        assert!(staged_path.exists());
        // Scope ends, staged is dropped without commit()
    }

    // Verify temp file is gone
    let entries: Vec<_> = fs::read_dir(&dst_dir)?.collect();
    for entry in entries {
        let name = entry?.file_name().to_string_lossy().into_owned();
        assert!(!name.contains(".pxs.") || !name.contains(".tmp"));
    }

    Ok(())
}

#[tokio::test]
async fn test_sync_stats_aggregation() -> Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    fs::create_dir_all(&src_dir)?;
    fs::create_dir_all(&dst_dir)?;

    // 1. One updated file (8 blocks)
    let file1 = src_dir.join("file1.bin");
    fs::write(&file1, vec![0u8; 1024 * 1024])?; // 1MB = 8 blocks

    // 2. One skipped file
    let file2 = src_dir.join("file2.bin");
    fs::write(&file2, "content")?;
    fs::write(dst_dir.join("file2.bin"), "content")?;
    let time = FileTime::from_unix_time(1_700_000_000, 0);
    set_file_times(&file2, time, time)?;
    set_file_times(dst_dir.join("file2.bin"), time, time)?;

    let options = SyncOptions::new(1.0, false, false, false, Vec::new(), false, false);
    let stats = sync::sync_dir(&src_dir, &dst_dir, &options).await?;

    // 1MB file = 8 blocks (128KB each)
    // small file = 1 block
    // Total should be 9 blocks, 8 updated (from file1), 0 updated (from file2)
    assert_eq!(stats.updated_blocks, 8);
    assert_eq!(stats.total_blocks, 9);

    Ok(())
}

#[tokio::test]
async fn test_sync_stats_aggregation_with_backpressure() -> Result<()> {
    let dir = tempdir()?;
    let src_dir = dir.path().join("src");
    let dst_dir = dir.path().join("dst");
    fs::create_dir_all(&src_dir)?;
    fs::create_dir_all(&dst_dir)?;

    let file_count = tools::MAX_PARALLELISM + 8;
    for index in 0..file_count {
        fs::write(src_dir.join(format!("file-{index}.bin")), [b'x'])?;
    }

    let options = SyncOptions::new(1.0, false, false, false, Vec::new(), false, false);
    let stats = sync::sync_dir(&src_dir, &dst_dir, &options).await?;

    assert_eq!(stats.updated_blocks, file_count);
    assert_eq!(stats.total_blocks, file_count);
    Ok(())
}
