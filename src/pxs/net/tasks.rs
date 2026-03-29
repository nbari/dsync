use super::{
    path::{protocol_path_to_pathbuf, validate_protocol_path},
    protocol::FileMetadata,
};
use anyhow::Result;
use std::{
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
};

#[derive(Clone)]
pub(crate) enum SyncTask {
    Dir {
        path: Vec<u8>,
        metadata: FileMetadata,
    },
    Symlink {
        path: Vec<u8>,
        target: Vec<u8>,
        metadata: FileMetadata,
    },
    File {
        path: Vec<u8>,
    },
}

fn build_overrides(src_root: &Path, ignores: &[String]) -> Result<ignore::overrides::Override> {
    use ignore::overrides::OverrideBuilder;

    let mut override_builder = OverrideBuilder::new(src_root);
    for pattern in ignores {
        override_builder.add(&format!("!{pattern}"))?;
    }
    Ok(override_builder.build()?)
}

pub(crate) fn source_path_for(src_root: &Path, rel_path: &[u8]) -> Result<PathBuf> {
    if src_root.is_file() {
        Ok(src_root.to_path_buf())
    } else {
        Ok(src_root.join(protocol_path_to_pathbuf(rel_path)?))
    }
}

fn collect_sync_tasks_sync(src_root: &Path, ignores: &[String]) -> Result<(Vec<SyncTask>, u64)> {
    use ignore::WalkBuilder;

    let mut tasks = Vec::new();
    let mut total_size = 0_u64;

    if src_root.is_file() {
        let metadata = FileMetadata::from(std::fs::metadata(src_root)?);
        let path = src_root
            .file_name()
            .map(OsStrExt::as_bytes)
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow::anyhow!("source file has no name: {}", src_root.display()))?;
        validate_protocol_path(&path)?;
        total_size = metadata.size;
        tasks.push(SyncTask::File { path });
        return Ok((tasks, total_size));
    }

    let overrides = build_overrides(src_root, ignores)?;
    let walker = WalkBuilder::new(src_root)
        .hidden(false)
        .git_ignore(false)
        .git_global(false)
        .git_exclude(false)
        .ignore(false)
        .parents(false)
        .overrides(overrides)
        .build();

    for entry in walker {
        let entry = entry?;
        let path = entry.path();
        if path == src_root {
            continue;
        }

        let mut rel_path = Vec::new();
        for (index, component) in path.strip_prefix(src_root)?.components().enumerate() {
            if index > 0 {
                rel_path.push(b'/');
            }
            rel_path.extend_from_slice(component.as_os_str().as_bytes());
        }
        validate_protocol_path(&rel_path)?;
        let metadata = FileMetadata::from(entry.metadata()?);

        if entry.file_type().is_some_and(|ft| ft.is_symlink()) {
            let target = std::fs::read_link(path)?;
            tasks.push(SyncTask::Symlink {
                path: rel_path,
                target: target.as_os_str().as_bytes().to_vec(),
                metadata,
            });
        } else if entry.file_type().is_some_and(|ft| ft.is_dir()) {
            tasks.push(SyncTask::Dir {
                path: rel_path,
                metadata,
            });
        } else if entry.file_type().is_some_and(|ft| ft.is_file()) {
            total_size += metadata.size;
            tasks.push(SyncTask::File { path: rel_path });
        }
    }

    Ok((tasks, total_size))
}

pub(crate) async fn collect_sync_tasks(
    src_root: &Path,
    ignores: &[String],
) -> Result<(Vec<SyncTask>, u64)> {
    let src_root_owned = src_root.to_path_buf();
    let ignores_owned = ignores.to_vec();
    tokio::task::spawn_blocking(move || collect_sync_tasks_sync(&src_root_owned, &ignores_owned))
        .await?
}
