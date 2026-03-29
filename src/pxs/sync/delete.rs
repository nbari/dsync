use crate::pxs::sync::dir::DirectorySyncContext;
use crate::pxs::tools;
use anyhow::{Context, Result};

/// Delete extraneous files from destination that do not exist in the source.
///
/// # Errors
///
/// Returns an error if any IO operation fails.
pub(crate) async fn delete_extraneous_files(context: &DirectorySyncContext<'_>) -> Result<()> {
    let mut override_builder = ignore::overrides::OverrideBuilder::new(context.dst_dir);
    for pattern in &context.options.ignores {
        override_builder.add(&format!("!{pattern}"))?;
    }
    let overrides = override_builder.build()?;

    let walker = ignore::WalkBuilder::new(context.dst_dir)
        .hidden(false)
        .git_ignore(false)
        .git_global(false)
        .git_exclude(false)
        .ignore(false)
        .parents(false)
        .overrides(overrides)
        .build();

    let mut to_delete = Vec::new();
    for entry in walker {
        let entry = entry?;
        let dst_path = entry.path();
        if dst_path == context.dst_dir {
            continue;
        }

        let rel_path = dst_path.strip_prefix(context.dst_dir)?;
        let src_path = context.src_dir.join(rel_path);

        match tokio::fs::symlink_metadata(&src_path).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                to_delete.push(dst_path.to_path_buf());
            }
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("failed to read source metadata for {}", src_path.display())
                });
            }
        }
    }

    // Sort to delete deepest items first
    to_delete.sort_by_key(|path| std::cmp::Reverse(path.components().count()));

    for path in to_delete {
        // Optimization: check if path still exists (might have been deleted by remove_dir_all on parent)
        if tokio::fs::symlink_metadata(&path).await.is_err() {
            continue;
        }

        if context.options.dry_run {
            eprintln!("(dry-run) delete: {}", path.display());
            continue;
        }

        let meta = tokio::fs::symlink_metadata(&path).await.with_context(|| {
            format!("failed to read destination metadata for {}", path.display())
        })?;
        if meta.is_dir() {
            tokio::fs::remove_dir_all(&path)
                .await
                .with_context(|| format!("failed to remove directory {}", path.display()))?;
        } else {
            tokio::fs::remove_file(&path)
                .await
                .with_context(|| format!("failed to remove file {}", path.display()))?;
        }
        if context.options.fsync {
            tools::sync_parent_directory(&path)?;
        }
    }

    Ok(())
}
