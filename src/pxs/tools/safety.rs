use anyhow::Result;
use std::path::Path;

/// Ensure that no existing ancestor of `path` is a symlink.
///
/// # Errors
///
/// Returns an error if any existing ancestor is a symlink or metadata lookup fails.
pub fn ensure_no_symlink_ancestors(path: &Path) -> Result<()> {
    let mut current = path.parent();
    while let Some(parent) = current {
        match std::fs::symlink_metadata(parent) {
            Ok(meta) => {
                anyhow::ensure!(
                    !meta.file_type().is_symlink(),
                    "destination path traverses through symlinked parent: {}",
                    parent.display()
                );
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        current = parent.parent();
    }
    Ok(())
}

/// Ensure that no existing ancestor below `root` is a symlink.
///
/// # Errors
///
/// Returns an error if `path` is not under `root`, any existing ancestor below
/// `root` is a symlink, or metadata lookup fails.
pub fn ensure_no_symlink_ancestors_under_root(root: &Path, path: &Path) -> Result<()> {
    ensure_no_symlink_ancestors(root)?;
    match std::fs::symlink_metadata(root) {
        Ok(meta) => {
            anyhow::ensure!(
                !meta.file_type().is_symlink(),
                "destination root must not be a symlink: {}",
                root.display()
            );
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    let rel_path = path.strip_prefix(root).map_err(|_| {
        anyhow::anyhow!(
            "destination path {} is not under root {}",
            path.display(),
            root.display()
        )
    })?;

    let mut current = root.to_path_buf();
    let components = rel_path.components().collect::<Vec<_>>();
    for (index, component) in components.iter().enumerate() {
        current.push(component.as_os_str());
        let is_leaf = index + 1 == components.len();
        if is_leaf {
            break;
        }
        match std::fs::symlink_metadata(&current) {
            Ok(meta) => {
                anyhow::ensure!(
                    !meta.file_type().is_symlink(),
                    "destination path escapes root through symlinked parent: {}",
                    current.display()
                );
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => return Err(error.into()),
        }
    }

    Ok(())
}
