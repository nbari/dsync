use crate::pxs::tools;
use std::path::{Path, PathBuf};

pub(crate) fn validate_protocol_path(path: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!path.is_empty(), "protocol path must not be empty");
    anyhow::ensure!(
        !path.starts_with('/'),
        "protocol path must be relative: {path}"
    );
    anyhow::ensure!(
        !path.contains('\\'),
        "protocol path must use '/' separators only: {path}"
    );

    for component in path.split('/') {
        anyhow::ensure!(
            !component.is_empty(),
            "protocol path must not contain empty components: {path}"
        );
        anyhow::ensure!(
            component != "." && component != "..",
            "protocol path must not contain '.' or '..': {path}"
        );
    }

    Ok(())
}

pub(crate) fn resolve_protocol_path(root: &Path, path: &str) -> anyhow::Result<PathBuf> {
    validate_protocol_path(path)?;

    let mut full_path = root.to_path_buf();
    for component in path.split('/') {
        full_path.push(component);
    }
    tools::ensure_no_symlink_ancestors_under_root(root, &full_path)?;
    Ok(full_path)
}

pub(crate) fn ensure_expected_protocol_path(expected: &str, received: &str) -> anyhow::Result<()> {
    validate_protocol_path(received)?;
    anyhow::ensure!(
        expected == received,
        "protocol path mismatch: expected {expected}, got {received}"
    );
    Ok(())
}

pub(crate) fn relative_protocol_path(src_root: &Path, path: &Path) -> anyhow::Result<String> {
    let rel_path = if src_root.is_file() {
        path.file_name()
            .map(|name| name.to_string_lossy().to_string())
            .ok_or_else(|| anyhow::anyhow!("source file has no name: {}", path.display()))?
    } else {
        path.strip_prefix(src_root)
            .unwrap_or(path)
            .components()
            .map(|component| component.as_os_str().to_string_lossy().to_string())
            .collect::<Vec<_>>()
            .join("/")
    };
    validate_protocol_path(&rel_path)?;
    Ok(rel_path)
}
