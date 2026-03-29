use crate::pxs::tools;
use anyhow::Result;
use std::{
    ffi::{OsStr, OsString},
    os::unix::ffi::{OsStrExt, OsStringExt},
    path::{Path, PathBuf},
};

pub(crate) fn display_protocol_path(path: &[u8]) -> String {
    String::from_utf8_lossy(path).into_owned()
}

fn protocol_component_is_invalid(component: &[u8]) -> bool {
    component.is_empty() || component == b"." || component == b".."
}

pub(crate) fn validate_protocol_path(path: &[u8]) -> Result<()> {
    anyhow::ensure!(!path.is_empty(), "protocol path must not be empty");
    anyhow::ensure!(
        !path.starts_with(b"/"),
        "protocol path must be relative: {}",
        display_protocol_path(path)
    );
    anyhow::ensure!(
        !path.contains(&b'\\'),
        "protocol path must use '/' separators only: {}",
        display_protocol_path(path)
    );
    anyhow::ensure!(
        !path.contains(&b'\0'),
        "protocol path must not contain NUL bytes: {}",
        display_protocol_path(path)
    );

    for component in path.split(|byte| *byte == b'/') {
        anyhow::ensure!(
            !protocol_component_is_invalid(component),
            "protocol path must not contain empty components, '.' or '..': {}",
            display_protocol_path(path)
        );
    }

    Ok(())
}

pub(crate) fn resolve_protocol_path(root: &Path, path: &[u8]) -> Result<PathBuf> {
    validate_protocol_path(path)?;

    let mut full_path = root.to_path_buf();
    for component in path.split(|byte| *byte == b'/') {
        full_path.push(OsStr::from_bytes(component));
    }
    tools::ensure_no_symlink_ancestors_under_root(root, &full_path)?;
    Ok(full_path)
}

pub(crate) fn resolve_requested_root(root: &Path, requested: &[u8]) -> Result<PathBuf> {
    let relative = requested.strip_prefix(b"/").unwrap_or(requested);
    validate_protocol_path(relative)?;
    resolve_protocol_path(root, relative)
}

pub(crate) fn ensure_expected_protocol_path(expected: &[u8], received: &[u8]) -> Result<()> {
    validate_protocol_path(received)?;
    anyhow::ensure!(
        expected == received,
        "protocol path mismatch: expected {}, got {}",
        display_protocol_path(expected),
        display_protocol_path(received)
    );
    Ok(())
}

pub(crate) fn protocol_path_depth(path: &[u8]) -> usize {
    path.split(|byte| *byte == b'/').count()
}

pub(crate) fn relative_protocol_path(src_root: &Path, path: &Path) -> Result<Vec<u8>> {
    let rel_path = if src_root.is_file() {
        path.file_name()
            .map(OsStrExt::as_bytes)
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow::anyhow!("source file has no name: {}", path.display()))?
    } else {
        let mut bytes = Vec::new();
        for (index, component) in path.strip_prefix(src_root)?.components().enumerate() {
            if index > 0 {
                bytes.push(b'/');
            }
            bytes.extend_from_slice(component.as_os_str().as_bytes());
        }
        bytes
    };
    validate_protocol_path(&rel_path)?;
    Ok(rel_path)
}

pub(crate) fn protocol_path_to_pathbuf(path: &[u8]) -> Result<PathBuf> {
    validate_protocol_path(path)?;
    let mut out = PathBuf::new();
    for component in path.split(|byte| *byte == b'/') {
        out.push(OsString::from_vec(component.to_vec()));
    }
    Ok(out)
}
