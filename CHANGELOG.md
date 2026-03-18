# Changelog

All notable changes to this project will be documented in this file.

## [0.3.0] - 2026-03-18

### Added

- Added end-to-end BLAKE3 verification for network transfers when `--checksum` is enabled.
- Added protocol version validation during handshake to reject incompatible peers earlier.
- Added CLI parser and dispatch regression coverage for checksum, fsync, stdio, SSH, listener, threshold, and ignore handling.
- Added network regression tests for:
  - checksum mismatch rollback
  - interrupted updates preserving existing files
  - incompatible handshakes
  - sender-listener source refresh per client
  - multi-batch full-copy transfers
  - multi-batch requested-block transfers
- Added Podman SSH end-to-end coverage for pull and resume flows in `tests/podman/`.

### Changed

- Switched file replacement to staged atomic writes so existing destinations are preserved until commit.
- Delayed final commit for checksum-verified network transfers until the receiver confirms the staged file hash.
- Refreshed sender-listener task discovery per client connection instead of reusing a stale startup snapshot.
- Updated progress bar rendering with denser block characters and a smoother spinner.
- Clarified `--checksum` help text to describe network verification behavior.

### Fixed

- Fixed partial-transfer cleanup so failed updates no longer remove a previously valid destination file.
- Fixed checksum mismatch handling so invalid staged data is discarded instead of being left committed on disk.
- Fixed receiver-side handling of oversized destinations so delta and resume flows truncate safely.
- Fixed protocol startup handling so transfer messages before handshake are rejected.

### Performance

- Reduced staged delta-write overhead by attempting filesystem clone/reflink before falling back to a byte copy.
- Reduced local incremental sync scan cost by using mmap-backed slice comparison when available.
- Reduced sender-side remote transfer overhead by reusing a single opened source file and mmap context across full-copy, hash, and block-request responses.
- Bounded local directory sync in-flight work to avoid unbounded task buildup on very large trees.

### Verification

- `cargo fmt --all`
- `cargo test -q`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo build --release`
- `./tests/podman/test_ssh_pull.sh`
- `./tests/podman/test_ssh_pull_resume.sh`

### Benchmarks

- Local file benchmark (`./benchmark.sh`):
  - 1 GiB full copy: `pxs` about `0.21s`, `rsync` about `0.64s`
  - 1 GiB incremental with one 64 KiB change: `pxs` about `0.53s`, `rsync` about `1.05s`
  - no-change metadata pass: `pxs` about `0.003s`, `rsync` about `0.046s`
- Local checksum benchmark (`env PXS=./target/release/pxs ./local_pxs_vs_rsync.sh`):
  - 100 MiB aligned overwrite: `pxs` about `0.04s`, `rsync` about `0.13s`
  - 100 MiB prepend-byte shift: `pxs` about `0.09s`, `rsync` about `1.04s`
- Local loopback TCP smoke benchmark:
  - 256 MiB full-copy push: about `372 ms`
  - 64 KiB delta push: about `80 ms`
