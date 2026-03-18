# Changelog

All notable changes to this project will be documented in this file.

## [0.4.0] - 2026-03-18

### Added

- Added direct TCP `serve` to `pull` Podman end-to-end coverage in `tests/podman/test_tcp_pull.sh`.

### Changed

- Reworked the public CLI around explicit subcommands: `sync`, `push`, `pull`, `listen`, and `serve`.
- Replaced the previous flat public mode flags such as `--source`, `--destination`, `--remote`, `--listen`, `--pull`, and `--sender` with subcommand-specific positional arguments and options.
- Reorganized the README usage guide around subcommand intent, including “use this when…” guidance and explicit raw TCP and SSH command pairings.
- Updated benchmark and Podman helper scripts to use the public subcommand syntax.

### Fixed

- Fixed raw TCP `pull` UX so source-side flags are rejected with a clear error that points users to configure `serve` instead.

### Verification

- `cargo test`
- `cargo clippy --all-targets --all-features`
- `./tests/podman/test_tcp_pull.sh`
- `./tests/podman/test_tcp_push.sh`
- `./tests/podman/test_tcp_directory_resume.sh`
- `./tests/podman/test_ssh_pull.sh`
- `./tests/podman/test_ssh_pull_resume.sh`

### Benchmarks

- Local checksum benchmark (`env PXS=./target/release/pxs ./local_pxs_vs_rsync.sh`):
  - 100 MiB aligned overwrite: `pxs` about `0.043s`, `rsync` about `0.136s`
  - 100 MiB prepend-byte shift: `pxs` about `0.051s`, `rsync` about `0.985s`

## [0.3.2] - 2026-03-18

### Changed

- Clarified local directory sync progress reporting to use a single aggregate progress bar across concurrently processed files instead of a separate per-file bar.

### Fixed

- Fixed local single-file sync so an existing destination directory keeps its directory type and receives the source file at `DEST/<filename>` instead of being replaced.

## [0.3.1] - 2026-03-18

### Changed

- Simplified build-time metadata generation and made the CLI long version show the git commit only when it is available.
- Refactored CLI command construction around a shared `base_command()` scaffold while keeping the option list inline in `new()`.
- Updated the progress bar theme to use a green spinner and green filled bar segments.

### Fixed

- Moved the generated `built.rs` doc-markdown allowance to the shared build info module so strict clippy checks pass without post-processing generated files.

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
