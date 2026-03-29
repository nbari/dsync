# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- Added negotiated block compression for outbound SSH and raw TCP transfers, preferring `zstd` and falling back to `lz4` before the existing uncompressed path.
- Added `--network-file-concurrency` to bound how many smaller outbound network file transfers can stay in flight on the main control session.
- Added regression coverage for negotiated `zstd` chunk-writer batches and bounded small-file network concurrency on the sender control path.

### Changed

- Clarified the public project objective across the README, crate metadata, CLI help text, and benchmark script wording so `pxs` is consistently described as an integrity-first sync/clone tool for large mutable datasets rather than a general `rsync` replacement.
- Reworked the README into an operator-focused guide and moved protocol and architecture notes into a dedicated contributor-facing design document.
- Restored the public `sync` operand order to `pxs sync SRC DST` across local, SSH, and raw TCP flows so the command again follows source-first semantics instead of the newer destination-first model.
- Reworked outbound network scheduling so smaller files can share the control session concurrently, while files at or above `--large-file-parallel-threshold` drain in-flight small work and continue to use chunk-parallel worker transfers.
- Extended `--large-file-parallel-threshold` from a large-file worker toggle into the scheduler boundary between small-file control-session fan-out and large-file chunk-worker mode for outbound network sync.
- Kept SSH transport compression disabled so protocol-level `zstd`/`lz4` remains the only compression layer by default.

## [0.6.0] - 2026-03-27

### Added

- Added broader transport validation across the public sync surface, including local CLI smoke tests, raw TCP delete regressions, Podman SSH/raw TCP suite runners, and Linux CI jobs for the SSH and raw TCP end-to-end suites.
- Added dedicated SSH end-to-end coverage for `--delete`, `--fsync`, ignore forwarding, exact-file syncs, and large-file parallel push worker fan-out.
- Added dedicated raw TCP end-to-end coverage for directory pull with `--delete`, `--checksum`, and ignore handling.

### Changed

- Standardized the public interface around `pxs sync DEST SRC` for local, SSH, and raw TCP transfers. `push` and `pull` remain available as hidden compatibility aliases.
- Extended raw TCP endpoint syntax to `host:port/path` so the client can address an exact file or directory path within a configured `listen` or `serve` root.
- Reworked the README, Podman helpers, benchmark helpers, and PostgreSQL migration script examples around the unified `sync DEST SRC` model.
- Split the shared tools implementation into focused `src/pxs/tools/` modules and standardized `anyhow::Result` style across the codebase and tests by importing `Result` and using plain `Result<T>` in signatures.

### Fixed

- Fixed remote exact-path sync handling so explicit single-file targets are honored instead of being treated like directory roots during remote sync sessions.
- Fixed the macOS release build by making staging probe no-op hooks available on non-test non-Linux targets instead of gating them away at compile time.
- Fixed release documentation drift around raw TCP sync flags so the README now reflects current client-side `--checksum`, `--threshold`, `--delete`, and `--ignore` behavior.

## [0.5.1] - 2026-03-26

### Added

- Added raw TCP push chunk-parallel large-file transfer using receiver-issued transfer ids and attached worker connections, bringing single-large-file raw TCP behavior closer to SSH push and local sync.
- Added regression coverage for zero-byte remote file synchronization, raw TCP chunk-writer attachment errors, raw TCP large-file multi-connection transfer, and TCP parsing of the large-file parallel CLI flags.

### Changed

- Extended `--large-file-parallel-threshold` and `--large-file-parallel-workers` so they apply to raw TCP push as well as SSH push.
- Generalized the receiver chunk-writer path so SSH stdio workers and raw TCP attached workers share the same staged-file validation and write flow.

### Fixed

- Fixed a panic in `compute_requested_blocks()` when a remote sender reported an empty block-hash list for a zero-byte file.
- Fixed raw TCP and SSH push handling of zero-byte files so an existing non-empty destination is truncated cleanly instead of crashing during hash comparison.

## [0.5.0] - 2026-03-26

### Added

- Added SSH push chunk-parallel large-file transfer with automatic activation above `--large-file-parallel-threshold` and conservative worker defaults from available CPU cores.
- Added new `push` flags `--large-file-parallel-threshold <SIZE>` and `--large-file-parallel-workers <N>` for tuning large-file SSH worker fan-out.
- Added regression coverage for the new parallel receiver path, including transfer-id negotiation, parallel block/full-copy requests, chunk-writer path validation, and non-UTF8 staged-path handling.

### Changed

- Reworked the internal SSH large-file worker flow to use receiver-issued transfer ids instead of exposing staged filesystem paths back to the sender.
- Hardened hidden `--chunk-writer` execution so worker sessions resolve receiver-owned transfer records under the destination root before writing any block data.
- Kept raw TCP behavior unchanged; the new large-file parallelism remains scoped to SSH push.

### Fixed

- Fixed a write-scope escape in the new SSH chunk-writer path where a caller-supplied staged path could previously target arbitrary writable files.
- Fixed lossy staged-path transport in the SSH large-file worker flow so non-UTF8 staged temp paths no longer break chunk-writer transfers.
- Fixed parallel-transfer cleanup so abandoned or failed receiver-side staged files remove their transfer records before the session exits.

## [0.4.2] - 2026-03-24

### Fixed

- **Critical Security Fix**: Fixed a symlink traversal vulnerability in the network receiver where a malicious sender could cause the receiver to follow a symlink and truncate an out-of-scope file during metadata application.
- Hardened destination path validation so local sync and the network receiver now reject symlinked destination roots and symlinked parent path components instead of writing through them.
- Fixed a crash during synchronization when an existing broken symlink was present at the destination path.
- Fixed a durability issue where the `--fsync` flag was ignored when pushing files over an SSH connection; the flag is now correctly forwarded to the remote receiver, and raw TCP push now propagates the same durability setting to the receiver.
- Fixed SSH remote sync so `push --delete` and `pull --delete` now converge the destination tree by deleting extraneous entries after an explicit end-of-sync acknowledgment instead of leaving stale files behind.
- Fixed receiver-side completion handling so raw TCP push reports non-file control-path failures and rejects premature `SyncComplete` messages while transfer state is still pending.
- Fixed checksum-state handling so `VerifyChecksum` is only accepted for an active transfer, and checksum negotiation now falls back safely if the destination changes during `RequestHashes` to `BlockHashes`.
- Updated `safe_mmap` documentation to accurately reflect that while `MAP_PRIVATE` protects against in-place data modifications, it does not prevent `SIGBUS` if the underlying file is externally truncated.

### Changed

- Optimized `--checksum` mode for network transfers: the receiver now avoids creating a temporary staging file and cloning data if the destination file already matches the source size and BLAKE3 hash.
- Hardened replacement semantics for file, directory, and symlink transitions so conflicting destinations are installed with rollback-safe helpers instead of delete-then-create behavior.
- Expanded `--fsync` durability coverage beyond file writes so committed directory installs, symlink installs, and final directory metadata application are durably synchronized before completion is acknowledged.
- Hardened the public single-file sync API so direct library callers receive the same symlink-ancestor destination protection as the CLI.

### Notes

- SSH remote mirror mode now supports `--delete` for `push` and `pull`, with receiver-side cleanup completed before success is reported.
- Raw TCP and public stdio flows still reject remote `--delete`.
- Local `sync --delete` safely removes extra entries, including leaf symlinks, without following their targets. Its deletions are not yet crash-durable under `--fsync`; only committed create/replace paths, SSH remote delete finalization, and final metadata application currently receive durability syncing.
- The bundled `sync.sh` PGDATA migration workflow now uses repeated SSH `push` passes plus `pg_backup_start()` / `pg_backup_stop()`, and it currently defaults to speed-first transfers without `--fsync`.
- PostgreSQL tablespaces under `pg_tblspc` are preserved as symlinks; the destination host must already provide valid tablespace targets for that workflow.
- `pxs` still does not provide full `rsync -aHAXx` parity for the bundled PGDATA script. Hard-link preservation remains the documented parity gap.

## [0.4.1] - 2026-03-20

### Added

- Added `--delete` flag to the `sync` subcommand to remove extraneous files from destination directories that do not exist in the source.
- Added regression tests for directory-vs-file structural mismatches and `--delete` functionality in `tests/sync_test.rs`.
- Added negotiated LZ4 compression for large, beneficial `ApplyBlocks` batches on raw TCP transports, with fallback to the existing uncompressed path for older peers and incompressible payloads.
- Added CLI subprocess regression coverage for malformed endpoints, unsupported stdio pull, raw TCP source-side flag rejection, missing source paths, and incompatible peer handshake reporting.
- Added Podman SSH push end-to-end coverage and strengthened the existing SSH pull script to cover spaced remote paths plus `--checksum` and `--ignore` forwarding.

### Changed

- Standardized the default TCP port in examples and help messages to `8080` (reverting from `7878`) to prioritize firewall compatibility in enterprise and data center environments.
- Enhanced runtime error reporting by adding `anyhow::Context` to critical filesystem, metadata, and IO operations across the sync engine and CLI dispatch.
- Refactored `src/pxs/sync.rs` into a structured module directory `src/pxs/sync/` with separate components for directory traversal, file synchronization, metadata application, and extraneous file deletion.
- Refactored internal synchronization functions to use a shared `SyncOptions` configuration struct to improve maintainability and resolve clippy argument-count warnings.
- Consolidated hardware-aware concurrency logic into a shared utility with safer fallback defaults (defaulting to single-threaded if CPU detection fails).
- Improved large file synchronization to use throttled task spawning, ensuring constant memory usage even for multi-terabyte files.
- Strengthened the network protocol review with regressions for malformed handshakes, invalid receiver-side messages, idle timeouts, codec resynchronization, and optimization-path coverage.
- Improved SSH transport safety by routing remote internal stdio helpers through `--quiet`, using shell-safe remote command construction, and centralizing SSH child-process cleanup.
- Tightened public endpoint parsing for bracketed IPv6 TCP/SSH endpoints and malformed bracketed syntax instead of falling through to later address-resolution failures.
- Expanded end-to-end coverage for manual `--stdio` transport, SSH pull/push flows, and the current Podman TCP/SSH transport matrix.

### Fixed

- Fixed a regression in SSH endpoint parsing where paths containing internal colons (e.g., `user@host:path:with:colons`) were incorrectly split; now accurately isolates the host and path using the first colon outside of brackets.
- Fixed directory synchronization bug where the sync would fail with "Not a directory" if a file already existed at the destination where a subdirectory needed to be created.
- Fixed symlink handling to correctly detect and replace broken symlinks at the destination.
- Fixed directory replacement logic to ensure real directories correctly replace symlinks-to-directories.
- Fixed security vulnerability where ownership preservation could follow symlinks; now uses `lchown` to safely apply ownership to the link itself.
- Improved directory traversal robustness by gracefully skipping unsupported file types (sockets, pipes) instead of returning an error.
- Fixed the public `sync` subcommand so directory sources can replace an existing file at the destination root, matching the already-correct library behavior.
- Fixed `pxs sync FILE DST --delete` to fail fast with a clear error instead of silently accepting a no-op flag combination.
- Fixed receiver-side network sync so destination files can be replaced by incoming directories, matching local sync behavior.
- Fixed network metadata application for symlinks to avoid following the symlink target when preserving ownership.
- Fixed malformed bracketed endpoints such as `[::1` so they now fail during CLI parsing with a clear error instead of being treated as raw TCP addresses.

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
