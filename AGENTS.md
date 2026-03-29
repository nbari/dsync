# Repository Guidelines

## Project Structure & Module Organization
- `src/bin/pxs.rs`: CLI entrypoint.
- `src/cli/`: argument parsing, command dispatch, startup, and telemetry.
- `src/pxs/`: sync engine root module.
- `src/pxs/sync/`: local sync logic split by responsibility:
  - `mod.rs` (public sync API and shared sync types),
  - `dir.rs` (directory traversal and recursive sync),
  - `file.rs` (block-level file synchronization),
  - `meta.rs` (metadata application and hashing helpers),
  - `delete.rs` (deletion handling).
- `src/pxs/net/`: network protocol and remote transfer logic:
  - `mod.rs` (public net API and exports),
  - `protocol.rs` (wire message definitions and serialization),
  - `codec.rs` (transport framing),
  - `sender.rs` / `receiver.rs` (transfer endpoints),
  - `tasks.rs`, `shared.rs`, `path.rs` (supporting network utilities).
- `src/pxs/tools.rs`: shared helpers/utilities used across sync and net.
- `tests/`: integration-style tests (`sync_test.rs`, `net_test.rs`).
- `.github/workflows/`: CI for tests and deploy.
- Benchmark helpers: `benchmark.sh`, `local_pxs_vs_rsync.sh`, `remote_pxs_vs_rsync.sh`.

## Build, Test, and Development Commands
- `cargo build --release`: build optimized binary (`target/release/pxs`).
- `cargo test`: run all tests.
- `cargo clippy --all-targets --all-features`: run strict lint checks.
- `cargo fmt --all`: format code.
- `just test`: run clippy + tests via `.justfile`.
- `./local_pxs_vs_rsync.sh`: local rsync vs pxs benchmark.
- `./remote_pxs_vs_rsync.sh --source <PATH> --host <USER@HOST> --remote-root <PATH>`: remote benchmark.

## Project Objective
- Keep the public story consistent: `pxs` is an integrity-first sync/clone tool for large mutable datasets, designed to outperform `rsync` in its target workloads.
- Do not position `pxs` as a general `rsync` replacement or imply full `rsync -aHAXx` parity unless that is actually implemented.
- Keep `README.md`, `Cargo.toml` description, and CLI `about` text aligned when changing public wording.
- Keep the public sync order consistent: `pxs sync SRC DST`.
- Keep the main README operator-focused; move protocol and contributor design detail to `docs/design.md`.

## Coding Style & Naming Conventions
- All production code should have documentation. Document public modules, types, functions, constants, and any non-obvious internal logic that would otherwise be hard to maintain safely.
- Rust 2024 edition; defaults to `rustfmt`.
- Clippy is strict (`all` + `pedantic` deny). Avoid `unwrap`, `expect`, and panics; prefer `?` and typed errors.
- When using `anyhow`, import `anyhow::Result` (and `Context` when needed) at the top of the file and use plain `Result<T>` in signatures instead of fully-qualified `anyhow::Result<T>`.
- Do not add `#[allow(...)]` in production code; only acceptable inside test modules when needed.
- File/module names `snake_case`; types `UpperCamelCase`; constants `SCREAMING_SNAKE_CASE`.
- Group imports from the same crate/namespace (for example, `use std::{...};`) rather than many single-line imports.
- Keep functions small; prefer explicit structs over loose maps; use builder-style constructors for configs where appropriate.
- Keep modules focused: local sync flow in `src/pxs/sync/`, protocol and transport flow in `src/pxs/net/`, and reusable helpers in `src/pxs/tools.rs`.

## Testing Guidelines
- Prefer integration tests in `tests/` with descriptive `test_*` names.
- Use `#[tokio::test]` for async/network behavior.
- Add regression tests for protocol, threshold, checksum, and metadata edge cases when changing sync logic.
- For performance-sensitive changes, include benchmark output from relevant scripts in PR notes.

## Commit & Pull Request Guidelines
- Recent history includes short messages (for example, `sync`, `pre 0.1.0`, `still very slow`).
- For new work, use concise imperative subjects with intent, e.g.:
  - `net: reuse connection per worker`
  - `tools: move block hash helpers`
- PRs should include:
  - what changed and why,
  - risk/behavior impact (especially data correctness),
  - commands run (`fmt`, `clippy`, `test`),
  - benchmark evidence for performance claims.

## AI Agent Workflow
To ensure consistency and quality, all AI agents MUST follow these steps before concluding any task involving code changes:
1. **Format Code**: Run `cargo fmt --all`.
2. **Lint Code**: Run `cargo clippy --all-targets --all-features -- -D warnings`.
3. **Verify Tests**: Run `cargo test --all-features`.
4. **Self-Review**: Perform a final scan of the changes for any obvious errors or deviations from these guidelines.
