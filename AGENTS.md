# Repository Guidelines

## Project Structure & Module Organization
- `src/bin/pxs.rs`: CLI entrypoint.
- `src/cli/`: argument parsing, command dispatch, startup, and telemetry.
- `src/pxs/`: sync engine modules:
  - `sync.rs` (local block sync),
  - `net.rs` (protocol + remote transfer),
  - `tools.rs` (shared helpers/utilities).
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

## Coding Style & Naming Conventions
- All production code should have documentation. Document public modules, types, functions, constants, and any non-obvious internal logic that would otherwise be hard to maintain safely.
- Rust 2024 edition; defaults to `rustfmt`.
- Clippy is strict (`all` + `pedantic` deny). Avoid `unwrap`, `expect`, and panics; prefer `?` and typed errors.
- Do not add `#[allow(...)]` in production code; only acceptable inside test modules when needed.
- File/module names `snake_case`; types `UpperCamelCase`; constants `SCREAMING_SNAKE_CASE`.
- Group imports from the same crate/namespace (for example, `use std::{...};`) rather than many single-line imports.
- Keep functions small; prefer explicit structs over loose maps; use builder-style constructors for configs where appropriate.
- Keep modules focused: protocol flow in `net.rs`, reusable logic in `tools.rs`.

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
