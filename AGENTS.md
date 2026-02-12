# Repository Guidelines

## Project Structure & Module Organization
- `src/bin/dsync.rs`: CLI entrypoint.
- `src/cli/`: argument parsing, command dispatch, startup, and telemetry.
- `src/dsync/`: sync engine modules:
  - `sync.rs` (local block sync),
  - `net.rs` (protocol + remote transfer),
  - `tools.rs` (shared helpers/utilities).
- `tests/`: integration-style tests (`sync_test.rs`, `net_test.rs`).
- `.github/workflows/`: CI for tests and deploy.
- Benchmark helpers: `benchmark.sh`, `local_dsync_vs_rsync.sh`, `remote_dsync_vs_rsync.sh`.

## Build, Test, and Development Commands
- `cargo build --release`: build optimized binary (`target/release/dsync`).
- `cargo test`: run all tests.
- `cargo clippy --all-targets --all-features`: run strict lint checks.
- `cargo fmt --all`: format code.
- `just test`: run clippy + tests via `.justfile`.
- `./local_dsync_vs_rsync.sh`: local rsync vs dsync benchmark.
- `./remote_dsync_vs_rsync.sh --source <PATH> --host <USER@HOST> --remote-root <PATH>`: remote benchmark.

## Coding Style & Naming Conventions
- Language: Rust (edition 2024), standard `rustfmt` formatting (4-space indentation).
- Follow idiomatic Rust naming:
  - `snake_case` for functions/modules/variables,
  - `PascalCase` for structs/enums,
  - `SCREAMING_SNAKE_CASE` for constants.
- Lints are strict (`warnings = deny`, Clippy `all` + `pedantic` denied in `Cargo.toml`); avoid `unwrap`, `expect`, and unchecked indexing.
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
