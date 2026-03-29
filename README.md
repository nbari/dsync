# pxs

[![Test](https://github.com/nbari/pxs/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/nbari/pxs/actions/workflows/test.yml)
[![Build](https://github.com/nbari/pxs/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/nbari/pxs/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/nbari/pxs/branch/main/graph/badge.svg)](https://codecov.io/gh/nbari/pxs)

**pxs** (Parallel X-Sync) is an integrity-first sync and clone tool for large
mutable datasets. It keeps existing copies accurate across local paths, SSH,
and raw TCP, and it is designed to outperform `rsync` in the workloads it
targets.

The project is aimed at repeated refreshes of datasets such as:

- PostgreSQL `PGDATA`
- VM images
- large directory trees with many unchanged files
- large files that are usually modified in place instead of shifted

`pxs` is not a drop-in replacement for `rsync`. Its goal is narrower: exact and
safe synchronization first, then speed through Rust parallelism, fixed-block
delta sync, and transport choices that fit modern large-data workloads.

> [!NOTE]
> `0.6.2` hardens local directory sync against transient source entries that
> disappear mid-run and tightens the PostgreSQL helper defaults for
> destination-specific config and transient runtime files.

## What `pxs` Is For

Use `pxs` when you need:

- exact refreshes of an existing copy
- safe replacement behavior instead of in-place corruption risk
- repeated large-data sync where many files or blocks stay unchanged
- local, SSH, or raw TCP sync with one public command shape
- a tool that favors large mutable dataset workflows over general archive parity

`pxs` is a good fit when the destination already exists and you want to keep it
accurate with as little rewrite work as possible.

## What `pxs` Is Not

`pxs` is intentionally not trying to be all of `rsync`.

- It is not a full `rsync -aHAXx` replacement.
- It does not currently promise hardlink, ACL, xattr, SELinux-label, or sparse
  layout parity.
- It does not target Windows.
- Raw TCP is for trusted networks and private links, not hostile networks.

If you need universal protocol compatibility or broad filesystem metadata
parity, `rsync` is still the reference point.

## Installation

Install from crates.io:

```bash
cargo install pxs
```

Build from source:

```bash
cargo build --release
```

The binary will be available at `./target/release/pxs`.

> [!IMPORTANT]
> For SSH or raw TCP sync, `pxs` must be installed and available in `$PATH` on
> both sides.

## Command Model

The public sync model is:

```bash
pxs sync SRC DST
```

The first operand is always the source. The second operand is always the
destination.

`DEST` and `SRC` can be:

- local filesystem paths
- SSH endpoints like `user@host:/path`
- raw TCP endpoints like `host:port/path`

### Local Examples

```bash
# File -> file
pxs sync snapshot/base.tar.zst backup/base.tar.zst

# Directory -> directory
pxs sync /var/lib/postgresql/data /srv/restore/pgdata

# Exact mirror with checksum validation and delete
pxs sync /var/lib/postgresql/data /srv/restore/pgdata --checksum --delete
```

### SSH Examples

```bash
# Remote file -> local file
pxs sync user@db2:/srv/export/snapshot.bin ./snapshot.bin

# Local file -> remote file
pxs sync ./snapshot.bin user@db2:/srv/incoming/snapshot.bin

# Remote directory -> local directory
pxs sync user@db2:/srv/export/pgdata /srv/restore/pgdata

# Local directory -> remote directory
pxs sync /var/lib/postgresql/data user@db2:/srv/incoming/pgdata
```

> [!IMPORTANT]
> SSH sync is designed for non-interactive authentication. In practice that
> means SSH keys, `ssh-agent`, or an already-established multiplexed session.

### Raw TCP Examples

Raw TCP uses `pxs sync` for the client side and `pxs listen` or `pxs serve` on
the remote side.

```bash
# Remote file -> local file
pxs sync 192.168.1.10:8080/snapshots/base.tar.zst ./snapshot.bin

# Local file -> remote file
pxs sync ./snapshot.bin 192.168.1.10:8080/incoming/snapshot.bin

# Remote directory -> local directory
pxs sync 192.168.1.10:8080/pgdata /srv/restore/pgdata

# Local directory -> remote directory
pxs sync /var/lib/postgresql/data 192.168.1.10:8080/incoming/pgdata
```

## Raw TCP Setup

Use `listen` on the receiving host to expose an allowed destination root:

```bash
pxs listen 0.0.0.0:8080 /srv
pxs listen 0.0.0.0:8080 /srv --fsync
```

Then sync into a path beneath that root:

```bash
pxs sync ./snapshot.bin 192.168.1.10:8080/incoming/snapshot.bin
pxs sync /var/lib/postgresql/data 192.168.1.10:8080/incoming/pgdata --delete
```

Use `serve` on the source host to expose an allowed source root:

```bash
pxs serve 0.0.0.0:8080 /srv/export
pxs serve 0.0.0.0:8080 /srv/export --checksum
```

Then sync from a path beneath that root:

```bash
pxs sync 192.168.1.10:8080/snapshots/base.tar.zst ./snapshot.bin
pxs sync 192.168.1.10:8080/pgdata /srv/restore/pgdata --checksum
```

The `host:port/path` suffix selects what to read or write inside the configured
root. Client-side per-sync flags such as `--checksum`, `--threshold`,
`--delete`, and `--ignore` travel with the `pxs sync` request.

For SSH and raw TCP, `pxs` negotiates block compression automatically. It uses
`zstd` when both peers support it and otherwise uses the existing uncompressed
path.

## Guarantees and Safety Model

`pxs` is built around exactness first.

What it aims to preserve today:

- exact file contents
- exact file, directory, and symlink shape
- exact names, including valid Unix non-UTF8 names over local, SSH, and raw TCP
- file mode and modification time
- ownership when privileges and platform support allow it
- staged replacement so an existing destination is kept until the new object is
  ready to commit

Safety rules that are intentionally strict:

- destination roots and destination parent path components must be real
  directories, not symlinks
- leaf symlinks inside the destination tree are treated as entries and may be
  replaced or removed without following their targets
- raw TCP requested paths are resolved beneath the configured `listen` or
  `serve` root
- raw TCP and SSH protocol paths reject absolute paths, traversal components,
  and unsupported path forms

Durability and verification options:

- `--checksum` forces block comparison and enables end-to-end BLAKE3
  verification for network sync
- `--delete` removes destination entries that are not present in the source
  tree for directory sync
- `--fsync` flushes committed file writes, deletes, directory installs, symlink
  installs, and final metadata before success is reported

> [!NOTE]
> Without `--checksum`, `pxs` skips unchanged files by size and modification
> time. Keep clocks synchronized between hosts if you rely on mtime-based skip
> decisions.

## Performance Model

`pxs` tries to win where its model fits the workload.

- It hashes and compares blocks in parallel.
- It walks directory trees concurrently.
- It uses fixed 128 KiB blocks, which works well for many in-place update
  workloads.
- It can compress outbound network block batches with negotiated `zstd`.
- It can keep multiple small outbound network files in flight on the main
  control session.
- It can avoid SSH overhead entirely on trusted networks via raw TCP.
- It can fan out large outbound network transfers with multiple worker
  sessions.

This is workload-dependent. `pxs` should be described as:

- faster than `rsync` in its target workloads
- not universally faster than `rsync`
- optimized for repeated large-data refreshes on modern hardware

### Threshold Behavior

`--threshold` controls when `pxs` should give up on reuse and rewrite a file
fully.

- Default: `0.1`
- Meaning: if `destination_size / source_size` is below the threshold, do a
  full copy instead of block reuse

That default is intentionally low so interrupted or partially existing files can
still benefit from delta sync and resume-like reuse.

## Benchmarking

This repository includes workload-specific comparison helpers:

- `./benchmark.sh`
- `./local_pxs_vs_rsync.sh`
- `./remote_pxs_vs_rsync.sh --source <PATH> --host <USER@HOST> --remote-root <PATH>`

Use them as targeted evidence, not as universal performance proof. When
benchmarking:

- compare equivalent integrity settings
- keep transport choices explicit
- describe the workload shape
- report both speed and correctness results

## Common Options

- `--checksum`, `-c`: force block comparison and end-to-end network verification
- `--delete`: remove extraneous destination entries during directory sync
- `--fsync`, `-f`: durably flush committed data and metadata before completion
- `--ignore`, `-i`: repeatable glob-based ignore pattern
- `--exclude-from`, `-E`: read ignore patterns from a file
- `--threshold`, `-t`: reuse threshold for block-based sync, default `0.1`
- `--dry-run`, `-n`: show what would change without mutating the destination
- `--large-file-parallel-threshold`: send outbound network files at or above
  this size with chunk-parallel workers
- `--large-file-parallel-workers`: set the worker count for those large outbound
  network transfers
- `--network-file-concurrency`: keep multiple smaller outbound network files in
  flight on the main control session

If `--large-file-parallel-workers` or `--network-file-concurrency` is omitted,
`pxs` chooses a conservative default from available CPU cores. That is a
starting point, not a guarantee that the link can sustain it. On bandwidth-poor,
high-latency, or otherwise congested networks, lower values can perform better
and reduce saturation.

## PostgreSQL Helper

This repository includes [`sync.sh`](./sync.sh), a PostgreSQL-oriented helper
for repeated SSH `pxs sync SRC DST` passes around `pg_backup_start()` and
`pg_backup_stop()`.

It is useful when evaluating `pxs` on the workload it was originally built to
care about most: repeated `PGDATA` refreshes.

`sync.sh` also accepts optional outbound SSH tuning overrides through
environment variables: `PXS_NETWORK_FILE_CONCURRENCY`,
`PXS_LARGE_FILE_PARALLEL_THRESHOLD`, and
`PXS_LARGE_FILE_PARALLEL_WORKERS`. Leave them unset to keep `pxs` defaults, and
reduce them on bandwidth-poor or high-latency links if the default fan-out
saturates the network.

The helper also keeps destination-specific PostgreSQL config/control files and
transient runtime state out of the repeated sync passes by default, including
`postgresql.conf`, `postgresql.auto.conf`, `pg_hba.conf`, `pg_ident.conf`,
`postmaster.pid`, `postmaster.opts`, `current_logfiles`, `pg_stat_tmp`,
`pg_stat_tmp/**`, `pg_dynshmem`, `pg_dynshmem/**`, and `**/pg_internal.init`.
Add newline-delimited extra ignore patterns through `PXS_EXTRA_IGNORE_PATTERNS`
when a deployment has additional host-local files that should not be mirrored.

## Design Notes

The front page is intentionally operator-focused. Protocol flow, transport
design, and internal architecture notes live in [docs/design.md](./docs/design.md).

## Testing

Run the core validation flow:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

Podman end-to-end suites are also available for SSH and raw TCP under
`tests/podman/`.

## License

BSD-3-Clause
