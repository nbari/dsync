# dsync

A high-performance, concurrent file synchronization tool written in Rust. Designed to saturate high-speed networks (10GbE+) and utilize multi-core CPUs for extremely fast data transfers and incremental updates.

`dsync` is specifically optimized for massive data moves (e.g., 4TB+ Postgres `PG_DATA`) where standard tools like `rsync` often bottleneck on single-threaded hashing or SSH encryption overhead.

## Key Features

*   **Multi-threaded Engine**: Parallelizes file walking, block-level hashing, and I/O operations.
*   **Fixed-Block Synchronization**: Uses 64KB chunks and **XxHash64** for ultra-fast delta analysis.
*   **Zero-Copy Networking**: High-speed network protocol using **rkyv** serialization over raw TCP.
*   **Auto-SSH Mode**: Seamlessly tunnels through SSH for secure transfers without manual port forwarding.
*   **In-place Updates**: Modifies existing files directly without creating large temporary files.
*   **Smart Skipping**: Automatically skips unchanged files based on size and modification time.

## Installation

```bash
cargo build --release
```
The binary will be available at `./target/release/dsync`.

> [!IMPORTANT]
> For **Network** or **SSH** synchronization, `dsync` must be installed and available in the `$PATH` on **both** the source and destination servers.

## Usage

### 1. Local Synchronization
Synchronize files or directories on the same machine:
```bash
dsync --source /path/to/source --destination /path/to/dest
```

### 2. Network Synchronization (Direct TCP)
Best for high-speed local networks where maximum performance is needed.

**Receiver (Server 2):**
```bash
dsync --listen 0.0.0.0:8080 --destination /new/data
```

**Sender (Server 1):**
```bash
dsync --remote 192.168.1.10:8080 --source /old/data
```

### 3. Secure Network Synchronization (SSH)
Easiest way to sync securely between servers without opening extra firewall ports. `dsync` automatically spawns an SSH tunnel and executes itself on the remote side.

**Automatic SSH (recommended):**
```bash
dsync --source /path/to/old/pgdata --remote user@remote-server:/path/to/new/pgdata
```

**Manual SSH (using stdio pipe):**
If you need custom SSH flags, you can use the `--stdio` mode manually:
```bash
ssh user@remote-server "dsync --stdio --destination /path/to/new/data" < <(dsync --remote - --source /path/to/old/data)
```

### 4. Advanced Options

*   **`--checksum` (-c)**: Force a block-by-block hash comparison even if size/mtime match.
*   **`--threshold` (-t)**: (Default: 0.5) If the destination file is less than X% the size of the source, perform a full copy instead of hashing.
*   **`--dry-run` (-n)**: Show what would have been transferred without making any changes.
*   **`--verbose` (-v)**: Increase logging verbosity (use `-vv` for debug).

## Why dsync is faster than rsync

1.  **Parallelism**: `rsync` is largely single-threaded. `dsync` uses all available CPU cores to hash different parts of your data simultaneously.
2.  **Algorithm Efficiency**: For database files (like Postgres), data is modified in-place and never "shifted." `dsync` uses a fixed-block algorithm that is much lighter than `rsync`'s rolling hash.
3.  **No Encryption Bottleneck**: By using raw TCP (when appropriate), `dsync` avoids the CPU overhead of SSH encryption which often caps transfers at ~120MB/s.

## Tests

The project includes a robust test suite for both local and network logic:
```bash
# Run all tests
cargo test
```

## License

BSD-3-Clause
