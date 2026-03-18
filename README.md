# pxs

**pxs** (Parallel X-Sync) is a high-performance, concurrent file
synchronization tool written in Rust. Designed to saturate high-speed networks
(10GbE+) and utilize multi-core CPUs for extremely fast data transfers and
incremental updates.

The name is intentionally short for CLI use: `pxs` stands for **Parallel X-Sync**.

`pxs` is specifically optimized for massive data moves (e.g., 4TB+ Postgres
`PG_DATA`) where standard tools like `rsync` often bottleneck on
single-threaded hashing or SSH encryption overhead.

`pxs` is not a drop-in replacement for `rsync`. The goal is narrower: transfer
large files and datasets faster by focusing on parallelism, fixed-block delta
sync, and high-throughput transport.

## Key Features

*   **Multi-threaded Engine**: Parallelizes file walking, block-level hashing, and I/O operations.
*   **Fixed-Block Synchronization**: Uses **128KB** chunks and **XxHash64** for ultra-fast delta analysis.
*   **Zero-Copy Networking**: High-speed network protocol using **rkyv** serialization over raw TCP.
*   **Auto-SSH Mode**: Seamlessly tunnels through SSH for secure transfers without manual port forwarding.
*   **Pull Mode**: Supports both pushing to and pulling from remote servers.
*   **In-place Updates**: Modifies existing files directly without creating large temporary files.
*   **Smart Skipping**: Automatically skips unchanged files based on size and modification time.

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
> For **Network** or **SSH** synchronization, `pxs` must be installed and available in the `$PATH` on **both** the source and destination servers.

> [!NOTE]
> **Clock Synchronization**: When using mtime-based skip detection (the default without `--checksum`), ensure source and destination systems have synchronized clocks (e.g., via NTP). Clock skew can cause files to be incorrectly skipped or unnecessarily re-synced. Use `--checksum` to force content-based comparison if clock sync is not guaranteed.

## Platform Support

`pxs` currently targets **Unix-like systems only**:

*   Linux
*   macOS
*   BSD

Windows is **not supported**.

For network and `--stdio` transports, `pxs` uses normalized relative POSIX paths in the protocol. Incoming paths are rejected if they are absolute or contain `.` / `..` traversal components. Paths containing `\` are also rejected by the protocol, so filenames with backslashes are not supported for remote sync.

## How It Works

### Local Synchronization

```mermaid
flowchart LR
    SRC[Source path] --> WALK[Parallel file walker]
    WALK --> HASH[Parallel block hasher]
    HASH --> COMPARE[Block comparator]
    COMPARE -->|Changed blocks only| WRITE[Parallel block writer]
    WRITE --> DST[Destination path]
```

Mermaid source: [`docs/diagrams/local-sync.mmd`](docs/diagrams/local-sync.mmd)
Fallback image: [`docs/diagrams/local-sync.svg`](docs/diagrams/local-sync.svg)

### Network Synchronization (Direct TCP)

```mermaid
sequenceDiagram
    participant S as Sender
    participant R as Receiver

    S->>R: Handshake
    R->>S: Handshake ACK

    loop For each file
        S->>R: SyncFile(path, metadata, size)
        alt Destination can delta sync
            R->>S: RequestHashes
            S->>R: BlockHashes
            R->>S: RequestBlocks(changed indexes)
            S->>R: ApplyBlocks(delta data)
        else Full copy required
            R->>S: RequestFullCopy
            S->>R: ApplyBlocks(all data)
        end
        S->>R: ApplyMetadata
        R->>S: MetadataApplied
    end
```

Mermaid source: [`docs/diagrams/direct-tcp.mmd`](docs/diagrams/direct-tcp.mmd)
Fallback image: [`docs/diagrams/direct-tcp.svg`](docs/diagrams/direct-tcp.svg)

### SSH Synchronization (Auto-Tunnel)

```mermaid
flowchart LR
    CLI[Local pxs CLI] -->|starts| SSH[SSH process]
    CLI <-->|pxs protocol over stdio| SSH
    SSH <-->|encrypted transport| REMOTE[Remote pxs --stdio]
    REMOTE --> DST[Destination path]
```

Mermaid source: [`docs/diagrams/ssh-flow.mmd`](docs/diagrams/ssh-flow.mmd)
Fallback image: [`docs/diagrams/ssh-flow.svg`](docs/diagrams/ssh-flow.svg)

### Delta Sync Algorithm

```mermaid
flowchart TD
    START([Start sync]) --> EXISTS{Destination exists?}
    EXISTS -->|No| FULL[Full copy]
    EXISTS -->|Yes| SIZE{Size matches?}
    SIZE -->|No| THRESH{Below threshold?}
    THRESH -->|Yes| FULL
    THRESH -->|No| DELTA[Delta sync]
    SIZE -->|Yes| MTIME{mtime matches and no checksum?}
    MTIME -->|Yes| SKIP[Skip file]
    MTIME -->|No| DELTA
    DELTA --> HASH[Hash source and destination blocks]
    HASH --> DIFF[Compare block hashes]
    DIFF --> APPLY[Transfer changed blocks only]
    FULL --> META[Apply metadata]
    APPLY --> META
    SKIP --> DONE([Done])
    META --> DONE
```

Mermaid source: [`docs/diagrams/delta-sync.mmd`](docs/diagrams/delta-sync.mmd)
Fallback image: [`docs/diagrams/delta-sync.svg`](docs/diagrams/delta-sync.svg)

## Usage

### 1. Local Synchronization
Synchronize a single file:
```bash
pxs --source file.bin --destination backup.bin
```

Synchronize a directory:
```bash
pxs --source /path/to/source_dir --destination /path/to/dest_dir
```

More local examples:

```bash
# Copy one local file
pxs --source ./pg_wal/000000010000000000000001 --destination /backup/000000010000000000000001

# Copy one local directory tree
pxs --source /var/lib/postgresql/data --destination /srv/replica/base-backup

# Force checksum-based verification
pxs --source ./dataset.bin --destination /mnt/backup/dataset.bin --checksum

# Flush file data to disk before completion
pxs --source ./dataset.bin --destination /mnt/backup/dataset.bin --fsync
```

### 2. Network Synchronization (Direct TCP)
Best for high-speed local networks where maximum performance is needed (no encryption overhead).

**A. Pushing to a Receiver (Remote is getting the file)**
*   **Receiver (Server 2):**
    ```bash
    pxs --listen 0.0.0.0:8080 --destination /new/data
    ```
*   **Sender (Server 1):**
    ```bash
    pxs --remote 192.168.1.10:8080 --source /old/data/file.bin
    ```

Concrete examples without SSH:

```bash
# Receiver on host B
pxs --listen 0.0.0.0:8080 --destination /srv/incoming

# Sender on host A: copy one file to host B
pxs --remote 192.168.1.10:8080 --source ./archive.tar

# Sender on host A: copy a directory tree to host B
pxs --remote 192.168.1.10:8080 --source /var/lib/postgresql/data
```

**B. Pulling from a Sender (You are getting the file)**
*   **Sender (Server 2):**
    ```bash
    pxs --listen 0.0.0.0:8080 --source /old/data/file.bin --sender
    ```
*   **Receiver (Server 1):**
    ```bash
    pxs --remote 192.168.1.10:8080 --destination ./local_copy.bin --pull
    ```

Concrete pull examples without SSH:

```bash
# Source host serves a file
pxs --listen 0.0.0.0:8080 --source /srv/export/snapshot.bin --sender

# Destination host pulls it over raw TCP
pxs --remote 192.168.1.10:8080 --destination ./snapshot.bin --pull
```

### 3. Secure Network Synchronization (SSH)
Easiest way to sync securely between servers. `pxs` automatically spawns an SSH tunnel.

**Push (Local -> Remote):**
```bash
pxs --source my_file.bin --remote user@remote-server:/path/to/dest/my_file.bin
```

```bash
# Push one file over SSH
pxs --source ./backup.tar.zst --remote db2@example.net:/srv/backups/backup.tar.zst

# Push a directory tree over SSH
pxs --source /var/lib/postgresql/data --remote db2@example.net:/srv/replica/data
```

**Pull (Remote -> Local):**
```bash
pxs --remote user@remote-server:/path/to/remote/file.bin --destination ./local_file.bin --pull
```

```bash
# Pull one file over SSH
pxs --remote db1@example.net:/srv/export/base.tar.zst --destination ./base.tar.zst --pull

# Pull a directory tree over SSH
pxs --remote db1@example.net:/var/lib/postgresql/data --destination /srv/restore/data --pull
```

**Manual SSH (using stdio pipe):**
If you need custom SSH flags, you can use the `--stdio` mode manually:
```bash
ssh user@remote-server "pxs --stdio --destination /path/to/new/data" < <(pxs --remote - --source /path/to/old/data)
```

### 4. Advanced Options

*   **`--checksum` (-c)**: Force a block-by-block hash comparison even if size/mtime match.
*   **`--fsync` (-f)**: Force `fsync(2)` after file writes. Slower, but safer for durability-sensitive copies.
*   **`--ignore` (-i)**: (Repeatable) Skip files/directories matching a glob pattern (e.g., `-i "*.log"`).
*   **`--exclude-from` (-E)**: Read exclude patterns from a file (one pattern per line).
*   **`--threshold` (-t)**: (Default: 0.5) If the destination file is less than X% the size of the source, perform a full copy instead of hashing.
*   **`--dry-run` (-n)**: Show what would have been transferred without making any changes.
*   **`--verbose` (-v)**: Increase logging verbosity (use `-vv` for debug).

## Progress Output

`pxs` shows a progress bar for:

*   local directory syncs
*   direct TCP sender/receiver transfers where the receiving side knows total size
*   SSH and `--stdio` transfers

Currently, a single local file sync does **not** show a visible progress bar; it prints summary information when the copy completes.

### Exclude Example
If you want to skip Postgres configuration files during a sync:
```bash
pxs --source /var/lib/postgresql/data --destination /backup/data \
  --ignore "postmaster.opts" \
  --ignore "pg_hba.conf" \
  --ignore "postgresql.conf"
```

Or using a file:
```bash
echo "postmaster.pid" > excludes.txt
echo "*.log" >> excludes.txt
pxs -s /src -d /dst -E excludes.txt
```

## How the Ignore Mechanism Works

`pxs` uses the same high-performance engine as `ripgrep` (the `ignore` crate) to filter files during the synchronization process.

### Default Behavior (Full Clone)
By default, `pxs` is configured for **Total Data Fidelity**. It will **NOT** skip:
*   Hidden files or directories (starting with `.`).
*   Files listed in `.gitignore`.
*   Global or local ignore files.

### Using Patterns (Globs)
When you provide patterns via `--ignore` or `--exclude-from`, they are applied as **overrides**. Matching files are skipped entirely: they are not hashed, not counted in the total size, and not transferred.

| Pattern | Effect |
| :--- | :--- |
| `postmaster.pid` | Ignores this specific file anywhere in the tree. |
| `*.log` | Ignores all files ending in `.log`. |
| `temp/*` | Ignores everything inside the top-level `temp` directory. |
| `**/cache/*` | Ignores everything inside any directory named `cache` at any depth. |

### Exclusion Pass-through (SSH)
When using **Auto-SSH mode**, your local ignore patterns are automatically sent to the remote server. This ensures that the receiver doesn't waste time looking at files you've already decided to skip.

## Why pxs is faster than rsync

| Feature | rsync | pxs |
|---------|-------|-----|
| File hashing | Single-threaded | **Parallel** (all CPU cores) |
| Block comparison | Single-threaded | **Parallel** |
| Network connections | Single | **Multiple workers** |
| Directory walking | Sequential | **Parallel** |
| Algorithm | Rolling hash | Fixed 128KB blocks |

1.  **Parallelism**: `rsync` is largely single-threaded. `pxs` uses all available CPU cores to hash different parts of your data simultaneously.
2.  **Algorithm Efficiency**: For database files (like Postgres), data is modified in-place and never "shifted." `pxs` uses a fixed-block algorithm that is much lighter than `rsync`'s rolling hash.
3.  **No Encryption Bottleneck**: By using raw TCP (when appropriate), `pxs` avoids the CPU overhead of SSH encryption which often caps transfers at ~120MB/s.

## Tests

The project includes a robust test suite for both local and network logic:
```bash
# Run all tests
cargo test
```

Podman end-to-end tests are also available:

```bash
# SSH pull end-to-end
./tests/podman/test_ssh_pull.sh

# SSH pull resume/truncation end-to-end
./tests/podman/test_ssh_pull_resume.sh

# Direct TCP push end-to-end
./tests/podman/test_tcp_push.sh

# Direct TCP directory/resume edge cases end-to-end
./tests/podman/test_tcp_directory_resume.sh
```

## License

BSD-3-Clause
