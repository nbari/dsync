# pxs

**pxs** (Parallel X-Sync) is a high-performance, concurrent file synchronization tool written in Rust. Designed to saturate high-speed networks (10GbE+) and utilize multi-core CPUs for extremely fast data transfers and incremental updates.

The name is intentionally short for CLI use: `pxs` stands for **Parallel X-Sync**.

`pxs` is specifically optimized for massive data moves (e.g., 4TB+ Postgres `PG_DATA`) where standard tools like `rsync` often bottleneck on single-threaded hashing or SSH encryption overhead.

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

## How It Works

### Local Synchronization

```mermaid
flowchart LR
    subgraph "Local Machine"
        SRC[("Source\n/data/pgdata")]
        DST[("Destination\n/backup/pgdata")]
        
        subgraph "pxs Engine"
            W["Parallel\nFile Walker"]
            H["Parallel\nBlock Hasher\n(XxHash64)"]
            C["Block\nComparator"]
            T["Parallel\nBlock Writer"]
        end
    end
    
    SRC --> W
    W --> H
    H --> C
    C -->|"Changed blocks only"| T
    T --> DST
    
    style SRC fill:#e1f5fe
    style DST fill:#c8e6c9
    style H fill:#fff3e0
    style T fill:#fff3e0
```

### Network Synchronization (Direct TCP)

```mermaid
sequenceDiagram
    box rgb(225, 245, 254) Server 1 (Sender)
        participant S as pxs Sender
        participant SF as Source Files
    end
    box rgb(200, 230, 201) Server 2 (Receiver)
        participant R as pxs Receiver
        participant DF as Dest Files
    end

    Note over S,R: TCP Connection (Port 8080)
    
    S->>R: Handshake (version)
    R->>S: Handshake ACK
    
    loop For each file
        S->>R: SyncFile (path, metadata, size)
        
        alt File exists & size matches
            R->>S: RequestHashes
            S->>R: BlockHashes (128KB chunks)
            R->>R: Compare with local hashes
            R->>S: RequestBlocks (changed indices)
            S->>R: ApplyBlocks (delta data)
        else New file or size mismatch
            R->>S: RequestFullCopy
            S->>R: ApplyBlocks (all data)
        end
        
        S->>R: ApplyMetadata
        R->>S: MetadataApplied
    end
```

### SSH Synchronization (Auto-Tunnel)

```mermaid
flowchart TB
    subgraph local["Local Machine"]
        CLI["pxs --source /data\n--remote user@server:/backup"]
        STDIN["stdin"]
        STDOUT["stdout"]
    end
    
    subgraph ssh["SSH Tunnel"]
        SSH["SSH Process\n(aes128-gcm)"]
    end
    
    subgraph remote["Remote Server"]
        RPXS["pxs --stdio\n--destination /backup"]
        RDST[("Destination\n/backup")]
    end
    
    CLI -->|"Spawns"| SSH
    CLI <-->|"pxs protocol"| STDIN
    STDIN <--> SSH
    SSH <-->|"Encrypted"| RPXS
    STDOUT <--> SSH
    RPXS --> RDST
    
    style local fill:#e3f2fd
    style ssh fill:#fff8e1
    style remote fill:#e8f5e9
```

### Delta Sync Algorithm

```mermaid
flowchart TD
    START([Start Sync]) --> CHECK{File exists\nat dest?}
    
    CHECK -->|No| FULL[Full Copy]
    CHECK -->|Yes| SIZE{Size\nmatches?}
    
    SIZE -->|No| THRESH{Dest < threshold%\nof source?}
    SIZE -->|Yes| MTIME{mtime\nmatches?}
    
    THRESH -->|Yes| FULL
    THRESH -->|No| DELTA
    
    MTIME -->|Yes & !checksum| SKIP[Skip File]
    MTIME -->|No or checksum| DELTA[Delta Sync]
    
    subgraph DELTA[Delta Sync]
        direction TB
        D1[Hash source blocks\nin parallel] --> D2[Hash dest blocks\nin parallel]
        D2 --> D3[Compare hashes]
        D3 --> D4[Transfer only\nchanged blocks]
    end
    
    FULL --> META[Apply Metadata]
    DELTA --> META
    SKIP --> DONE([Done])
    META --> DONE
    
    style SKIP fill:#c8e6c9
    style FULL fill:#ffcdd2
    style DELTA fill:#fff3e0
```

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

**B. Pulling from a Sender (You are getting the file)**
*   **Sender (Server 2):**
    ```bash
    pxs --listen 0.0.0.0:8080 --source /old/data/file.bin --sender
    ```
*   **Receiver (Server 1):**
    ```bash
    pxs --remote 192.168.1.10:8080 --destination ./local_copy.bin --pull
    ```

### 3. Secure Network Synchronization (SSH)
Easiest way to sync securely between servers. `pxs` automatically spawns an SSH tunnel.

**Push (Local -> Remote):**
```bash
pxs --source my_file.bin --remote user@remote-server:/path/to/dest/my_file.bin
```

**Pull (Remote -> Local):**
```bash
pxs --remote user@remote-server:/path/to/remote/file.bin --destination ./local_file.bin --pull
```

**Manual SSH (using stdio pipe):**
If you need custom SSH flags, you can use the `--stdio` mode manually:
```bash
ssh user@remote-server "pxs --stdio --destination /path/to/new/data" < <(pxs --remote - --source /path/to/old/data)
```

### 4. Advanced Options

*   **`--checksum` (-c)**: Force a block-by-block hash comparison even if size/mtime match.
*   **`--ignore` (-i)**: (Repeatable) Skip files/directories matching a glob pattern (e.g., `-i "*.log"`).
*   **`--exclude-from` (-E)**: Read exclude patterns from a file (one pattern per line).
*   **`--threshold` (-t)**: (Default: 0.5) If the destination file is less than X% the size of the source, perform a full copy instead of hashing.
*   **`--dry-run` (-n)**: Show what would have been transferred without making any changes.
*   **`--verbose` (-v)**: Increase logging verbosity (use `-vv` for debug).

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

## License

BSD-3-Clause
