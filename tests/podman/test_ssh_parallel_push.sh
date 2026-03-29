#!/bin/bash
set -euo pipefail

cargo build --release

bash tests/podman/ensure_ssh_key.sh

DOCKER=${DOCKER:-podman}
IMAGE="pxs-ssh-parallel-test"
NETWORK="pxs-ssh-parallel-net"

WORK_DIR=$(mktemp -d)
SRC_DIR="$WORK_DIR/src"
SOURCE_FILE="$SRC_DIR/large.bin"
REMOTE_DIR="/home/devops/Parallel Targets"
REMOTE_FILE="$REMOTE_DIR/final.bin"
REMOTE_PARALLEL_DIR="$REMOTE_DIR/parallel-dir"

cleanup() {
    echo "Cleaning up..."
    $DOCKER rm -f pxs-ssh-parallel-server pxs-ssh-parallel-client 2>/dev/null || true
    $DOCKER network rm "$NETWORK" 2>/dev/null || true
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

mkdir -p "$SRC_DIR"

echo "Creating large source file..."
dd if=/dev/urandom of="$SOURCE_FILE" bs=1M count=128
SOURCE_HASH=$(sha256sum "$SOURCE_FILE" | awk '{print $1}')

$DOCKER network rm "$NETWORK" 2>/dev/null || true
$DOCKER network create "$NETWORK"

echo "Building test image..."
$DOCKER build -t "$IMAGE" -f tests/podman/Containerfile tests/podman/

echo "Starting SSH server..."
$DOCKER run -d --name pxs-ssh-parallel-server \
    --network "$NETWORK" \
    -v "$(pwd)/target/release/pxs:/usr/local/bin/pxs:ro" \
    "$IMAGE"

echo "Waiting for SSH server to be ready..."
sleep 2

$DOCKER exec pxs-ssh-parallel-server bash -lc "mkdir -p \"$REMOTE_PARALLEL_DIR\" && chown -R devops:devops \"$REMOTE_DIR\""

echo "Running SSH exact-file sync with fsync..."
$DOCKER run --name pxs-ssh-parallel-client \
    -t \
    --network "$NETWORK" \
    -v "$(pwd)/target/release/pxs:/usr/local/bin/pxs:ro" \
    -v "$(pwd)/tests/podman/id_ed25519:/tmp/id_ed25519:ro" \
    -v "$SRC_DIR:/src:ro" \
    "$IMAGE" \
    bash -lc "mkdir -p /home/devops/.ssh && \
             cp /tmp/id_ed25519 /home/devops/.ssh/id_ed25519 && \
             chown devops:devops /home/devops/.ssh/id_ed25519 && \
             chmod 600 /home/devops/.ssh/id_ed25519 && \
             echo -e 'Host pxs-ssh-parallel-server\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n  IdentityFile ~/.ssh/id_ed25519' > /home/devops/.ssh/config && \
             chown devops:devops /home/devops/.ssh/config && \
             sudo -u devops pxs sync /src/large.bin \"devops@pxs-ssh-parallel-server:$REMOTE_FILE\" \
             --fsync \
             -vv"

REMOTE_HASH=$($DOCKER exec pxs-ssh-parallel-server sha256sum "$REMOTE_FILE" | awk '{print $1}')
if [ "$REMOTE_HASH" != "$SOURCE_HASH" ]; then
    echo "SSH exact-file push hash mismatch"
    echo "source: $SOURCE_HASH"
    echo "remote: $REMOTE_HASH"
    exit 1
fi

$DOCKER rm -f pxs-ssh-parallel-client >/dev/null

SAMPLER_CMD=$(cat <<'EOF'
rm -f /tmp/ssh_conn_max
max=0
for _ in $(seq 1 400); do
    count=$(ss -tan | awk '$1 == "ESTAB" && $4 ~ /:22$/ {count++} END {print count+0}')
    if [ "$count" -gt "$max" ]; then
        max=$count
    fi
    sleep 0.05
done
echo "$max" > /tmp/ssh_conn_max
EOF
)

echo "Sampling SSH connection fan-out during transfer..."
$DOCKER exec pxs-ssh-parallel-server bash -lc "$SAMPLER_CMD" &
sampler_pid=$!

echo "Running SSH large-file sync with parallel workers..."
$DOCKER run --name pxs-ssh-parallel-client \
    -t \
    --network "$NETWORK" \
    -v "$(pwd)/target/release/pxs:/usr/local/bin/pxs:ro" \
    -v "$(pwd)/tests/podman/id_ed25519:/tmp/id_ed25519:ro" \
    -v "$SRC_DIR:/src:ro" \
    "$IMAGE" \
    bash -lc "mkdir -p /home/devops/.ssh && \
             cp /tmp/id_ed25519 /home/devops/.ssh/id_ed25519 && \
             chown devops:devops /home/devops/.ssh/id_ed25519 && \
             chmod 600 /home/devops/.ssh/id_ed25519 && \
             echo -e 'Host pxs-ssh-parallel-server\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n  IdentityFile ~/.ssh/id_ed25519' > /home/devops/.ssh/config && \
             chown devops:devops /home/devops/.ssh/config && \
             sudo -u devops pxs sync /src/large.bin \"devops@pxs-ssh-parallel-server:$REMOTE_PARALLEL_DIR\" \
             --fsync \
             --large-file-parallel-threshold 1MiB \
             --large-file-parallel-workers 2 \
             -vv"

wait "$sampler_pid"

REMOTE_HASH=$($DOCKER exec pxs-ssh-parallel-server sha256sum "$REMOTE_PARALLEL_DIR/large.bin" | awk '{print $1}')
MAX_CONN=$($DOCKER exec pxs-ssh-parallel-server cat /tmp/ssh_conn_max)

if [ "$REMOTE_HASH" != "$SOURCE_HASH" ]; then
    echo "SSH parallel push hash mismatch"
    echo "source: $SOURCE_HASH"
    echo "remote: $REMOTE_HASH"
    exit 1
fi

if [ "$MAX_CONN" -lt 2 ]; then
    echo "Expected multiple SSH connections during large-file parallel sync, saw: $MAX_CONN"
    exit 1
fi

echo "✅ SSH parallel push test passed!"
