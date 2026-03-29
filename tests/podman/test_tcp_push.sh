#!/bin/bash
set -euo pipefail

# Build pxs release
cargo build --release

bash tests/podman/ensure_ssh_key.sh

# Use podman or docker
DOCKER=${DOCKER:-podman}

# Image and network
IMAGE="pxs-tcp-test"
NETWORK="pxs-tcp-net"
PORT="8080"

WORK_DIR=$(mktemp -d)
SRC_DIR="$WORK_DIR/src"
DST_DIR="$WORK_DIR/dst"
SOURCE_FILE="$SRC_DIR/test.bin"
DEST_FILE="$DST_DIR/test.bin"

mkdir -p "$SRC_DIR" "$DST_DIR"

cleanup() {
    echo "Cleaning up..."
    $DOCKER rm -f pxs-tcp-receiver pxs-tcp-sender 2>/dev/null || true
    $DOCKER network rm "$NETWORK" 2>/dev/null || true
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

# Create a deterministic test payload on the host so we can verify it after transfer
echo "Creating local test file..."
dd if=/dev/urandom of="$SOURCE_FILE" bs=1M count=64
SOURCE_HASH=$(sha256sum "$SOURCE_FILE" | awk '{print $1}')

# Create network
$DOCKER network rm "$NETWORK" 2>/dev/null || true
$DOCKER network create "$NETWORK"

# Build image
echo "Building test image..."
$DOCKER build -t "$IMAGE" -f tests/podman/Containerfile tests/podman/

# Start receiver
echo "Starting TCP receiver..."
$DOCKER run -d --name pxs-tcp-receiver \
    --network "$NETWORK" \
    -v "$(pwd)/target/release/pxs:/usr/local/bin/pxs:ro" \
    -v "$DST_DIR:/data" \
    "$IMAGE" \
    bash -lc "pxs listen 0.0.0.0:$PORT /data -vv"

# Wait for listener to be ready
echo "Waiting for receiver to be ready..."
sleep 2

# Sync file into the raw TCP listener
echo "Starting sender and syncing file..."
$DOCKER run --name pxs-tcp-sender \
    -t \
    --network "$NETWORK" \
    -v "$(pwd)/target/release/pxs:/usr/local/bin/pxs:ro" \
    -v "$SRC_DIR:/src:ro" \
    "$IMAGE" \
    bash -lc "pxs sync /src/test.bin pxs-tcp-receiver:$PORT/test.bin -vv"

# Verify copied bytes on the host bind mount
if [ ! -f "$DEST_FILE" ]; then
    echo "Destination file was not created: $DEST_FILE"
    exit 1
fi

DEST_HASH=$(sha256sum "$DEST_FILE" | awk '{print $1}')
if [ "$SOURCE_HASH" != "$DEST_HASH" ]; then
    echo "Hash mismatch after TCP sync"
    echo "source: $SOURCE_HASH"
    echo "dest:   $DEST_HASH"
    exit 1
fi

echo "✅ Direct TCP sync-to-listen test passed!"
