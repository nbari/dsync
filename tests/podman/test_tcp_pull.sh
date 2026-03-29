#!/bin/bash
set -euo pipefail

# Build pxs release
cargo build --release

bash tests/podman/ensure_ssh_key.sh

DOCKER=${DOCKER:-podman}
IMAGE="pxs-tcp-pull-test"
NETWORK="pxs-tcp-pull-net"
PORT="7877"

WORK_DIR=$(mktemp -d)
SRC_DIR="$WORK_DIR/src"
DST_DIR="$WORK_DIR/dst"
SOURCE_FILE="$SRC_DIR/test.bin"
DEST_FILE="$DST_DIR/test.bin"

mkdir -p "$SRC_DIR" "$DST_DIR"

cleanup() {
    echo "Cleaning up..."
    $DOCKER rm -f pxs-tcp-source pxs-tcp-client 2>/dev/null || true
    $DOCKER network rm "$NETWORK" 2>/dev/null || true
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

echo "Creating source file to serve..."
dd if=/dev/urandom of="$SOURCE_FILE" bs=1M count=64
SOURCE_HASH=$(sha256sum "$SOURCE_FILE" | awk '{print $1}')

$DOCKER network rm "$NETWORK" 2>/dev/null || true
$DOCKER network create "$NETWORK"

echo "Building test image..."
$DOCKER build -t "$IMAGE" -f tests/podman/Containerfile tests/podman/

echo "Starting TCP serve endpoint..."
$DOCKER run -d --name pxs-tcp-source \
    --network "$NETWORK" \
    -v "$(pwd)/target/release/pxs:/usr/local/bin/pxs:ro" \
    -v "$SRC_DIR:/srv/export:ro" \
    "$IMAGE" \
    bash -lc "pxs serve 0.0.0.0:$PORT /srv/export -vv"

echo "Waiting for source server to be ready..."
sleep 2

echo "Starting client and syncing file from serve..."
$DOCKER run --name pxs-tcp-client \
    -t \
    --network "$NETWORK" \
    -v "$(pwd)/target/release/pxs:/usr/local/bin/pxs:ro" \
    -v "$DST_DIR:/data" \
    "$IMAGE" \
    bash -lc "pxs sync pxs-tcp-source:$PORT/test.bin /data/test.bin -vv"

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

echo "✅ Direct TCP sync-from-serve test passed!"
