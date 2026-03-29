#!/bin/bash
set -euo pipefail

cargo build --release

bash tests/podman/ensure_ssh_key.sh

DOCKER=${DOCKER:-podman}
IMAGE="pxs-tcp-dir-pull-test"
NETWORK="pxs-tcp-dir-pull-net"
PORT="7979"
SYNC_TIME="2024-01-02 03:04:05 UTC"

WORK_DIR=$(mktemp -d)
SRC_DIR="$WORK_DIR/src"
DST_DIR="$WORK_DIR/dst"
SOURCE_ROOT="$SRC_DIR/dataset"

cleanup() {
    echo "Cleaning up..."
    $DOCKER rm -f pxs-tcp-dir-source pxs-tcp-dir-client 2>/dev/null || true
    $DOCKER network rm "$NETWORK" 2>/dev/null || true
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

mkdir -p "$SOURCE_ROOT/nested" "$DST_DIR/stale/subdir"

printf '%s' 'source payload 123' > "$SOURCE_ROOT/same.bin"
printf '%s' 'nested payload' > "$SOURCE_ROOT/nested/keep.txt"
printf '%s' 'ignored payload' > "$SOURCE_ROOT/skip.tmp"
touch -d "$SYNC_TIME" "$SOURCE_ROOT/same.bin"

printf '%s' 'destin payload 123' > "$DST_DIR/same.bin"
touch -d "$SYNC_TIME" "$DST_DIR/same.bin"
printf '%s' 'obsolete' > "$DST_DIR/stale/subdir/old.txt"

$DOCKER network rm "$NETWORK" 2>/dev/null || true
$DOCKER network create "$NETWORK"

echo "Building test image..."
$DOCKER build -t "$IMAGE" -f tests/podman/Containerfile tests/podman/

echo "Starting TCP serve endpoint..."
$DOCKER run -d --name pxs-tcp-dir-source \
    --network "$NETWORK" \
    -v "$(pwd)/target/release/pxs:/usr/local/bin/pxs:ro" \
    -v "$SRC_DIR:/srv/export:ro" \
    "$IMAGE" \
    bash -lc "pxs serve 0.0.0.0:$PORT /srv/export -vv"

echo "Waiting for source server to be ready..."
sleep 2

echo "Starting client and syncing directory tree from serve..."
$DOCKER run --name pxs-tcp-dir-client \
    -t \
    --network "$NETWORK" \
    -v "$(pwd)/target/release/pxs:/usr/local/bin/pxs:ro" \
    -v "$DST_DIR:/data" \
    "$IMAGE" \
    bash -lc "pxs sync pxs-tcp-dir-source:$PORT/dataset /data --delete --checksum --ignore '*.tmp' -vv"

if [ "$(cat "$DST_DIR/same.bin")" != "source payload 123" ]; then
    echo "TCP directory pull did not refresh same.bin under checksum mode"
    exit 1
fi

if [ "$(cat "$DST_DIR/nested/keep.txt")" != "nested payload" ]; then
    echo "TCP directory pull did not sync nested/keep.txt"
    exit 1
fi

if [ -e "$DST_DIR/skip.tmp" ]; then
    echo "TCP directory pull did not honor client-side ignore"
    exit 1
fi

if [ -e "$DST_DIR/stale" ]; then
    echo "TCP directory pull did not remove stale destination entries"
    exit 1
fi

echo "✅ TCP directory pull test passed!"
