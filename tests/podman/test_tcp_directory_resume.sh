#!/bin/bash
set -euo pipefail

cargo build --release

bash tests/podman/ensure_ssh_key.sh

DOCKER=${DOCKER:-podman}
IMAGE="pxs-tcp-tree-test"
NETWORK="pxs-tcp-tree-net"
PORT="8080"

TREE_MTIME=1000000001
NESTED_MTIME=1000000002

WORK_DIR=$(mktemp -d)
SRC_DIR="$WORK_DIR/src"
DST_DIR="$WORK_DIR/dst"

cleanup() {
    echo "Cleaning up..."
    $DOCKER rm -f pxs-tree-receiver pxs-tree-sender 2>/dev/null || true
    $DOCKER network rm "$NETWORK" 2>/dev/null || true
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

mkdir -p "$SRC_DIR/tree/nested" "$DST_DIR"

echo "Creating source tree..."
dd if=/dev/urandom of="$SRC_DIR/resume.bin" bs=1M count=48
dd if=/dev/urandom of="$SRC_DIR/truncate.bin" bs=1M count=40
dd if=/dev/urandom of="$SRC_DIR/unchanged.bin" bs=1M count=4
dd if=/dev/urandom of="$SRC_DIR/tree/nested/file.bin" bs=1M count=8
printf 'root-content\n' > "$SRC_DIR/tree/root.txt"
printf 'replacement file\n' > "$SRC_DIR/replace_me"
ln -s "missing/target" "$SRC_DIR/broken_link"

touch -d "@$TREE_MTIME" "$SRC_DIR/tree"
touch -d "@$NESTED_MTIME" "$SRC_DIR/tree/nested"

RESUME_HASH=$(sha256sum "$SRC_DIR/resume.bin" | awk '{print $1}')
TRUNCATE_HASH=$(sha256sum "$SRC_DIR/truncate.bin" | awk '{print $1}')
UNCHANGED_HASH=$(sha256sum "$SRC_DIR/unchanged.bin" | awk '{print $1}')
NESTED_HASH=$(sha256sum "$SRC_DIR/tree/nested/file.bin" | awk '{print $1}')
TRUNCATE_SIZE=$(stat -c %s "$SRC_DIR/truncate.bin")

echo "Preparing destination edge cases..."
dd if="$SRC_DIR/resume.bin" of="$DST_DIR/resume.bin" bs=1M count=12 status=none
cp "$SRC_DIR/truncate.bin" "$DST_DIR/truncate.bin"
dd if=/dev/urandom bs=1M count=5 >> "$DST_DIR/truncate.bin"
cp "$SRC_DIR/unchanged.bin" "$DST_DIR/unchanged.bin"
touch -r "$SRC_DIR/unchanged.bin" "$DST_DIR/unchanged.bin"
mkdir -p "$DST_DIR/replace_me/stale"
printf 'stale\n' > "$DST_DIR/replace_me/stale/file.txt"
mkdir -p "$DST_DIR/broken_link/stale"
printf 'stale\n' > "$DST_DIR/broken_link/stale/file.txt"

$DOCKER network rm "$NETWORK" 2>/dev/null || true
$DOCKER network create "$NETWORK"

echo "Building test image..."
$DOCKER build -t "$IMAGE" -f tests/podman/Containerfile tests/podman/

echo "Starting TCP receiver..."
$DOCKER run -d --name pxs-tree-receiver \
    --network "$NETWORK" \
    -v "$(pwd)/target/release/pxs:/usr/local/bin/pxs:ro" \
    -v "$DST_DIR:/data" \
    "$IMAGE" \
    bash -lc "pxs listen 0.0.0.0:$PORT /data -vv"

echo "Waiting for receiver to be ready..."
sleep 2

echo "Starting sender and syncing directory tree..."
$DOCKER run --name pxs-tree-sender \
    -t \
    --network "$NETWORK" \
    -v "$(pwd)/target/release/pxs:/usr/local/bin/pxs:ro" \
    -v "$SRC_DIR:/src:ro" \
    "$IMAGE" \
    bash -lc "pxs sync /src pxs-tree-receiver:$PORT -vv"

if [ "$(sha256sum "$DST_DIR/resume.bin" | awk '{print $1}')" != "$RESUME_HASH" ]; then
    echo "resume.bin hash mismatch after resume sync"
    exit 1
fi

if [ "$(sha256sum "$DST_DIR/truncate.bin" | awk '{print $1}')" != "$TRUNCATE_HASH" ]; then
    echo "truncate.bin hash mismatch after sync"
    exit 1
fi

if [ "$(stat -c %s "$DST_DIR/truncate.bin")" != "$TRUNCATE_SIZE" ]; then
    echo "truncate.bin size mismatch after sync"
    exit 1
fi

if [ "$(sha256sum "$DST_DIR/unchanged.bin" | awk '{print $1}')" != "$UNCHANGED_HASH" ]; then
    echo "unchanged.bin hash mismatch after skipped-file sync"
    exit 1
fi

if [ ! -f "$DST_DIR/replace_me" ] || [ -d "$DST_DIR/replace_me" ]; then
    echo "replace_me was not converted from directory to file"
    exit 1
fi

if [ "$(cat "$DST_DIR/replace_me")" != "replacement file" ]; then
    echo "replace_me content mismatch"
    exit 1
fi

if [ ! -L "$DST_DIR/broken_link" ]; then
    echo "broken_link was not converted to a symlink"
    exit 1
fi

if [ "$(readlink "$DST_DIR/broken_link")" != "missing/target" ]; then
    echo "broken_link target mismatch"
    exit 1
fi

if [ "$(sha256sum "$DST_DIR/tree/nested/file.bin" | awk '{print $1}')" != "$NESTED_HASH" ]; then
    echo "nested file hash mismatch"
    exit 1
fi

if [ "$(cat "$DST_DIR/tree/root.txt")" != "root-content" ]; then
    echo "tree/root.txt content mismatch"
    exit 1
fi

if [ "$(stat -c %Y "$DST_DIR/tree")" != "$TREE_MTIME" ]; then
    echo "tree directory mtime mismatch"
    exit 1
fi

if [ "$(stat -c %Y "$DST_DIR/tree/nested")" != "$NESTED_MTIME" ]; then
    echo "tree/nested directory mtime mismatch"
    exit 1
fi

echo "✅ Direct TCP directory resume test passed!"
