#!/bin/bash
set -euo pipefail

cargo build --release

bash tests/podman/ensure_ssh_key.sh

DOCKER=${DOCKER:-podman}
IMAGE="pxs-ssh-resume-test"
NETWORK="pxs-ssh-resume-net"

WORK_DIR=$(mktemp -d)
SRC_DIR="$WORK_DIR/src"
DST_DIR="$WORK_DIR/dst"
SOURCE_FILE="$SRC_DIR/test.bin"
DEST_FILE="$DST_DIR/test.bin"

cleanup() {
    echo "Cleaning up..."
    $DOCKER rm -f pxs-ssh-resume-server pxs-ssh-resume-client 2>/dev/null || true
    $DOCKER network rm "$NETWORK" 2>/dev/null || true
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

mkdir -p "$SRC_DIR" "$DST_DIR"

echo "Creating source file and oversized destination..."
dd if=/dev/urandom of="$SOURCE_FILE" bs=1M count=48
cp "$SOURCE_FILE" "$DEST_FILE"
dd if=/dev/urandom bs=1M count=6 >> "$DEST_FILE"

SOURCE_HASH=$(sha256sum "$SOURCE_FILE" | awk '{print $1}')
SOURCE_SIZE=$(stat -c %s "$SOURCE_FILE")

$DOCKER network rm "$NETWORK" 2>/dev/null || true
$DOCKER network create "$NETWORK"

echo "Building test image..."
$DOCKER build -t "$IMAGE" -f tests/podman/Containerfile tests/podman/

echo "Starting SSH server..."
$DOCKER run -d --name pxs-ssh-resume-server \
    --network "$NETWORK" \
    -v "$(pwd)/target/release/pxs:/usr/local/bin/pxs:ro" \
    -v "$SRC_DIR:/srv/export:ro" \
    "$IMAGE"

echo "Waiting for SSH server to be ready..."
sleep 2

echo "Starting client and syncing with a pre-existing oversized destination..."
$DOCKER run --name pxs-ssh-resume-client \
    -t \
    --network "$NETWORK" \
    -v "$(pwd)/target/release/pxs:/usr/local/bin/pxs:ro" \
    -v "$(pwd)/tests/podman/id_ed25519:/tmp/id_ed25519:ro" \
    -v "$DST_DIR:/data" \
    "$IMAGE" \
    bash -lc "mkdir -p /root/.ssh && \
             cp /tmp/id_ed25519 /root/.ssh/id_ed25519 && \
             chmod 600 /root/.ssh/id_ed25519 && \
             echo -e 'Host pxs-ssh-resume-server\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n  IdentityFile ~/.ssh/id_ed25519' > /root/.ssh/config && \
             chmod 600 /root/.ssh/config && \
             pxs sync devops@pxs-ssh-resume-server:/srv/export/test.bin /data/test.bin \
             -vv"

if [ ! -f "$DEST_FILE" ]; then
    echo "Destination file was not created: $DEST_FILE"
    exit 1
fi

if [ "$(sha256sum "$DEST_FILE" | awk '{print $1}')" != "$SOURCE_HASH" ]; then
    echo "SSH resume hash mismatch"
    exit 1
fi

if [ "$(stat -c %s "$DEST_FILE")" != "$SOURCE_SIZE" ]; then
    echo "SSH resume size mismatch"
    exit 1
fi

echo "✅ SSH sync resume test passed!"
