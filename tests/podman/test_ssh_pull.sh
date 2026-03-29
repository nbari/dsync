#!/bin/bash
set -euo pipefail

cargo build --release

bash tests/podman/ensure_ssh_key.sh

DOCKER=${DOCKER:-podman}
IMAGE="pxs-ssh-test"
NETWORK="pxs-net"
REMOTE_ROOT="/home/devops/Remote Data"
EXPECTED_SAME_CONTENT="source payload 123"
STALE_SAME_CONTENT="destin payload 123"
EXPECTED_NESTED_CONTENT="nested payload"
SYNC_TIME="2024-01-02 03:04:05 UTC"

cleanup() {
    echo "Cleaning up..."
    $DOCKER rm -f pxs-server pxs-client 2>/dev/null || true
    $DOCKER network rm $NETWORK 2>/dev/null || true
}
trap cleanup EXIT

$DOCKER network rm $NETWORK 2>/dev/null || true
$DOCKER network create $NETWORK

echo "Building test image..."
$DOCKER build -t "$IMAGE" -f tests/podman/Containerfile tests/podman/

echo "Starting SSH server..."
$DOCKER run -d --name pxs-server \
    --network "$NETWORK" \
    -v "$(pwd)/target/release/pxs:/usr/local/bin/pxs:ro" \
    "$IMAGE"

echo "Waiting for SSH server to be ready..."
sleep 2

echo "Creating remote source tree..."
$DOCKER exec pxs-server bash -lc "mkdir -p \"$REMOTE_ROOT/nested\" && \
    printf '%s' '$EXPECTED_SAME_CONTENT' > \"$REMOTE_ROOT/same.bin\" && \
    printf '%s' '$EXPECTED_NESTED_CONTENT' > \"$REMOTE_ROOT/nested/keep.txt\" && \
    printf '%s' 'ignored payload' > \"$REMOTE_ROOT/skip.tmp\" && \
    touch -d '$SYNC_TIME' \"$REMOTE_ROOT/same.bin\" && \
    chown -R devops:devops \"$REMOTE_ROOT\""

echo "Starting client and syncing directory from the remote source..."
$DOCKER run --name pxs-client \
    -t \
    --network "$NETWORK" \
    -v "$(pwd)/target/release/pxs:/usr/local/bin/pxs:ro" \
    -v "$(pwd)/tests/podman/id_ed25519:/tmp/id_ed25519:ro" \
    "$IMAGE" \
    bash -lc "mkdir -p /home/devops/.ssh && \
             cp /tmp/id_ed25519 /home/devops/.ssh/id_ed25519 && \
             chown devops:devops /home/devops/.ssh/id_ed25519 && \
             chmod 600 /home/devops/.ssh/id_ed25519 && \
             mkdir -p /data && \
             chown -R devops:devops /data && \
             printf '%s' '$STALE_SAME_CONTENT' > /data/same.bin && \
             touch -d '$SYNC_TIME' /data/same.bin && \
             echo -e 'Host pxs-server\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n  IdentityFile ~/.ssh/id_ed25519' > /home/devops/.ssh/config && \
             chown devops:devops /home/devops/.ssh/config && \
             sudo -u devops pxs sync \"devops@pxs-server:$REMOTE_ROOT\" /data \
             --checksum \
             --ignore '*.tmp' \
             -vv && \
             [ -f /data/same.bin ] && \
             [ \"\$(cat /data/same.bin)\" = '$EXPECTED_SAME_CONTENT' ] && \
             [ -f /data/nested/keep.txt ] && \
             [ \"\$(cat /data/nested/keep.txt)\" = '$EXPECTED_NESTED_CONTENT' ] && \
             [ ! -e /data/skip.tmp ]"

echo "✅ SSH sync-from-remote test passed!"
