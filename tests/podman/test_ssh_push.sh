#!/bin/bash
set -euo pipefail

cargo build --release

bash tests/podman/ensure_ssh_key.sh

DOCKER=${DOCKER:-podman}
IMAGE="pxs-ssh-push-test"
NETWORK="pxs-ssh-push-net"
WORK_DIR=$(mktemp -d)
SRC_DIR="$WORK_DIR/src"
REMOTE_ROOT="/home/devops/Remote Dst"
EXPECTED_ROOT_CONTENT="push root payload"
EXPECTED_NESTED_CONTENT="push nested payload"

cleanup() {
    echo "Cleaning up..."
    $DOCKER rm -f pxs-ssh-push-server pxs-ssh-push-client 2>/dev/null || true
    $DOCKER network rm "$NETWORK" 2>/dev/null || true
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

mkdir -p "$SRC_DIR/nested"
printf '%s' "$EXPECTED_ROOT_CONTENT" > "$SRC_DIR/keep.txt"
printf '%s' "$EXPECTED_NESTED_CONTENT" > "$SRC_DIR/nested/keep.txt"
printf '%s' 'ignored payload' > "$SRC_DIR/skip.tmp"

$DOCKER network rm "$NETWORK" 2>/dev/null || true
$DOCKER network create "$NETWORK"

echo "Building test image..."
$DOCKER build -t "$IMAGE" -f tests/podman/Containerfile tests/podman/

echo "Starting SSH server..."
$DOCKER run -d --name pxs-ssh-push-server \
    --network "$NETWORK" \
    -v "$(pwd)/target/release/pxs:/usr/local/bin/pxs:ro" \
    "$IMAGE"

echo "Waiting for SSH server to be ready..."
sleep 2

echo "Starting client and syncing directory to the remote destination..."
$DOCKER run --name pxs-ssh-push-client \
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
             echo -e 'Host pxs-ssh-push-server\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n  IdentityFile ~/.ssh/id_ed25519' > /home/devops/.ssh/config && \
             chown devops:devops /home/devops/.ssh/config && \
             sudo -u devops pxs sync /src \"devops@pxs-ssh-push-server:$REMOTE_ROOT\" \
             --ignore '*.tmp' \
             -vv"

REMOTE_CHECK_SCRIPT=$(cat <<'EOF'
set -euo pipefail
if [ ! -f "/home/devops/Remote Dst/keep.txt" ]; then
    echo "Missing pushed keep.txt"
    exit 1
fi
if [ "$(cat "/home/devops/Remote Dst/keep.txt")" != "push root payload" ]; then
    echo "Root file content mismatch"
    exit 1
fi
if [ ! -f "/home/devops/Remote Dst/nested/keep.txt" ]; then
    echo "Missing pushed nested file"
    exit 1
fi
if [ "$(cat "/home/devops/Remote Dst/nested/keep.txt")" != "push nested payload" ]; then
    echo "Nested file content mismatch"
    exit 1
fi
if [ -e "/home/devops/Remote Dst/skip.tmp" ]; then
    echo "Ignored file was transferred during SSH sync"
    exit 1
fi
EOF
)

$DOCKER exec pxs-ssh-push-server bash -lc "$REMOTE_CHECK_SCRIPT"

echo "✅ SSH sync-to-remote test passed!"
