#!/bin/bash
set -euo pipefail

cargo build --release

bash tests/podman/ensure_ssh_key.sh

DOCKER=${DOCKER:-podman}
IMAGE="pxs-ssh-delete-test"
NETWORK="pxs-ssh-delete-net"

WORK_DIR=$(mktemp -d)
PUSH_SRC_DIR="$WORK_DIR/push-src"
PULL_DST_DIR="$WORK_DIR/pull-dst"
REMOTE_PUSH_DST="/home/devops/Delete Push Dst"
REMOTE_PULL_SRC="/home/devops/Delete Pull Src"

cleanup() {
    echo "Cleaning up..."
    $DOCKER rm -f pxs-ssh-delete-server pxs-ssh-delete-push-client pxs-ssh-delete-pull-client 2>/dev/null || true
    $DOCKER network rm "$NETWORK" 2>/dev/null || true
    rm -rf "$WORK_DIR" 2>/dev/null || true
}
trap cleanup EXIT

mkdir -p "$PUSH_SRC_DIR/nested" "$PULL_DST_DIR/stale/subdir"
printf '%s' 'push-keep' > "$PUSH_SRC_DIR/keep.txt"
printf '%s' 'push-nested' > "$PUSH_SRC_DIR/nested/keep.txt"
printf '%s' 'local-stale' > "$PULL_DST_DIR/stale/subdir/old.txt"
printf '%s' 'local-old' > "$PULL_DST_DIR/keep.txt"

$DOCKER network rm "$NETWORK" 2>/dev/null || true
$DOCKER network create "$NETWORK"

echo "Building test image..."
$DOCKER build -t "$IMAGE" -f tests/podman/Containerfile tests/podman/

echo "Starting SSH server..."
$DOCKER run -d --name pxs-ssh-delete-server \
    --network "$NETWORK" \
    -v "$(pwd)/target/release/pxs:/usr/local/bin/pxs:ro" \
    "$IMAGE"

echo "Waiting for SSH server to be ready..."
sleep 2

echo "Preparing remote delete destinations..."
$DOCKER exec pxs-ssh-delete-server bash -lc "mkdir -p \"$REMOTE_PUSH_DST/stale/subdir\" \"$REMOTE_PULL_SRC/nested\" && \
    printf '%s' 'remote-old' > \"$REMOTE_PUSH_DST/keep.txt\" && \
    printf '%s' 'remote-stale' > \"$REMOTE_PUSH_DST/stale/subdir/old.txt\" && \
    printf '%s' 'pull-keep' > \"$REMOTE_PULL_SRC/keep.txt\" && \
    printf '%s' 'pull-nested' > \"$REMOTE_PULL_SRC/nested/keep.txt\" && \
    printf '%s' 'ignored' > \"$REMOTE_PULL_SRC/skip.tmp\" && \
    chown -R devops:devops \"$REMOTE_PUSH_DST\" \"$REMOTE_PULL_SRC\""

echo "Running SSH push delete + fsync..."
$DOCKER run --name pxs-ssh-delete-push-client \
    -t \
    --network "$NETWORK" \
    -v "$(pwd)/target/release/pxs:/usr/local/bin/pxs:ro" \
    -v "$(pwd)/tests/podman/id_ed25519:/tmp/id_ed25519:ro" \
    -v "$PUSH_SRC_DIR:/push-src:ro" \
    "$IMAGE" \
    bash -lc "mkdir -p /home/devops/.ssh && \
             cp /tmp/id_ed25519 /home/devops/.ssh/id_ed25519 && \
             chown devops:devops /home/devops/.ssh/id_ed25519 && \
             chmod 600 /home/devops/.ssh/id_ed25519 && \
             echo -e 'Host pxs-ssh-delete-server\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n  IdentityFile ~/.ssh/id_ed25519' > /home/devops/.ssh/config && \
             chown devops:devops /home/devops/.ssh/config && \
             sudo -u devops pxs sync /push-src \"devops@pxs-ssh-delete-server:$REMOTE_PUSH_DST\" --delete --fsync -vv"

$DOCKER exec pxs-ssh-delete-server bash -lc "test -f \"$REMOTE_PUSH_DST/keep.txt\" && \
    test -f \"$REMOTE_PUSH_DST/nested/keep.txt\" && \
    [ \"\$(cat \"$REMOTE_PUSH_DST/keep.txt\")\" = 'push-keep' ] && \
    [ \"\$(cat \"$REMOTE_PUSH_DST/nested/keep.txt\")\" = 'push-nested' ] && \
    [ ! -e \"$REMOTE_PUSH_DST/stale\" ]"

echo "Running SSH pull delete + fsync..."
$DOCKER run --name pxs-ssh-delete-pull-client \
    -t \
    --network "$NETWORK" \
    -v "$(pwd)/target/release/pxs:/usr/local/bin/pxs:ro" \
    -v "$(pwd)/tests/podman/id_ed25519:/tmp/id_ed25519:ro" \
    -v "$PULL_DST_DIR:/pull-dst" \
    "$IMAGE" \
    bash -lc "mkdir -p /home/devops/.ssh && \
             cp /tmp/id_ed25519 /home/devops/.ssh/id_ed25519 && \
             chown devops:devops /home/devops/.ssh/id_ed25519 && \
             chmod 600 /home/devops/.ssh/id_ed25519 && \
             chown -R devops:devops /pull-dst && \
             echo -e 'Host pxs-ssh-delete-server\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n  IdentityFile ~/.ssh/id_ed25519' > /home/devops/.ssh/config && \
             chown devops:devops /home/devops/.ssh/config && \
             sudo -u devops pxs sync \"devops@pxs-ssh-delete-server:$REMOTE_PULL_SRC\" /pull-dst \
             --delete --fsync --ignore '*.tmp' -vv && \
             chmod -R a+rwX /pull-dst"

if [ "$(cat "$PULL_DST_DIR/keep.txt")" != "pull-keep" ]; then
    echo "SSH pull delete did not refresh keep.txt"
    exit 1
fi

if [ "$(cat "$PULL_DST_DIR/nested/keep.txt")" != "pull-nested" ]; then
    echo "SSH pull delete did not refresh nested/keep.txt"
    exit 1
fi

if [ -e "$PULL_DST_DIR/stale" ]; then
    echo "SSH pull delete did not remove stale entries"
    exit 1
fi

if [ -e "$PULL_DST_DIR/skip.tmp" ]; then
    echo "SSH pull ignore did not skip skip.tmp"
    exit 1
fi

echo "✅ SSH delete test passed!"
