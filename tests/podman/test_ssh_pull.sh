#!/bin/bash
set -e

# Build dsync release
cargo build --release

# Ensure SSH keys exist
if [ ! -f tests/podman/id_ed25519 ]; then
    echo "Generating SSH keys for test..."
    ssh-keygen -t ed25519 -f tests/podman/id_ed25519 -N ""
fi

# Use podman or docker
DOCKER=${DOCKER:-podman}

# Image name
IMAGE="dsync-ssh-test"
NETWORK="dsync-net"

# Cleanup
cleanup() {
    echo "Cleaning up..."
    $DOCKER rm -f dsync-server dsync-client 2>/dev/null || true
    $DOCKER network rm $NETWORK 2>/dev/null || true
}
trap cleanup EXIT

# Create network
$DOCKER network rm $NETWORK 2>/dev/null || true
$DOCKER network create $NETWORK

# Build image
echo "Building test image..."
$DOCKER build -t $IMAGE -f tests/podman/Containerfile tests/podman/

# Start server
echo "Starting SSH server..."
$DOCKER run -d --name dsync-server \
    --network $NETWORK \
    -v $(pwd)/target/release/dsync:/usr/local/bin/dsync:ro \
    $IMAGE

# Wait for SSH to be ready
echo "Waiting for SSH server to be ready..."
sleep 2

# Create a test file on server
echo "Creating test file on server..."
$DOCKER exec dsync-server bash -c "mkdir -p /home/devops/Movies && dd if=/dev/urandom of=/home/devops/Movies/test.mkv bs=1M count=100 && chown -R devops:devops /home/devops/Movies"

# Start client and pulling file
echo "Starting client and pulling file..."
# We mount the private key and the dsync binary
# We configure SSH to use the key and skip host key checking
# We use -t to allocate a TTY so indicatif shows the progress bar
$DOCKER run --name dsync-client \
    -t \
    --network $NETWORK \
    -v $(pwd)/target/release/dsync:/usr/local/bin/dsync:ro \
    -v $(pwd)/tests/podman/id_ed25519:/tmp/id_ed25519:ro \
    $IMAGE \
    bash -c "mkdir -p /home/devops/.ssh && \
             cp /tmp/id_ed25519 /home/devops/.ssh/id_ed25519 && \
             chown devops:devops /home/devops/.ssh/id_ed25519 && \
             chmod 600 /home/devops/.ssh/id_ed25519 && \
             echo -e 'Host dsync-server\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n  IdentityFile ~/.ssh/id_ed25519' > /home/devops/.ssh/config && \
             chown devops:devops /home/devops/.ssh/config && \
             sudo -u devops dsync --remote devops@dsync-server:\"/home/devops/Movies/test.mkv\" \
             --destination /home/devops/ \
             --pull \
             -vv"

# Check exit code of previous command implicitly by 'set -e'
echo "✅ SSH Pull test passed!"
