#!/bin/bash
set -euo pipefail

if [ ! -f tests/podman/id_ed25519 ] || [ ! -f tests/podman/id_ed25519.pub ]; then
    echo "Generating SSH keys for Podman tests..."
    ssh-keygen -t ed25519 -f tests/podman/id_ed25519 -N ""
fi
