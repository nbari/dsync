#!/bin/bash
set -euo pipefail

bash tests/podman/run_ssh_suite.sh
bash tests/podman/run_tcp_suite.sh
