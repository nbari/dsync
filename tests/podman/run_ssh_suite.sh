#!/bin/bash
set -euo pipefail

bash tests/podman/test_ssh_pull.sh
bash tests/podman/test_ssh_push.sh
bash tests/podman/test_ssh_pull_resume.sh
bash tests/podman/test_ssh_delete.sh
bash tests/podman/test_ssh_parallel_push.sh
