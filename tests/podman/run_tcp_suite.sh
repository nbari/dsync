#!/bin/bash
set -euo pipefail

bash tests/podman/test_tcp_push.sh
bash tests/podman/test_tcp_pull.sh
bash tests/podman/test_tcp_directory_resume.sh
bash tests/podman/test_tcp_directory_pull.sh
