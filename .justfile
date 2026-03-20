test: clippy
  cargo test

clippy:
  cargo clippy --all-targets --all-features

podman-test:
  ./tests/podman/test_ssh_pull.sh

podman-test-ssh-push:
  ./tests/podman/test_ssh_push.sh

podman-test-tcp:
  ./tests/podman/test_tcp_push.sh

podman-test-tcp-resume:
  ./tests/podman/test_tcp_directory_resume.sh

podman-test-ssh-resume:
  ./tests/podman/test_ssh_pull_resume.sh

podman-test-all:
  ./tests/podman/test_ssh_pull.sh
  ./tests/podman/test_ssh_push.sh
  ./tests/podman/test_ssh_pull_resume.sh
  ./tests/podman/test_tcp_push.sh
  ./tests/podman/test_tcp_directory_resume.sh
