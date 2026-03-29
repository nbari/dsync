test: clippy
  cargo test

clippy:
  cargo clippy --all-targets --all-features

podman-test-local:
  cargo test --test cli_test
  cargo test --test sync_test

podman-test:
  bash tests/podman/run_ssh_suite.sh

podman-test-ssh:
  bash tests/podman/run_ssh_suite.sh

podman-test-ssh-push:
  bash tests/podman/test_ssh_push.sh

podman-test-ssh-delete:
  bash tests/podman/test_ssh_delete.sh

podman-test-ssh-parallel:
  bash tests/podman/test_ssh_parallel_push.sh

podman-test-tcp:
  bash tests/podman/run_tcp_suite.sh

podman-test-tcp-pull:
  bash tests/podman/test_tcp_pull.sh

podman-test-tcp-dir-pull:
  bash tests/podman/test_tcp_directory_pull.sh

podman-test-tcp-resume:
  bash tests/podman/test_tcp_directory_resume.sh

podman-test-ssh-resume:
  bash tests/podman/test_ssh_pull_resume.sh

podman-test-all:
  bash tests/podman/run_all.sh
