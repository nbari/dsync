test: clippy
  cargo test

clippy:
  cargo clippy --all-targets --all-features

podman-test:
  ./tests/podman/test_ssh_pull.sh
