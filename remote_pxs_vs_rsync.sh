#!/usr/bin/env bash
# ./remote_pxs_vs_rsync.sh \
#    --source ./src.bin \
#    --host devops@10.246.0.54 \
#    --remote-root /home/devops/pxs_bench/pgdata \
#    --pxs-bin ./target/release/pxs

# Targeted remote benchmark for repeated SSH sync into pre-seeded destinations.
# Use it to compare pxs and rsync on a specific workload shape, not as a
# universal performance proof.

set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  remote_pxs_vs_rsync.sh --source PATH --host USER@HOST --remote-root PATH [options]

Required:
  --source PATH         Local source file or directory
  --host USER@HOST      Remote SSH target
  --remote-root PATH    Remote benchmark root (script uses PATH/pxs and PATH/rsync)

Options:
  --pxs-bin PATH      Local pxs binary (default: ./target/release/pxs)
  --skip-seed           Do not re-seed destinations before incremental rounds
  --no-checksum-round   Skip checksum comparison round
  --ssh-opt OPT         Extra ssh option for rsync/remote setup (repeatable)
  -h, --help            Show this help

Notes:
  - This is a workload-specific comparison helper, not a universal benchmark.
  - The script creates and resets only:
      REMOTE_ROOT/pxs
      REMOTE_ROOT/rsync
  - It does not touch other remote paths.
EOF
}

fail() {
    echo "error: $*" >&2
    exit 1
}

format_ms() {
    local ms="$1"
    local sec=$((ms / 1000))
    local rem=$((ms % 1000))
    printf '%d.%03ds' "$sec" "$rem"
}

run_timed() {
    local key="$1"
    shift
    local start_ns end_ns elapsed_ms
    start_ns="$(date +%s%N)"
    "$@"
    end_ns="$(date +%s%N)"
    elapsed_ms=$(((end_ns - start_ns) / 1000000))
    TIMES["$key"]="$elapsed_ms"
}

SOURCE=""
HOST=""
REMOTE_ROOT=""
PXS_BIN="${PXS_BIN:-./target/release/pxs}"
SKIP_SEED="false"
RUN_CHECKSUM_ROUND="true"

DEFAULT_SSH_OPTS=(
    -o Compression=no
    -o Ciphers=aes128-gcm@openssh.com,chacha20-poly1305@openssh.com,aes128-ctr
    -o IPQoS=throughput
)
EXTRA_SSH_OPTS=()

while (($#)); do
    case "$1" in
    --source)
        SOURCE="${2:-}"
        shift 2
        ;;
    --host)
        HOST="${2:-}"
        shift 2
        ;;
    --remote-root)
        REMOTE_ROOT="${2:-}"
        shift 2
        ;;
    --pxs-bin)
        PXS_BIN="${2:-}"
        shift 2
        ;;
    --skip-seed)
        SKIP_SEED="true"
        shift
        ;;
    --no-checksum-round)
        RUN_CHECKSUM_ROUND="false"
        shift
        ;;
    --ssh-opt)
        EXTRA_SSH_OPTS+=("${2:-}")
        shift 2
        ;;
    -h | --help)
        usage
        exit 0
        ;;
    *)
        fail "unknown argument: $1"
        ;;
    esac
done

[[ -n "$SOURCE" ]] || fail "--source is required"
[[ -n "$HOST" ]] || fail "--host is required"
[[ -n "$REMOTE_ROOT" ]] || fail "--remote-root is required"
[[ -e "$SOURCE" ]] || fail "source path does not exist: $SOURCE"
[[ -x "$PXS_BIN" ]] || fail "pxs binary is not executable: $PXS_BIN"

if [[ -d "$SOURCE" ]]; then
    RSYNC_SOURCE="${SOURCE%/}/"
else
    RSYNC_SOURCE="$SOURCE"
fi

PXS_DST="${REMOTE_ROOT%/}/pxs"
RSYNC_DST="${REMOTE_ROOT%/}/rsync"

SSH_CMD=(ssh "${DEFAULT_SSH_OPTS[@]}" "${EXTRA_SSH_OPTS[@]}")

RSYNC_SSH_CMD="ssh"
for opt in "${DEFAULT_SSH_OPTS[@]}" "${EXTRA_SSH_OPTS[@]}"; do
    RSYNC_SSH_CMD+=" $opt"
done

declare -A TIMES

echo "== Remote Benchmark Config =="
echo "source:       $SOURCE"
echo "host:         $HOST"
echo "remote root:  $REMOTE_ROOT"
echo "pxs bin:    $PXS_BIN"
echo "skip seed:    $SKIP_SEED"
echo "checksum rnd: $RUN_CHECKSUM_ROUND"
echo

command -v rsync >/dev/null || fail "rsync is not installed locally"
"${SSH_CMD[@]}" "$HOST" "command -v rsync >/dev/null && command -v pxs >/dev/null" ||
    fail "remote host must have rsync and pxs in PATH"

if [[ "$SKIP_SEED" == "false" ]]; then
    echo "== Seeding destinations =="
    "${SSH_CMD[@]}" "$HOST" "mkdir -p '$PXS_DST' '$RSYNC_DST'"
    run_timed "seed_pxs" \
        rsync -a --delete --inplace --no-whole-file -e "$RSYNC_SSH_CMD" "$RSYNC_SOURCE" "$HOST:$PXS_DST/"
    run_timed "seed_rsync" \
        rsync -a --delete --inplace --no-whole-file -e "$RSYNC_SSH_CMD" "$RSYNC_SOURCE" "$HOST:$RSYNC_DST/"
    echo "seed pxs dst: $(format_ms "${TIMES[seed_pxs]}")"
    echo "seed rsync dst: $(format_ms "${TIMES[seed_rsync]}")"
    echo
fi

echo "== No-change Round (no checksum) =="
run_timed "pxs_no_checksum" \
    "$PXS_BIN" sync "$SOURCE" "$HOST:$PXS_DST"
run_timed "rsync_no_checksum" \
    rsync -a --inplace --no-whole-file -e "$RSYNC_SSH_CMD" "$RSYNC_SOURCE" "$HOST:$RSYNC_DST/"
echo "pxs: $(format_ms "${TIMES[pxs_no_checksum]}")"
echo "rsync: $(format_ms "${TIMES[rsync_no_checksum]}")"
echo

if [[ "$RUN_CHECKSUM_ROUND" == "true" ]]; then
    echo "== No-change Round (checksum) =="
    run_timed "pxs_checksum" \
        "$PXS_BIN" sync "$SOURCE" "$HOST:$PXS_DST" -c
    run_timed "rsync_checksum" \
        rsync -a --checksum --inplace --no-whole-file -e "$RSYNC_SSH_CMD" "$RSYNC_SOURCE" "$HOST:$RSYNC_DST/"
    echo "pxs -c: $(format_ms "${TIMES[pxs_checksum]}")"
    echo "rsync --checksum: $(format_ms "${TIMES[rsync_checksum]}")"
    echo
fi

echo "== Summary =="
printf '%-28s %12s\n' "case" "duration"
printf '%-28s %12s\n' "pxs (no checksum)" "$(format_ms "${TIMES[pxs_no_checksum]}")"
printf '%-28s %12s\n' "rsync (no checksum)" "$(format_ms "${TIMES[rsync_no_checksum]}")"
if [[ "$RUN_CHECKSUM_ROUND" == "true" ]]; then
    printf '%-28s %12s\n' "pxs (-c)" "$(format_ms "${TIMES[pxs_checksum]}")"
    printf '%-28s %12s\n' "rsync (--checksum)" "$(format_ms "${TIMES[rsync_checksum]}")"
fi
