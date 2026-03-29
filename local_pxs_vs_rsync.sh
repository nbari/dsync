#!/usr/bin/env bash
set -euo pipefail

# local_pxs_vs_rsync.sh - targeted local benchmark
# Compares pxs and rsync on selected fixed-block large-file workloads.
# Useful for workload-specific evaluation, not as a universal speed claim.

WORKDIR="${WORKDIR:-/tmp/pxs-bench}"
SRC="$WORKDIR/src.bin"
DST="$WORKDIR/dst.bin"
PXS_CMD="${PXS:-cargo run --release --}"

# Use --checksum to FORCE content-based comparison for the benchmark
RSYNC_FLAGS=(--checksum --no-whole-file --inplace)
PXS_FLAGS=(--checksum)

sha() { sha256sum "$1" | awk '{print $1}'; }

run_bench() {
    local label="$1"
    local tool="$2"
    local cmd="$3"

    echo -e "\n>> Benchmarking $tool ($label)"
    echo "CMD: $cmd"
    echo "Workload note: targeted comparison, not a blanket throughput claim."
    
    TIMEFORMAT="  Time: %R seconds"
    if [[ "$tool" == "pxs" ]]; then
        # Show pxs output to see the "Summary: X/Y blocks updated"
        time {
            eval "$cmd"
        }
    else
        # Keep rsync quiet
        time {
            eval "$cmd" > /dev/null 2>&1
        }
    fi

    if cmp -s "$SRC" "$DST"; then
        echo "  Result: SUCCESS (files match)"
    else
        echo "  Result: FAILURE (files differ!)"
        echo "  src sha: $(sha "$SRC")"
        echo "  dst sha: $(sha "$DST")"
        exit 1
    fi
}

setup_baseline() {
    mkdir -p "$WORKDIR"
    rm -f "$SRC" "$DST"
    echo -e "\nCreating 100MiB baseline..."
    dd if=/dev/urandom of="$SRC" bs=1M count=100 status=none
    cp -f "$SRC" "$DST"
    cmp -s "$SRC" "$DST"
}

overwrite_middle() {
    echo -e "\n--- Scenario A: Overwrite 64KiB in middle (Block-aligned) ---"
    # Overwrite at block index 160 (10MB offset)
    dd if=/dev/urandom of="$SRC" bs=64k count=1 seek=160 conv=notrunc status=none
}

prepend_byte() {
    echo -e "\n--- Scenario B: Prepend 1 byte (Data shift) ---"
    local tmp; tmp="$(mktemp)"
    printf '\x00' >"$tmp"
    cat "$tmp" "$SRC" >"${tmp}.new"
    mv -f "${tmp}.new" "$SRC"
    rm -f "$tmp"
}

main() {
    # Scenario A: Aligned Overwrite
    setup_baseline
    overwrite_middle
    run_bench "Aligned-Overwrite" "rsync" "rsync ${RSYNC_FLAGS[*]} $SRC $DST"
    
    setup_baseline
    overwrite_middle
    run_bench "Aligned-Overwrite" "pxs" "$PXS_CMD sync $SRC $DST ${PXS_FLAGS[*]}"

    # Scenario B: Data Shift
    setup_baseline
    prepend_byte
    run_bench "Data-Shift" "rsync" "rsync ${RSYNC_FLAGS[*]} $SRC $DST"
    
    setup_baseline
    prepend_byte
    run_bench "Data-Shift" "pxs" "$PXS_CMD sync $SRC $DST ${PXS_FLAGS[*]}"

    echo -e "\nBenchmark complete."
}

main
