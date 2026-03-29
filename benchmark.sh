#!/bin/bash

# Targeted local benchmark for large-file sync behavior.
# This script is useful for comparing pxs and rsync on one specific workload
# shape. It is not a universal performance claim.

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}Building pxs in release mode...${NC}"
cargo build --release

PXS_BIN="./target/release/pxs"
SRC="bench_source.bin"
DST_PXS="bench_dest_pxs.bin"
DST_RSYNC="bench_dest_rsync.bin"

echo -e "${BLUE}Creating 1GB source file...${NC}"
dd if=/dev/urandom of=$SRC bs=1M count=1024 status=progress

echo -e "${BLUE}Benchmark scope: large local file sync with identical correctness expectations.${NC}"

echo -e "\n${GREEN}--- Initial Sync (Full Copy) ---${NC}"

echo -e "${BLUE}Running pxs...${NC}"
time $PXS_BIN sync $SRC $DST_PXS

echo -e "\n${BLUE}Running rsync...${NC}"
time rsync -ah --progress $SRC $DST_RSYNC

echo -e "\n${GREEN}--- Incremental Sync (Small Change - mtime differs) ---${NC}"

echo -e "${BLUE}Modifying one 64KB block in the middle of source...${NC}"
dd if=/dev/zero of=$SRC bs=64K count=1 seek=8192 conv=notrunc

echo -e "\n${BLUE}Running pxs (Incremental)...${NC}"
time $PXS_BIN sync $SRC $DST_PXS

echo -e "\n${BLUE}Running rsync (Incremental)...${NC}"
time rsync -ah --inplace --no-whole-file --progress $SRC $DST_RSYNC

echo -e "\n${GREEN}--- Metadata Sync (No Change - should be instant) ---${NC}"

echo -e "${BLUE}Running pxs (No change)...${NC}"
time $PXS_BIN sync $SRC $DST_PXS

echo -e "\n${BLUE}Running rsync (No change)...${NC}"
time rsync -ah --inplace --no-whole-file --progress $SRC $DST_RSYNC

# Cleanup
echo -e "\n${BLUE}Cleaning up benchmark files...${NC}"
rm $SRC $DST_PXS $DST_RSYNC
