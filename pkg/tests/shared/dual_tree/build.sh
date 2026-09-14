#!/bin/bash
# build.sh
# Creates two nearly identical folder trees (~10 nodes each):
#   A/  -> SRC (shared content + 2 SRC-only items)
#   B/  -> DST (shared content + 2 DST-only items)
# Does NOT change permissions. Use deny.sh afterward for retry scenarios.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

echo "Building dual-tree fixture at: $BaseDir"

rm -rf "$BaseDir"
mkdir -p "$TreeA" "$TreeB"

# --- Shared structure (identical on A and B) ---
# Top-level files
echo "shared alpha" > "$TreeA/shared_alpha.txt"
echo "shared alpha" > "$TreeB/shared_alpha.txt"
echo "shared beta"  > "$TreeA/shared_beta.txt"
echo "shared beta"  > "$TreeB/shared_beta.txt"
echo "shared gamma" > "$TreeA/shared_gamma.txt"
echo "shared gamma" > "$TreeB/shared_gamma.txt"

# Shared folders + nested files
mkdir -p "$TreeA/dir_shared/nested" "$TreeB/dir_shared/nested"
echo "nested one" > "$TreeA/dir_shared/nested_one.txt"
echo "nested one" > "$TreeB/dir_shared/nested_one.txt"
echo "nested two" > "$TreeA/dir_shared/nested/nested_two.txt"
echo "nested two" > "$TreeB/dir_shared/nested/nested_two.txt"

mkdir -p "$TreeA/dir_mid" "$TreeB/dir_mid"
echo "mid leaf" > "$TreeA/dir_mid/leaf.txt"
echo "mid leaf" > "$TreeB/dir_mid/leaf.txt"

mkdir -p "$TreeA/dir_empty" "$TreeB/dir_empty"

# --- SRC-only (A), at least 2 ---
echo "src only 1" > "$TreeA/src_only_1.txt"
mkdir -p "$TreeA/src_only_dir"
echo "src only nested" > "$TreeA/src_only_dir/inside.txt"

# --- DST-only (B), at least 2 ---
echo "dst only 1" > "$TreeB/dst_only_1.txt"
mkdir -p "$TreeB/dst_only_dir"
echo "dst only nested" > "$TreeB/dst_only_dir/inside.txt"

count_nodes() {
  find "$1" -mindepth 1 | wc -l | tr -d ' '
}

a_count="$(count_nodes "$TreeA")"
b_count="$(count_nodes "$TreeB")"

echo ""
echo "Created:"
echo "  SRC (A): $TreeA  ($a_count nodes under A)"
echo "  DST (B): $TreeB  ($b_count nodes under B)"
echo ""
echo "Asymmetry:"
echo "  A-only: src_only_1.txt, src_only_dir/ (+ inside.txt)"
echo "  B-only: dst_only_1.txt, dst_only_dir/ (+ inside.txt)"
echo ""
echo "Configure Sylos:"
echo "  Source root      -> $TreeA"
echo "  Destination root -> $TreeB"
echo ""
echo "Next (optional retry fixture): bash $SCRIPT_DIR/deny.sh"
echo "Done."
