#!/bin/bash
# restore.sh
# Restores normal owner rwx permissions on A/ and all of its descendants.
# Safe to run even if nothing was denied.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

if [ ! -d "$TreeA" ]; then
  echo "ERROR: SRC tree not found at: $TreeA"
  echo "Run build.sh first (or nothing to restore)."
  exit 1
fi

echo "Restoring permissions under: $TreeA"

# Always unlock the root first so find can walk children that were chmod 000.
chmod "$RESTORE_DIR_MODE" "$TreeA" 2>/dev/null || chmod u+rwx "$TreeA" 2>/dev/null || true

# Depth-first restore: directories need +x before we can enter them.
# Use find -chmod when available; fall back to -exec.
if find "$TreeA" -maxdepth 0 -chmod "$RESTORE_DIR_MODE" >/dev/null 2>&1; then
  find "$TreeA" -type d -chmod "$RESTORE_DIR_MODE"
  find "$TreeA" -type f -chmod "$RESTORE_FILE_MODE"
else
  # Walk may fail on 000 dirs until we chmod them; retry a few passes.
  for _ in 1 2 3 4 5; do
    find "$TreeA" \( -type d -exec chmod "$RESTORE_DIR_MODE" {} + \) \
      -o \( -type f -exec chmod "$RESTORE_FILE_MODE" {} + \) 2>/dev/null || true
  done
  # Last resort: fix any remaining top-level children then recurse.
  shopt -s nullglob
  for child in "$TreeA"/*; do
    [ -e "$child" ] || continue
    if [ -d "$child" ]; then
      chmod -R u+rwX "$child" 2>/dev/null || true
      find "$child" -type d -exec chmod "$RESTORE_DIR_MODE" {} + 2>/dev/null || true
      find "$child" -type f -exec chmod "$RESTORE_FILE_MODE" {} + 2>/dev/null || true
    else
      chmod "$RESTORE_FILE_MODE" "$child" 2>/dev/null || true
    fi
  done
  shopt -u nullglob
fi

echo "Permissions restored for A/ (dirs=$RESTORE_DIR_MODE files=$RESTORE_FILE_MODE)."
echo "Done."
