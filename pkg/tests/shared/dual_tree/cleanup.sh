#!/bin/bash
# cleanup.sh
# Restores permissions on A/ (so delete can succeed), then removes the dual-tree base dir.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

echo "Cleaning up dual-tree fixture at: $BaseDir"

if [ -d "$TreeA" ]; then
  echo "Restoring permissions before delete..."
  bash "$SCRIPT_DIR/restore.sh" || true
fi

if [ -d "$BaseDir" ]; then
  rm -rf "$BaseDir"
  echo "Removed: $BaseDir"
else
  echo "Nothing to remove (missing: $BaseDir)."
fi

echo "Cleanup complete."
