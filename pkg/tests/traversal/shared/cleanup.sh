#!/usr/bin/env bash
# Removes main_test artifacts from this shared folder (main_test.db, main_test.db.wal, spectra_test.db).
# Run from repo root: bash pkg/tests/traversal/shared/cleanup.sh
# Or from any dir: bash "$(dirname "$0")/cleanup.sh" (script must stay in shared).

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Cleaning up test databases..."
rm -f "$SCRIPT_DIR/main_test.db"
rm -f "$SCRIPT_DIR/main_test.db.wal"
rm -f "$SCRIPT_DIR/spectra_test.db"
echo "Cleanup complete"
