#!/usr/bin/env bash
# Removes main_test artifacts from this shared folder (main_test.db, main_test_logs.db, main_test.yaml).
# Run from repo root: bash pkg/tests/copy/shared/cleanup.sh

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Cleaning up test databases..."
rm -f "$SCRIPT_DIR/main_test.db"
rm -f "$SCRIPT_DIR/main_test.db.wal"
rm -f "$SCRIPT_DIR/main_test.yaml"
echo "Cleanup complete"
