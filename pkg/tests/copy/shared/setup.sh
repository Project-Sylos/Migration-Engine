#!/usr/bin/env bash
# Copies main.db to main_test.db in this shared folder (fresh test DB from template).
# Run from repo root: bash pkg/tests/copy/shared/setup.sh

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="$SCRIPT_DIR/main.db"
DEST="$SCRIPT_DIR/main_test.db"
DEST_WAL="$DEST.wal"

if [ ! -f "$SRC" ]; then
    echo "No main.db found at $SRC - skipping setup (tests may generate main_test.db instead)."
    exit 0
fi
echo "Copying main.db to main_test.db..."
# Clear stale WAL before replacing the base DB file.
rm -f "$DEST_WAL"
cp -f "$SRC" "$DEST"
echo "Setup complete"
