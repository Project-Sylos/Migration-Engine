#!/usr/bin/env bash
# Copies main.db to main_test.db in this shared folder (fresh test DB from template).
# Run from repo root: bash pkg/tests/traversal/shared/setup.sh

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="$SCRIPT_DIR/main.db"
DEST="$SCRIPT_DIR/main_test.db"

if [ ! -f "$SRC" ]; then
    echo "No main.db found at $SRC - skipping setup (tests may generate main_test.db instead)."
    exit 0
fi
echo "Copying main.db to main_test.db..."
cp -f "$SRC" "$DEST"
echo "Setup complete"
