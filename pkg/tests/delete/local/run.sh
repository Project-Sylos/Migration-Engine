#!/bin/bash
# Delete phase local E2E test
set -euo pipefail
# use cwd instead of this nonsense
SRC=$(mktemp -d)
DST=$(mktemp -d)

# Use home directory as source
homePath="${HOME}"

echo "Source (Home): $homePath"
echo ""

# Create temporary destination folder
# Use timestamp to avoid conflicts if script is run multiple times
timestamp=$(date +%Y%m%d_%H%M%S)
tempDestPath="${TMPDIR:-/tmp}/SylosCopyTest_${timestamp}"

echo "Creating temporary destination folder..."
echo "  Path: $tempDestPath"

# Create destination folder
if ! mkdir -p "$tempDestPath"; then
    echo "ERROR: Failed to create destination folder"
    exit 1
fi
echo "  Created successfully"

echo ""

# Clean up existing test databases
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
bash "$SCRIPT_DIR/../shared/cleanup.sh"
echo ""

# Set environment variables for source and destination paths
export SYLOS_COPY_TEST_SRC="$homePath"
export SYLOS_COPY_TEST_DST="$tempDestPath"

# once this is done, create another temp folder and run a copy to move things from temp 1 to temp 2
tempDestPath2=$(mktemp -d)
export SYLOS_COPY_TEST_SRC="$tempDestPath"
export SYLOS_COPY_TEST_DST="$tempDestPath2"

# run the copy test
go run ./pkg/tests/copy/local/main.go

# run the delete test
go run ./pkg/tests/delete/local/main.go

# clean up the temp folders
rm -rf "$tempDestPath" "$tempDestPath2"