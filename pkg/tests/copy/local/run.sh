#!/bin/bash
# Local Copy Phase Test Runner
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later

clear

echo "=== Local Copy Phase Test Runner ==="
echo ""

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

# Run the test
startTime=$(date +%s)

echo "Running local copy phase test..."
echo ""

# Execute local test runner
go run pkg/tests/copy/local/main.go

exitCode=$?

endTime=$(date +%s)
duration=$((endTime - startTime))

echo ""
echo "=== Test Summary ==="
echo "Duration: ${duration} seconds"

if [ $exitCode -eq 0 ]; then
    echo "Status: PASSED"
else
    echo "Status: FAILED"
fi

echo ""

# Cleanup: Delete temporary destination folder
echo "Cleaning up temporary destination folder..."
if [ -d "$tempDestPath" ]; then
    if rm -rf "$tempDestPath" 2>/dev/null; then
        echo "  Deleted successfully"
    else
        echo "WARNING: Failed to delete destination folder: $tempDestPath"
        echo "  You may need to delete it manually"
    fi
fi

echo ""

# Clear environment variables
unset SYLOS_COPY_TEST_SRC
unset SYLOS_COPY_TEST_DST

exit $exitCode
