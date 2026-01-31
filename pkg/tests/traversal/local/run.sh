#!/bin/bash
# Sylos Migration Local Filesystem Test Runner
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later

clear

echo "=== Sylos Migration Local Filesystem Test Runner ==="
echo ""

# Clean up existing test databases
echo "Cleaning up test databases..."
mainDB="pkg/tests/traversal/shared/main_test.db"

# Remove the BoltDB file if it exists
if [ -f "$mainDB" ]; then
    echo "Removing $mainDB file..."
    rm -f "$mainDB"
fi
rm -f "${mainDB%.db}_logs.db"

# Remove the migration config YAML file if it exists
if [ -f "pkg/tests/traversal/shared/main_test.yaml" ]; then
    echo "Removing pkg/tests/traversal/shared/main_test.yaml file..."
    rm -f "pkg/tests/traversal/shared/main_test.yaml"
fi

echo "Cleanup complete"
echo ""

# Run the test
startTime=$(date +%s)

# Execute local test runner
go run pkg/tests/traversal/local/main.go

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

exit $exitCode
