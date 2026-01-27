#!/bin/bash
# Sylos Migration Test Runner - Ephemeral Mode
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later

clear

echo "=== Sylos Migration Test Runner (Ephemeral Mode) ==="
echo ""

# Clean up existing test databases
echo "Cleaning up test databases..."

# Remove the BoltDB file if it exists
if [ -f "pkg/tests/traversal/shared/main_test.db" ]; then
    echo "Removing pkg/tests/traversal/shared/main_test.db file..."
    rm -f "pkg/tests/traversal/shared/main_test.db"
fi

# Remove the migration config YAML file if it exists
if [ -f "pkg/tests/traversal/shared/main_test.yaml" ]; then
    echo "Removing pkg/tests/traversal/shared/main_test.yaml file..."
    rm -f "pkg/tests/traversal/shared/main_test.yaml"
fi

# Note: Ephemeral mode doesn't use a Spectra DB, so no cleanup needed for spectra_test.db

echo "Cleanup complete"
echo ""

# Run the test
startTime=$(date +%s)

# Execute ephemeral test runner
go run pkg/tests/traversal/ephemeral/main.go

exitCode=$?

endTime=$(date +%s)
duration=$((endTime - startTime))

echo ""
echo "=== Test Summary ==="
echo "Duration: ${duration} seconds"

if [ $exitCode -eq 0 ]; then
    echo "Status: PASSED"
    
    # Note: Ephemeral mode doesn't persist a Spectra DB, so we can't do
    # cross-validation with Spectra. The test validates only the internal
    # Sylos migration database (no pending nodes, no NotOnSrc nodes).
else
    echo "Status: FAILED"
fi

echo ""

exit $exitCode
