#!/bin/bash
# Sylos Migration Test Runner
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later

clear

echo "=== Sylos Migration Test Runner ==="
echo ""

# Clean up existing test databases
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
bash "$SCRIPT_DIR/../shared/cleanup.sh"
echo ""

# Run the test
startTime=$(date +%s)

# Execute normal test runner
go run pkg/tests/traversal/normal/main.go

exitCode=$?

endTime=$(date +%s)
duration=$((endTime - startTime))

echo ""
echo "=== Test Summary ==="
echo "Duration: ${duration} seconds"

if [ $exitCode -eq 0 ]; then
    echo "Status: PASSED"
    
    # You can optionally enable the block below
    # to verify the test and Spectra DBs with the inspector.
    # NOTE: This will scan the entire migration and Spectra databases (O(n) runtime),
    # so it could take a while on large datasets!
    
    # # Run DB inspector with Spectra comparison if test passed
    # echo ""
    # echo "=== Running DB Inspector with Spectra Comparison ==="
    # # The normal test uses pkg/tests/normal/main_test.db (from shared.SetupTest)
    # if [ -f "pkg/tests/traversal/shared/main_test.db" ]; then
    #     go run cmd/inspect_db/main.go
    #     inspectExitCode=$?
    #     if [ $inspectExitCode -ne 0 ]; then
    #         echo "⚠️  DB Inspector reported issues (exit code: $inspectExitCode)"
    #     fi
    # else
    #     echo "⚠️  Database file not found (pkg/tests/traversal/shared/main_test.db), skipping inspection"
    # fi
else
    echo "Status: FAILED"
fi

echo ""

exit $exitCode
