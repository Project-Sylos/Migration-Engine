#!/bin/bash
# Sylos Migration Test Runner - Ephemeral Mode
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later

clear

echo "=== Sylos Migration Test Runner (Ephemeral Mode) ==="
echo ""

# Clean up existing test databases
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
bash "$SCRIPT_DIR/../shared/cleanup.sh"
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
