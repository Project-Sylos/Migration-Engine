#!/bin/bash
# Sylos Migration Resumption Test Runner
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later
#
# This test verifies that migrations can be cleanly shutdown and resumed:
# 1. Starts a migration
# 2. Kills it midway through (force shutdown)
# 3. Resumes the same migration with same config
# 4. Verifies it completes successfully

echo "=== Sylos Migration Resumption Test Runner ==="
echo ""

# Clean up existing test databases ONLY at the start for a fresh test
# The DB will persist between Phase 1 (kill) and Phase 2 (resume)
# Final cleanup happens at the end after verification
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
echo "Cleaning up test databases for fresh test run..."
bash "$SCRIPT_DIR/../shared/cleanup.sh"
echo ""

# Phase 1: Start migration and kill it midway
echo "=== Phase 1: Starting Migration (will be killed) ==="
echo ""

startTime=$(date +%s)

echo "Starting migration..."

# Start the process in background and capture its PID
go run pkg/tests/traversal/resumption/main.go &
processId=$!

echo "Migration started (PID: $processId)"
echo "Waiting for migration to progress..."

# Wait for migration to make some progress (let it run for a few seconds)
sleep 10

echo ""
echo "Killing migration mid-execution (sending SIGINT)..."

# Send SIGINT to the process (Ctrl+C equivalent)
if kill -0 "$processId" 2>/dev/null; then
    kill -INT "$processId" 2>/dev/null || kill -TERM "$processId" 2>/dev/null
    # Wait a moment for the process to handle the signal
    sleep 2
    # Force kill if still running
    if kill -0 "$processId" 2>/dev/null; then
        kill -KILL "$processId" 2>/dev/null
    fi
    # Wait for process to fully terminate
    wait "$processId" 2>/dev/null
fi

echo "Migration process terminated. Will resume"

endTime=$(date +%s)
phase1Duration=$((endTime - startTime))

echo ""
echo "Phase 1 Duration: ${phase1Duration} seconds"
echo ""

# Verify shutdown state was saved
if [ -f "pkg/tests/traversal/shared/main_test.yaml" ]; then
    echo "YAML config file exists (suspended state saved)"
    
    # Read YAML to check status
    if grep -qE 'state:\s*\n\s*status\s*:\s*(suspended|running)' "pkg/tests/traversal/shared/main_test.yaml" 2>/dev/null; then
        echo "Migration state indicates suspension or ready to resume"
    else
        echo "Warning: YAML status not found or unexpected"
    fi
else
    echo "YAML config file not found - shutdown may not have saved state!"
    exit 1
fi

if [ -f "pkg/tests/traversal/shared/main_test.db" ]; then
    echo "Database file exists (checkpoint saved)"
else
    echo "Database file not found!"
    exit 1
fi

echo ""
echo "=== Phase 2: Resuming Migration ==="
echo ""

# Phase 2: Resume the migration
resumeStartTime=$(date +%s)

echo "Starting migration with same config (should auto-resume)..."

# Run the resumption test runner (it will resume and complete)
go run pkg/tests/traversal/resumption/main.go -resume

exitCode=$?

resumeEndTime=$(date +%s)
phase2Duration=$((resumeEndTime - resumeStartTime))
totalDuration=$((resumeEndTime - startTime))

echo ""
echo "=== Test Summary ==="
echo "Phase 1 (Initial + Kill): ${phase1Duration} seconds"
echo "Phase 2 (Resume + Complete): ${phase2Duration} seconds"
echo "Total Duration: ${totalDuration} seconds"
echo ""

if [ $exitCode -eq 0 ]; then
    echo "TEST PASSED - Migration successfully resumed and completed!"
    
    # Verify final state
    if [ -f "pkg/tests/traversal/shared/main_test.yaml" ]; then
        if grep -qE 'status:\s*completed' "pkg/tests/traversal/shared/main_test.yaml" 2>/dev/null; then
            echo "Final YAML status is 'completed'"
        else
            echo "Warning: Final status may not be 'completed'"
        fi
    fi
    
    # Clean up test databases after successful test completion
    echo ""
    bash "$SCRIPT_DIR/../shared/cleanup.sh"
else
    echo "TEST FAILED - Migration resumption did not complete successfully"
    echo "Test databases preserved for inspection"
fi

echo ""

exit $exitCode
