#!/bin/bash
# Sylos ETL Test Runner (Duck to Bolt)
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later

# clear

echo "=== Sylos ETL Test Runner (Duck to Bolt) ==="
echo ""

# Clean up existing BoltDB file
echo "Cleaning up test databases..."

# Remove the BoltDB file if it exists
boltDBPath="pkg/tests/etl/duck_to_bolt/main-bolt.db"
if [ -f "$boltDBPath" ]; then
    echo "Removing $boltDBPath file..."
    rm -f "$boltDBPath"
fi

echo "Cleanup complete"
echo ""

# Check if DuckDB exists (source)
duckDBPath="pkg/tests/etl/duck_to_bolt/main-duck.db"
if [ ! -f "$duckDBPath" ]; then
    echo "⚠️  Warning: DuckDB file not found at $duckDBPath"
    echo "   The DuckDB file should exist as the source for this test."
    echo ""
fi

# Run the test
startTime=$(date +%s)

# Execute ETL test runner
go run pkg/tests/etl/duck_to_bolt/main/main.go

exitCode=$?

endTime=$(date +%s)
duration=$((endTime - startTime))

echo ""
echo "=== Test Summary ==="
echo "Duration: ${duration} seconds"
echo ""

if [ $exitCode -eq 0 ]; then
    echo "Status: PASSED"
else
    echo "Status: FAILED"
fi

echo ""

exit $exitCode
