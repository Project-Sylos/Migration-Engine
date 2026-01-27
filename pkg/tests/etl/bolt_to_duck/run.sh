#!/bin/bash
# Sylos ETL Test Runner
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later

# clear

echo "=== Sylos ETL Test Runner ==="
echo ""

# Clean up existing DuckDB file
echo "Cleaning up test databases..."

# Remove the DuckDB file if it exists
duckDBPath="pkg/tests/etl/bolt_to_duck/main-duck.db"
if [ -f "$duckDBPath" ]; then
    echo "Removing $duckDBPath file..."
    rm -f "$duckDBPath"
fi

echo "Cleanup complete"
echo ""

# Check if BoltDB exists (from traversal test)
boltDBPath="pkg/tests/etl/bolt_to_duck/main-bolt.db"
if [ ! -f "$boltDBPath" ]; then
    echo "⚠️  Warning: BoltDB file not found at $boltDBPath"
    echo "   You may need to run a traversal test first to generate the BoltDB."
    echo ""
fi

# Run the test
startTime=$(date +%s)

# Execute ETL test runner
go run pkg/tests/etl/bolt_to_duck/main/main.go

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
