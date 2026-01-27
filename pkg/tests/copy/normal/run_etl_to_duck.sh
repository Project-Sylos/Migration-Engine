#!/bin/bash
# BoltDB to DuckDB ETL Script
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later

# clear

echo "=== BoltDB to DuckDB ETL Tool ==="
echo ""

# Check if source database exists
sourceDB="pkg/tests/copy/shared/main_test.db"

if [ ! -f "$sourceDB" ]; then
    echo "ERROR: Source database not found: $sourceDB"
    echo "Please run the copy test first (run.sh) to generate main_test.db"
    exit 1
fi

echo "Source BoltDB: $sourceDB"
echo ""

# Run the ETL tool
startTime=$(date +%s)

echo "Running ETL migration..."
go run pkg/tests/copy/normal/etl/main.go

exitCode=$?
endTime=$(date +%s)
duration=$((endTime - startTime))

echo ""
echo "=== ETL Summary ==="
echo "Duration: ${duration} seconds"

if [ $exitCode -eq 0 ]; then
    echo "Status: SUCCESS"
    echo ""
    echo "DuckDB file created: pkg/tests/copy/shared/main_test-duck.db"
    echo "You can now open this file in DBeaver or another DuckDB client."
else
    echo "Status: FAILED"
fi

echo ""

exit $exitCode
