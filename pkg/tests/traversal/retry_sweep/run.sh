#!/usr/bin/env bash
# Retry Sweep Test Runner
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later
# Test DB is DuckDB at pkg/tests/traversal/shared/main_test.db. If missing, it is generated (traversal from Spectra roots).

set -e
clear

echo "=== Retry Sweep Test Runner ==="
echo ""

destDB="pkg/tests/traversal/shared/main_test.db"

# Generate DuckDB if missing (same roots/setup as traversal shared; Spectra DBs not modified)
if [ ! -f "$destDB" ]; then
    echo "Test DB not found. Generating DuckDB (traversal from Spectra roots)..."
    go run ./cmd/gen_traversal_test_db
    echo ""
fi

startTime=$(date +%s)
echo "Running retry sweep test..."
go run ./pkg/tests/traversal/retry_sweep/main.go
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
