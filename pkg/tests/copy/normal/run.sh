#!/usr/bin/env bash
# Copy Phase Test Runner
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later
# Test DB is DuckDB at pkg/tests/copy/shared/main_test.db. If missing, it is generated (traversal from Spectra roots).

set -e
clear

echo "=== Copy Phase Test Runner ==="
echo ""

destDB="pkg/tests/copy/shared/main_test.db"

# Refresh test DB from seeded template when available.
bash pkg/tests/copy/shared/setup.sh

# Fallback: generate DuckDB if still missing (schema + roots only).
if [ ! -f "$destDB" ]; then
    echo "Test DB not found. Generating DuckDB (traversal from Spectra roots)..."
    go run ./cmd/gen_copy_test_db
    echo ""
fi

startTime=$(date +%s)
echo "Running copy phase test..."
go run ./pkg/tests/copy/normal/main.go
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
