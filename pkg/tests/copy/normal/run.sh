#!/bin/bash
# Copy Phase Test Runner
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later

clear

echo "=== Copy Phase Test Runner ==="
echo ""

# Source files (pre-provisioned test data with traversal complete)
sourceDB="pkg/tests/copy/shared/main.db"
sourceYAML="pkg/tests/copy/shared/main.yaml"
sourceSpectra="pkg/tests/copy/shared/spectra.db"

# Destination files (mutable test objects)
destDB="pkg/tests/copy/shared/main_test.db"
destYAML="pkg/tests/copy/shared/main_test.yaml"
destSpectra="pkg/tests/copy/shared/spectra_test.db"

# Check if source files exist
if [ ! -f "$sourceDB" ]; then
    echo "❌ ERROR: Source database not found: $sourceDB"
    echo "Please provide a pre-provisioned test database with traversal complete."
    exit 1
fi

if [ ! -f "$sourceYAML" ]; then
    echo "⚠️  WARNING: Source YAML not found: $sourceYAML (continuing anyway)"
fi

if [ ! -f "$sourceSpectra" ]; then
    echo "⚠️  WARNING: Source Spectra DB not found: $sourceSpectra (continuing anyway)"
fi

# Clean up previous test files
echo "Cleaning up previous test files..."
if [ -f "$destDB" ]; then
    rm -f "$destDB"
fi
rm -f "${destDB%.db}_logs.db"
if [ -f "$destYAML" ]; then
    rm -f "$destYAML"
fi
if [ -f "$destSpectra" ]; then
    rm -f "$destSpectra"
fi

# Copy source files to destination
echo "Copying test files..."
cp -f "$sourceDB" "$destDB"
echo "  Copied: $sourceDB -> $destDB"

if [ -f "$sourceYAML" ]; then
    cp -f "$sourceYAML" "$destYAML"
    echo "  Copied: $sourceYAML -> $destYAML"
fi

if [ -f "$sourceSpectra" ]; then
    cp -f "$sourceSpectra" "$destSpectra"
    echo "  Copied: $sourceSpectra -> $destSpectra"
fi

echo ""

# Run the test
startTime=$(date +%s)

echo "Running copy phase test..."
go run pkg/tests/copy/normal/main.go

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
