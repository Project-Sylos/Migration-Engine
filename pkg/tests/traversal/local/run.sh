#!/bin/bash
# Sylos Migration Local Filesystem Test Runner
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later

set -u

clear

echo "=== Sylos Migration Local Filesystem Test Runner ==="
echo ""

# Clean up existing test databases
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../../" && pwd)"
bash "$SCRIPT_DIR/../shared/cleanup.sh"
echo ""

# Run the test
startTime=$(date +%s)

# Memory watcher configuration (system RAM % kill; 0 = no per-process RSS limit)
MEMWATCH_BIN="${MEMWATCH_BIN:-/home/lmaup/Code/Codeberg/Memory-Watcher/memwatch}"
MEMWATCH_MEMORY_LIMIT_PCT="${MEMWATCH_MEMORY_LIMIT_PCT:-95}"
MEMWATCH_CPU_LIMIT="${MEMWATCH_CPU_LIMIT:-0}"
MEMWATCH_INTERVAL_MS="${MEMWATCH_INTERVAL_MS:-1000}"
MEMWATCH_LOG="${MEMWATCH_LOG:-$SCRIPT_DIR/mem_local.log}"
MEMWATCH_PLOT="${MEMWATCH_PLOT:-$SCRIPT_DIR/mem_local.png}"

if [ ! -x "$MEMWATCH_BIN" ]; then
    echo "Memory watcher binary not found or not executable: $MEMWATCH_BIN"
    echo "Build it first (in /home/lmaup/Code/Codeberg/Memory-Watcher): go build ./cmd/memwatch"
    exit 1
fi

echo "Running under memwatch:"
echo "  binary: $MEMWATCH_BIN"
echo "  memory limit: ${MEMWATCH_MEMORY_LIMIT_PCT}% system RAM (kill when exceeded)"
echo "  cpu limit: ${MEMWATCH_CPU_LIMIT}% (0 = disabled)"
echo "  interval: ${MEMWATCH_INTERVAL_MS}ms"
echo "  log: $MEMWATCH_LOG"
echo "  plot: $MEMWATCH_PLOT"
echo ""

# Execute local test runner under memory watcher
(
    cd "$REPO_ROOT" || exit 1
    "$MEMWATCH_BIN" run \
        -interval "$MEMWATCH_INTERVAL_MS" \
        -cpu-limit "$MEMWATCH_CPU_LIMIT" \
        -memory-limit 0 \
        -memory-limit-pct "$MEMWATCH_MEMORY_LIMIT_PCT" \
        -log "$MEMWATCH_LOG" \
        -plot "$MEMWATCH_PLOT" \
        go run pkg/tests/traversal/local/main.go
)

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
