#!/bin/bash
# Sylos Migration Test Runner
# Copyright 2025 Sylos contributors
# SPDX-License-Identifier: LGPL-2.1-or-later

# Colors for output
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${CYAN}=== Sylos Migration Test Runner ===${NC}"
echo ""

# Clean up existing test databases
echo -e "${YELLOW}Cleaning up test databases...${NC}"
rm -f pkg/tests/migration_test.db
rm -f pkg/tests/migration_test.db-wal
rm -f pkg/tests/migration_test.db-shm
rm -f pkg/tests/migration_test.yaml
echo -e "${GREEN}Cleanup complete${NC}"
echo ""

# Run the test
startTime=$(date +%s)

# Run the Go-based normal test runner
go run pkg/tests/normal/main.go
exitCode=$?

endTime=$(date +%s)
duration=$((endTime - startTime))

echo ""
echo -e "${CYAN}=== Test Summary ===${NC}"
echo "Duration: ${duration} seconds"

if [ $exitCode -eq 0 ]; then
    echo -e "${GREEN}Status: PASSED${NC}"
else
    echo -e "${RED}Status: FAILED${NC}"
fi

echo ""

exit $exitCode

