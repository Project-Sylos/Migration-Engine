#!/usr/bin/env bash
# Spectra FUSE mount traversal integration test.
# Requires: FUSE (linux) or macFUSE (darwin), Spectra repo sibling to Migration-Engine.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../../" && pwd)"
cd "$REPO_ROOT"

bash "$SCRIPT_DIR/../shared/cleanup.sh"
echo ""

startTime=$(date +%s)
go run ./pkg/tests/traversal/fuse/
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
