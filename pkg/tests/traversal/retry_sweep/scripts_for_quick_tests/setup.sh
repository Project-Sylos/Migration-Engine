#!/bin/bash
# Thin wrapper: build trees then deny A's children (legacy setup flow).
# Prefer calling shared scripts directly when you want build without lock:
#   bash pkg/tests/shared/dual_tree/build.sh
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
DUAL="$HERE/../../../shared/dual_tree"
bash "$DUAL/build.sh"
bash "$DUAL/deny.sh"
