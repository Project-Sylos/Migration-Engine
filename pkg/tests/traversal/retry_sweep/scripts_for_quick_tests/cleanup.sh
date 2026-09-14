#!/bin/bash
# Thin wrapper -> pkg/tests/shared/dual_tree/cleanup.sh
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
exec bash "$HERE/../../../shared/dual_tree/cleanup.sh" "$@"
