#!/bin/bash
# Thin wrapper -> pkg/tests/shared/dual_tree/restore.sh
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
exec bash "$HERE/../../../shared/dual_tree/restore.sh" "$@"
