#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."
go run ./pkg/tests/traversal/local_classify/
