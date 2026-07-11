#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
cd "$ROOT"

if [[ -z "${SYLOS_SFTP_TEST_HOST:-}" ]]; then
  echo "Skipping SFTP traversal test: set SYLOS_SFTP_TEST_HOST (and related SYLOS_SFTP_TEST_* vars)."
  exit 0
fi

export SYLOS_SFTP_TEST_SRC="${SYLOS_SFTP_TEST_SRC:-/home/sylos_retry_test/A}"
export SYLOS_SFTP_TEST_DST="${SYLOS_SFTP_TEST_DST:-/home/sylos_retry_test/B}"

bash pkg/tests/traversal/shared/cleanup.sh
go run ./pkg/tests/traversal/sftp/main.go
