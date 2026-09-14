#!/bin/bash
# Shared paths for dual-tree local FS fixtures (SRC=A, DST=B).
# Override with SYLOS_DUAL_TREE_BASE if needed.

# shellcheck disable=SC2034
BaseDir="${SYLOS_DUAL_TREE_BASE:-${HOME}/sylos_dual_tree_test}"
TreeA="${BaseDir}/A"
TreeB="${BaseDir}/B"

# Deny mode for lock script: 000 = no access (best for ListChildren/Open failures).
# shellcheck disable=SC2034
DENY_MODE="${SYLOS_DUAL_TREE_DENY_MODE:-000}"
# Restore mode for unlock / cleanup.
# shellcheck disable=SC2034
RESTORE_DIR_MODE="${SYLOS_DUAL_TREE_RESTORE_DIR_MODE:-755}"
# shellcheck disable=SC2034
RESTORE_FILE_MODE="${SYLOS_DUAL_TREE_RESTORE_FILE_MODE:-644}"
