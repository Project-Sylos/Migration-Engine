#!/bin/bash
# deny.sh
# Permission-locks top-level children of A/ only (files and folders).
# Locking a folder denies access to its whole subtree (recursive effect).
# Does not chmod A/ itself so the root can still be listed.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

if [ ! -d "$TreeA" ]; then
  echo "ERROR: SRC tree not found at: $TreeA"
  echo "Run build.sh first."
  exit 1
fi

echo "Denying access to top-level children of: $TreeA"
echo "  mode=$DENY_MODE"

failed=0
shopt -s nullglob
for child in "$TreeA"/* "$TreeA"/.[!.]* "$TreeA"/..?*; do
  [ -e "$child" ] || continue
  name="$(basename "$child")"
  if chmod "$DENY_MODE" "$child" 2>/dev/null; then
    echo "  denied: $name"
  else
    echo "  WARNING: could not chmod $name"
    failed=1
  fi
done
shopt -u nullglob

if [ "$failed" -ne 0 ]; then
  echo "Some children could not be locked. You may need to own those paths."
  exit 1
fi

echo ""
echo "A/ root remains listable; children should fail open/list/delete until restore.sh."
echo "B/ is unchanged: $TreeB"
echo "Done."
