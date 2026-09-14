# Dual-tree local FS fixtures (manual / UI-assisted)

Bash helpers that build two nearly identical trees for **traversal / copy / delete**
(and their retry modes) against a real local filesystem.

These are **not** automated `go test` runners. Use them beside the UI/API (or point
`SYLOS_COPY_TEST_SRC` / similar env vars at the paths) to observe permission failures,
retries, and cleanup.

Default base: `$HOME/sylos_dual_tree_test` (override with `SYLOS_DUAL_TREE_BASE`).

```
sylos_dual_tree_test/
├── A/   <- SRC root
│   ├── shared_*.txt, dir_shared/, dir_mid/, dir_empty/   (also on B)
│   ├── src_only_1.txt
│   └── src_only_dir/
└── B/   <- DST root
    ├── shared_*.txt, dir_shared/, dir_mid/, dir_empty/   (also on A)
    ├── dst_only_1.txt
    └── dst_only_dir/
```

Roughly ~10 nodes under each root. A has at least two items B lacks; B has at least
two items A lacks.

---

## Scripts

| Script | Role |
|--------|------|
| `build.sh` | Create A/ and B/ only (no permission changes) |
| `deny.sh` | `chmod` **top-level children of A/** to deny (folders + files). Root A/ stays listable; denying a folder blocks its whole subtree |
| `restore.sh` | Restore normal modes under A/ |
| `cleanup.sh` | `restore.sh` then `rm -rf` the base dir |

```bash
cd /path/to/Migration-Engine
bash pkg/tests/shared/dual_tree/build.sh
# optional: bash pkg/tests/shared/dual_tree/deny.sh
# ... run traversal / copy / delete (and retries) with SRC=A DST=B ...
bash pkg/tests/shared/dual_tree/restore.sh   # before retry if you denied
bash pkg/tests/shared/dual_tree/cleanup.sh
```

---

## Suggested flows

### Traversal retry
1. `build.sh` then `deny.sh`
2. Sylos SRC=`.../A` DST=`.../B` -> start traversal (expect failures under A's children)
3. `restore.sh`
4. Mark failed for retry / retry sweep
5. Confirm SRC discovers shared + src-only; DST-only stay `not_on_src` until relevant

### Copy retry
1. Trees built and traversable (no deny, or restore first)
2. Run copy; optionally `deny.sh` mid-flight or before a retry to force copy failures on SRC open
3. `restore.sh` then copy-retry

### Delete retry
1. After a successful copy of selected items
2. `deny.sh` so SRC deletes fail on locked children
3. `restore.sh` then delete-retry / re-run delete
4. `cleanup.sh` when finished

---

## Env overrides

| Variable | Default | Meaning |
|----------|---------|---------|
| `SYLOS_DUAL_TREE_BASE` | `$HOME/sylos_dual_tree_test` | Fixture root |
| `SYLOS_DUAL_TREE_DENY_MODE` | `000` | Mode applied to A's top-level children |
| `SYLOS_DUAL_TREE_RESTORE_DIR_MODE` | `755` | Dir mode after restore |
| `SYLOS_DUAL_TREE_RESTORE_FILE_MODE` | `644` | File mode after restore |

---

## Notes

* Linux/`chmod` oriented. Windows ACL equivalents still live under
  `traversal/retry_sweep/scripts_for_quick_tests/*.ps1` (older smaller trees).
* `deny.sh` does not lock `A/` itself on purpose: ListChildren of the SRC root can
  succeed while each top-level child fails, which matches common retry UX.
* Always `restore.sh` (or `cleanup.sh`) before deleting the fixture, or `rm` may fail
  on mode `000` directories.
