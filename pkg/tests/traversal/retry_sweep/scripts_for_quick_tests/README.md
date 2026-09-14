# Retry Sweep Permission Test (manual)

Linux bash scripts here are **thin wrappers** around the shared dual-tree fixtures:

**Canonical scripts:** [`pkg/tests/shared/dual_tree/`](../../../shared/dual_tree/)

```bash
bash pkg/tests/shared/dual_tree/build.sh    # create A/ + B/
bash pkg/tests/shared/dual_tree/deny.sh     # lock A's top-level children
bash pkg/tests/shared/dual_tree/restore.sh  # unlock
bash pkg/tests/shared/dual_tree/cleanup.sh  # restore + delete trees
```

Or from this folder:

```bash
bash setup.sh    # -> build.sh
bash deny.sh
bash restore.sh
bash cleanup.sh
```

Default trees live at `$HOME/sylos_dual_tree_test` (`A/` = SRC, `B/` = DST): nearly identical ~10-node trees with 2+ SRC-only and 2+ DST-only items. See the shared README for traversal / copy / delete retry flows.

---

## Windows

`*.ps1` in this folder are the older smaller-tree ACL helpers (`icacls`). They still use `$env:USERPROFILE\..sylos_retry_test` and deny `A\items` as a whole. Prefer the shared bash dual_tree scripts on Linux; update or replace the `.ps1` set if you need Windows parity with the new layout.
