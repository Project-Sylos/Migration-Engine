# Retry Sweep Permission Test (Windows)

This folder contains **manual test utilities** for validating Sylos retry sweep behavior using **real Windows filesystem permissions**.

These scripts intentionally create an asymmetric SRC/DST scenario where one tree is inaccessible during initial traversal, then later restored to verify that retry logic correctly reconciles state.

This is **not** an automated test. It is meant to be run alongside the UI + API to observe real behavior.

---

## What the scripts do

The scripts create two identical directory trees:

```
sylos_retry_test/
├── A/items   ← SRC-like (permission denied initially)
└── B/items   ← DST-like (always accessible)
```

Both trees contain the same structure and files.
Access to `A/items` is denied to simulate an admin-only or permission-restricted folder.

---

## Scripts

### `setup.ps1`

Creates both folder trees and **denies access to `A/items` only**.

Use this **before** the initial Sylos traversal.

**Effect:**

* SRC cannot traverse `A/items`
* DST fully traverses `B/items`
* DST items may appear as `not_on_src`

---

### `restore.ps1`

Restores permissions on `A/items`.

Run this **after** the initial traversal and **before** running a retry sweep.

This simulates a user fixing permissions.

---

### `cleanup.ps1`

Restores permissions (if needed) and deletes the entire test directory.

Use this to reset state after testing.

---

## How to use (recommended flow)

1. Run `setup.ps1`
2. Configure Sylos:

   * SRC → `sylos_retry_test\A`
   * DST → `sylos_retry_test\B`
3. Run initial traversal in the UI
   Expect traversal failures under `A/items`
4. Run `restore.ps1`
5. Run retry sweep in the UI
6. Verify:

   * SRC subtree is discovered
   * `not_on_src` DST items resolve correctly
7. Run `cleanup.ps1`

---

## Notes

* These scripts are **Windows-only**
* They rely on NTFS ACLs via `icacls`
* No Sylos internals are modified
* Engine-level retry correctness is validated separately by automated tests
