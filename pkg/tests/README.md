# Migration Engine integration tests

Heavy **scenario** tests (Spectra, real queues, DuckDB on disk). They are **`package main`** runners under **`pkg/tests/traversal/`** and **`pkg/tests/copy/`**, not `go test` packages—run them with **`go run`** or the shell/PowerShell scripts next to each scenario.

Scenario databases use **plaintext** DuckDB files (`EncryptionKey == nil` in `db.Open`) and plaintext `oauth_credentials` rows (`tokenEncryptionKey == nil` in `GetMigration`). Production API opens pass a per-migration token key to encrypt OAuth credentials at rest inside each migration DB.

Do **not** rely on `go test ./...` for these; use the scripts when you want full validation.

---

## Layout

```
pkg/tests/
├── shared/
│   └── dual_tree/        # Local FS A/B trees: build, deny, restore, cleanup (bash)
├── traversal/
│   ├── shared/           # setup.go, verify.go, test_utils.go, spectra.json, cleanup scripts
│   ├── normal/           # Happy-path traversal (main.go, run.sh / run.ps1)
│   ├── ephemeral/        # Ephemeral Spectra mode
│   ├── autoscaler_throttle/  # Spectra chaos rate limits + autoscaler scale-back
│   ├── local/            # Local-focused runner
│   ├── resumption/       # Shutdown + resume (run_resumption_test.sh / .ps1)
│   └── retry_sweep/      # Mark retry, subtree behavior, re-run sweep (main.go, run.sh / run.ps1)
│       └── scripts_for_quick_tests/   # Wrappers -> shared/dual_tree (+ older Windows .ps1)
├── copy/
│   ├── shared/           # setup, verify, cleanup scripts
│   ├── normal/
│   └── local/
└── delete/
    └── local/
```

There is **no** top-level `pkg/tests/normal/` or `pkg/tests/retry_sweep/`—those paths live under **`traversal/`**.

### Local dual-tree permission fixtures (`shared/dual_tree`)

Manual bash helpers for real-disk SRC/DST trees (nearly identical, with SRC-only and DST-only items). Useful for UI/API checks of **traversal / copy / delete** and **retry** after permission failures:

```bash
bash pkg/tests/shared/dual_tree/build.sh
bash pkg/tests/shared/dual_tree/deny.sh      # lock A's top-level children
bash pkg/tests/shared/dual_tree/restore.sh   # unlock before retry
bash pkg/tests/shared/dual_tree/cleanup.sh
```

Point Sylos SRC at `~/sylos_dual_tree_test/A` and DST at `~/sylos_dual_tree_test/B`. Details: [`shared/dual_tree/README.md`](./shared/dual_tree/README.md).

---

## Shared utilities (`traversal/shared`, `copy/shared`)

- Build **`migration.Config`** with Spectra adapters, worker counts, log address, verification options.
- **`SetupTest`**-style helpers control whether to wipe Spectra DB and migration DuckDB before a run (see each `setup.go`).

---

## Scenarios (short)

| Path | Purpose |
|------|---------|
| **`traversal/normal`** | Fresh traversal to completion; **`migration.LetsMigrate`**. |
| **`traversal/ephemeral`** | Same idea with ephemeral Spectra config. |
| **`traversal/autoscaler_throttle`** | Ephemeral Spectra with chaos rate limits **and auth tokens**; asserts autoscaler worker step-down under throttle. Run: `./pkg/tests/traversal/autoscaler_throttle/run.sh` |
| **`traversal/local`** | Local-oriented entry (see `main.go`). |
| **`traversal/resumption`** | **`StartMigration`**, interrupt, then **`LetsMigrate`** again with `-resume` (see script + `main.go`). |
| **`traversal/retry_sweep`** | Full migration, then mark-for-retry / subtree / **`RunRetrySweep`** (engine retry sweep)—uses **DuckDB** tables and status events. |
| **`copy/normal`**, **`copy/local`** | Copy-phase exercises after a traversal DB exists. |
| **`copy/scale_down_resume`** | Spectra chaos 429s + AIMD scale-down during copy; asserts no duplicate copy success events and no `already_existed` on attempt-marked nodes. Run: `./pkg/tests/copy/scale_down_resume/run.sh` |
| **`delete/local`** | Full pipeline traversal → copy → delete; asserts source tree empty. |

---

## Running

Examples:

```bash
./pkg/tests/traversal/normal/run.sh
./pkg/tests/traversal/retry_sweep/run.sh
./pkg/tests/traversal/resumption/run_resumption_test.sh
```

Windows: use the matching **`.ps1`** files in the same folders.

---

## Troubleshooting

- Ensure **`pkg/configs/spectra.json`** (or ephemeral variant under `traversal/shared/`) matches your Spectra instance.
- Remove stale **`*.db` / `*.yaml`** artifacts under `pkg/tests` if a scenario expects a clean start (see per-test cleanup scripts).
- If resumption does not suspend, adjust timing in **`run_resumption_test.ps1`** so SIGINT arrives before traversal finishes.

---

## Outdated terminology

Older docs sometimes referred to “level-sharded” buckets and join tables **outside** DuckDB. The engine stores state in **DuckDB** (`src_nodes`, `dst_nodes`, **`src_status_events`**, **`dst_status_events`**, stats tables). When reading older notes, map them to these tables and **`pkg/db/README.md`**.
