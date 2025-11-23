# Migration Engine Tests

This package houses scenario-level integration tests for the Sylos Migration Engine. Each test spins up the Spectra simulator, seeds migration state, and runs the real coordinator/workers so that pause/resume behavior can be validated end-to-end.

## Layout

```
pkg/tests/
├── shared/                     # Shared test utilities (package shared)
│   ├── setup.go               # Builds migration configs and Spectra adapters
│   └── verify.go              # Pretty-printer for migration.Result
├── normal/                     # Normal migration happy-path runner (package main)
│   └── main.go
├── resumption/                 # Suspension + resumption runner (package main)
│   └── main.go
├── migration_test.db.yaml      # Sample state file produced by resumption test
├── run_test.ps1                # Windows runner for normal test
├── run_resumption_test.ps1     # Windows runner that kills then resumes
├── run_test.sh                 # Linux/macOS runner for the normal test
└── README.md                   # This file
```

### Shared Utilities (`shared/`)

`SetupTest(cleanSpectraDB, removeMigrationDB)` wires together a Spectra-backed migration config. Cleanup flags control whether `spectra.db` (plus WAL/SHM) or `pkg/tests/migration_test.db*` are removed before the test starts, allowing the resumption test to keep on-disk state between phases. The helper internally:

- Loads Spectra roots via `LoadSpectraRoots`, mapping them into `fsservices.Folder` values
- Builds `SpectraFS` adapters for source and destination (pointing at the same Spectra instance)
- Sets migration defaults such as worker counts, queue coordinator lead, log listener address, and verification options

`PrintVerification` renders the verification summary from `migration.Result` so that pass/fail conditions are human-readable in CI logs.

### Normal Test (`normal/`)

`pkg/tests/normal/main.go` drives a fresh migration start to finish:

1. **Setup** – calls `shared.SetupTest(true, true)` to guarantee a clean Spectra DB and migration DB.
2. **Execution** – invokes `migration.LetsMigrate(cfg)` and waits for completion.
3. **Verification** – prints traversal counts and failures via `shared.PrintVerification`.

This test exercises the happy-path traversal, queue coordination, verification, and log listener plumbing without any artificial shutdowns.

### Resumption Test (`resumption/`)

`pkg/tests/resumption/main.go` focuses on force-shutdown and resume:

- The default mode (`runInitialTest`) starts a migration via `migration.StartMigration`, lets workers make progress, then expects an external killswitch. When the process receives SIGINT/Stop-Process, the controller should surface `migration suspended by force shutdown`, confirming that DB checkpoints, YAML state, and Spectra adapters were closed cleanly.
- The `-resume` flag (`runResumeTest`) skips cleanup by calling `shared.SetupTest(false, false)`, then reruns `migration.LetsMigrate`. The migration should detect the `suspended` status in `migration_test.db.yaml`, reload the database, and finish the remaining work.

### Runner Scripts

- `run_test.ps1` – convenience wrapper for Windows; cleans previous DB artifacts and runs the normal test.
- `run_test.sh` – bash equivalent for Linux/macOS environments; run via `./run_test.sh` (or `go run pkg/tests/normal/main.go` directly if preferred).
- `run_resumption_test.ps1` – orchestrates the two-phase shutdown test:
  1. Launches the resumption runner (initial phase), waits ~10 seconds, then sends SIGINT/KILL to simulate an operator flipping the killswitch.
  2. Verifies YAML/DB artifacts exist.
  3. Re-runs the same binary with `-resume` so the migration resumes from disk.
  4. Cleans up artifacts only after a successful resume to preserve evidence on failure.

## Test Methodology

These runners validate the following behaviors:

- SQLite schema initialization, WAL checkpointing, and YAML state persistence
- Spectra simulator fidelity (seed data preserved across process restarts)
- Queue/coordinator orchestration, including worker shutdown when `ShutdownContext` is cancelled
- BFS traversal correctness under configurable worker counts and retry policy
- Log listener fan-out (port reuse guards prevent duplicate listeners)
- Forced shutdown safety: DB checkpoints, YAML `suspended` status, Spectra adapter closing, and subsequent resume correctness

## Expected Output

Both runners print phase banners (Setup → Migration → Verification plus Resume phases) and end with either `✅ TEST PASSED!` or a detailed failure reason. See the Windows PowerShell scripts for concrete examples of the log streams that CI should capture.

## Cleanup

The PowerShell runners remove `pkg/tests/migration_test.db*` and `pkg/tests/migration_test.yaml` during setup (and after success for the resumption test). For manual cleanup:

```powershell
Remove-Item pkg\tests\migration_test.db*
Remove-Item pkg\tests\migration_test.yaml -ErrorAction SilentlyContinue
Remove-Item spectra.db* -ErrorAction SilentlyContinue  # only if you want to reseed Spectra
```

## Troubleshooting

- **"Config file not found"** – ensure `pkg/configs/spectra.json` exists and points at a running Spectra instance.
- **"Root tasks not seeded"** – Spectra seed data might be missing; rerun with `cleanSpectraDB=true`.
- **"Migration timeout" or idle workers** – inspect the log listener terminal for queue/worker errors.
- **"Resumption test not suspending"** – confirm the external kill happened before the migration finished; adjust the sleep in `run_resumption_test.ps1` if needed.
- **Missing Spectra nodes after resume** – check that the Spectra SDK process flushed WAL files (look for `spectra.db-wal` lingering); rerun with additional logging if killswitch timing is suspicious.
