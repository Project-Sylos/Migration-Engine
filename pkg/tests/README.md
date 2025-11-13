# Migration Engine Tests

This directory contains integration tests for the Sylos Migration Engine.

## Running Tests

### Windows
```powershell
.\run_test.ps1
```

Or directly:
```powershell
cd main
go run setup.go test_runner.go verify.go
```

### Linux/macOS
```bash
cd main
go run setup.go test_runner.go verify.go
```

## Test Structure

The test suite is organized in three phases:

### Phase 1: Setup (`main/setup.go`)
- Creates SQLite database (`migration_test.db`)
- Registers tables (src_nodes, dst_nodes, logs)
- Initializes Spectra filesystem from `configs/spectra_test.json`
- Seeds root tasks to database (for logging/persistence)
- Initial tasks are then injected directly into queue round 0 via `seedQueueRootTasks()`

### Phase 2: Migration (`main/test_runner.go`)
- Starts logging service (spawns separate terminal window)
- Creates filesystem adapters for src (p-root) and dst (s1-root)
- Creates coordinator with maxLead (typically 4, must be at least 3)
- Configures queues with 3 src and 3 dst workers
- Monitors migration progress every 2 seconds
- Runs with 30-second timeout protection

### Phase 3: Verification (`main/verify.go`)
- Checks that nodes were discovered
- Validates src/dst node count match
- Ensures no pending traversals remain
- Reports any failed traversals
- Confirms migration completed successfully

## Files

```
pkg/tests/
├── run_test.ps1           # PowerShell test runner
├── README.md              # This file
└── main/
    ├── test_runner.go     # Main test orchestrator
    ├── setup.go           # Database and root task setup
    └── verify.go          # Post-migration verification
```

## Test Validation

The test validates:
- ✓ Database schema creation and table registration
- ✓ Spectra filesystem integration (shared instance)
- ✓ Queue coordination and round advancement
- ✓ Worker task processing (configurable worker count per queue)
- ✓ BFS traversal correctness
- ✓ Data integrity and completeness
- ✓ No pending or failed traversals

## Expected Output

```
=== Spectra Migration Test Runner ===

📋 Phase 1: Setup
================
Creating database...
Registering tables...
✓ Tables registered
Initializing Spectra filesystem...
✓ Spectra initialized
Seeding root tasks...
✓ Root tasks seeded and verified (src: 1, dst: 1)

🚀 Phase 2: Migration
=====================
Starting logging service...
✓ Log service started

Creating filesystem adapters...
✓ Adapters ready

Starting coordinator...
✓ Coordinator started

Monitoring migration progress...
  [2.0s] Src: 5 pending, 2 active | Dst: 0 pending, 0 active
  [4.0s] Src: 0 pending, 0 active | Dst: 3 pending, 1 active
  ...

✓ Migration completed in X.Xs

✓ Phase 3: Verification
========================
Nodes discovered: src=XX, dst=XX
Traversal status:
  Src: XX completed, 0 pending, 0 failed
  Dst: XX completed, 0 pending, 0 failed

✓ All verification checks passed!
✓ Successfully migrated XX nodes

✅ TEST PASSED!

=== Test Summary ===
Duration: X.X seconds
Status: PASSED
```

## Cleanup

Test databases are automatically cleaned up by `run_test.ps1`. To manually clean:

```powershell
Remove-Item pkg\tests\main\migration_test.db*
```

## Troubleshooting

**"Config file not found"**: Ensure `pkg/configs/spectra.json` exists

**"Root tasks not seeded"**: Check that Spectra has `p-root` and `s1-root` nodes configured and that queue seeding completed successfully

**"Migration timeout after 30s"**: Check the log service window for worker errors

**Workers not processing**: Verify root tasks were seeded into queues (check that `seedQueueRootTasks()` completed successfully)

## Adding More Tests

To add new integration tests:
1. Create a new `.go` file in the `main/` directory
2. Implement your test logic following the three-phase pattern
3. Update `test_runner.go` to orchestrate your test
4. Update this README with instructions
