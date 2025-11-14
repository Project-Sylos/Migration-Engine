# Migration Engine Tests

This directory contains integration tests for the Sylos Migration Engine.

## Running Tests

### Windows
```powershell
./run_test.ps1
```

### Linux/macOS
```bash
./run_test.sh
```

## Test Structure

The Spectra integration test runs in three phases driven by `pkg/migration`:

### Phase 1: Setup (`spectra/setup.go`)
- Loads Spectra simulator configuration
- Builds a `migration.Config` with database, adapter, and verification settings

### Phase 2: Migration (`spectra/test_runner.go`)
- Calls `migration.LetsMigrate(cfg)`
- Streams queue progress to STDOUT during execution

### Phase 3: Verification (`spectra/verify.go`)
- Prints summarized statistics from the `migration.Result`

## Files

```
pkg/tests/
├── run_test.ps1             # PowerShell test runner
├── run_test.sh              # Bash test runner
├── README.md                # This file
└── spectra/                 # Spectra-specific test implementation
    ├── setup.go             # Builds migration configuration
    ├── test_runner.go       # Main test orchestrator
    └── verify.go            # Report formatting
```

## Test Validation

The test validates:
- ✓ Database schema creation and table registration
- ✓ Spectra filesystem integration
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
Loading Spectra configuration...

🚀 Phase 2: Migration
=====================
Root tasks seeded (src: 1 dst: 1)
  Src: Round 0 (Pending:1 InProgress:1 Workers:10) | Dst: Round 0 (Pending:0 InProgress:0 Workers:10)
  ...

Migration complete!

✓ Phase 3: Verification
========================
Nodes discovered: src=XX, dst=XX
Traversal status:
  Src: XX Successful, 0 Pending, 0 Failed
  Dst: XX Successful, 0 Pending, 0 Failed

✓ All verification checks passed!
✓ Successfully migrated XX nodes

✅ TEST PASSED!
```

## Cleanup

Test databases are automatically cleaned up by the runner scripts. To manually clean:

```powershell
Remove-Item pkg\tests\migration_test.db*
```

## Troubleshooting

**"Config file not found"**: Ensure `pkg/configs/spectra.json` exists

**"Root tasks not seeded"**: Confirm that the Spectra simulator contains the expected root nodes and that `migration.LetsMigrate` succeeded in seeding them

**"Migration timeout"**: Check the log service window for worker errors

**Workers not processing**: Verify that `migration.LetsMigrate` seeded the queues (look for the "Root tasks seeded" log)
