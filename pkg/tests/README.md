# Migration Engine Tests

This directory contains integration tests for the Sylos Migration Engine.

## Test Structure

The tests are organized into subdirectories:

```
pkg/tests/
├── shared/                      # Shared test utilities (package shared)
│   ├── setup.go                # Builds migration configuration
│   └── verify.go               # Report formatting
├── normal/                     # Normal migration test (package main)
│   └── main.go                 # Runs a complete migration end-to-end
├── resumption/                 # Resumption test (package main)
│   └── main.go                 # Tests shutdown and resume functionality
├── run_test.ps1               # PowerShell runner for normal test
├── run_resumption_test.ps1    # PowerShell runner for resumption test
├── run_test.sh                # Bash runner for normal test
└── README.md                   # This file
```

### Shared Package (`shared/`)

Contains common test utilities used by all test types:
- **SetupTest()**: Builds a Spectra-backed migration configuration
- **LoadSpectraRoots()**: Fetches root nodes from Spectra simulator
- **PrintVerification()**: Formats and prints verification results

### Normal Test (`normal/`)

Runs a complete migration from start to finish:
1. Sets up Spectra configuration
2. Runs full migration
3. Verifies results

### Resumption Test (`resumption/`)

Tests shutdown and resume functionality:
1. Starts a migration
2. Gets killed mid-execution (external)
3. Verifies shutdown state is saved
4. Resumes from suspended state
5. Completes migration and verifies results

## Running Tests

### Normal Test (Windows)

```powershell
./run_test.ps1
```

### Normal Test (Linux/macOS)

```bash
./run_test.sh
```

### Resumption Test (Windows)

```powershell
./run_resumption_test.ps1
```

This test will:
1. Start a migration and let it run for ~5 seconds
2. Kill the migration process (simulates Ctrl+C)
3. Verify shutdown state was saved (YAML + DB checkpoint)
4. Resume the migration automatically
5. Verify it completes successfully

## Test Phases

### Normal Test Phases

1. **Setup**: Loads Spectra configuration and builds migration config
2. **Migration**: Runs `migration.LetsMigrate(cfg)` to completion
3. **Verification**: Prints statistics and validates results

### Resumption Test Phases

1. **Phase 1 - Initial Run & Kill**:
   - Starts migration using `migration.StartMigration()`
   - Waits for progress
   - Kills the process externally
   - Verifies YAML and DB files exist

2. **Phase 2 - Resume**:
   - Starts migration again with same config (but `RemoveExisting=false`)
   - `migration.LetsMigrate()` automatically detects suspended state
   - Resumes from checkpoint
   - Completes migration
   - Verifies final state

## Test Validation

The tests validate:
- ✓ Database schema creation and table registration
- ✓ Spectra filesystem integration
- ✓ Queue coordination and round advancement
- ✓ Worker task processing (configurable worker count per queue)
- ✓ BFS traversal correctness
- ✓ Data integrity and completeness
- ✓ No pending or failed traversals
- ✓ **Shutdown and resumption** (resumption test only)

## Expected Output

### Normal Test

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

### Resumption Test

```
=== Phase 1: Starting Migration (will be killed) ===
Migration started (PID: XXXX)
Waiting for migration to progress...
⏹️  Killing migration mid-execution (sending SIGINT)...
Migration process terminated.

✓ YAML config file exists (suspended state saved)
✓ Database file exists (checkpoint saved)

=== Phase 2: Resuming Migration ===
✓ Config loaded
✓ Will resume from existing database state

🔄 Phase 2: Resuming Migration
===============================
Resuming suspended migration from database state...
  ...

Migration complete!

✓ Phase 3: Verification
========================
...

✅ TEST PASSED - Migration successfully resumed and completed!
```

## Cleanup

Test databases are automatically cleaned up by the runner scripts. To manually clean:

```powershell
Remove-Item pkg\tests\migration_test.db*
Remove-Item pkg\tests\migration_test.yaml -ErrorAction SilentlyContinue
```

## Troubleshooting

**"Config file not found"**: Ensure `pkg/configs/spectra.json` exists

**"Root tasks not seeded"**: Confirm that the Spectra simulator contains the expected root nodes and that `migration.LetsMigrate` succeeded in seeding them

**"Migration timeout"**: Check the log service window for worker errors

**Workers not processing**: Verify that `migration.LetsMigrate` seeded the queues (look for the "Root tasks seeded" log)

**"Resumption test not suspending"**: Ensure the process is killed externally (the PowerShell script handles this). If migration completes too quickly, the kill may happen after completion.
