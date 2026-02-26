# Migration Package

Note: this package is currently in transition to an engine-centric lifecycle model.
For the latest architecture and API usage guidance, start with:

- `docs/ENGINE_ARCHITECTURE_OVERVIEW.md`
- `docs/API_USAGE_GUIDE.md`
- `docs/ENGINE_API_CUTOVER.md`

The **migration** package orchestrates the Migration Engine: it prepares or accepts the database (`pkg/db`), builds config (including YAML state), runs traversal and copy via `pkg/queue`, and performs verification. All persistence uses a single database instance (`*db.DB`, DuckDB).

---

## Overview

- **Database**: Uses `*db.DB` from `pkg/db`—either opened by the engine at `Database.Path` or supplied by the caller (`Config.DatabaseInstance`). The same DB holds nodes, staging, stats, and logs for traversal and copy.
- **Configuration**: YAML config files serialize/deserialize migration state (roots, status, options); config path defaults to `{Database.Path}.yaml` (e.g. `migration.duckdb.yaml`).
- **State and verification**: `InspectMigrationStatus(database)` and `VerifyMigration(database, opts)` read from the DB’s node and stats tables (see `pkg/db`).
- **Execution**: Coordinates queue setup, root seeding, traversal, copy (when applicable), and verification; supports resume from saved YAML state.

---

## Core Components

### Config

The `Config` struct aggregates all parameters required to run a migration:

```go
type Config struct {
    DatabaseInstance *db.DB  // If nil, engine opens DB at Database.Path
    CloseWhenDone    bool    // If true and engine opened the DB, close it when done
    Database         DatabaseConfig
    Source           Service
    Destination      Service
    SeedRoots        bool
    WorkerCount      int
    MaxRetries       int
    CoordinatorLead  int
    LogAddress       string
    LogLevel         string
    Verification     VerifyOptions
    // ... (ConfigPath, YAMLConfig, ShutdownContext, etc.)
}
```

### Service

Represents a filesystem service participating in the migration:

```go
type Service struct {
    Name    string
    Adapter types.FSAdapter  // From Sylos-FS; must be provided by caller
    Root    types.Folder    // Source or destination root folder
}
```

**Important**: The `Adapter` field must be provided by the caller. The Migration Engine never creates or closes adapters - the caller owns the adapter lifecycle. This design eliminates connection lock issues (especially with Spectra which only supports one connection at a time) and allows the API to manage adapter lifecycle centrally.

### LetsMigrate

The main entry point for executing a migration (synchronous):

```go
result, err := migration.LetsMigrate(cfg)
```

This function:
1. Opens the database (if `Config.DatabaseInstance` is nil, opens at `Config.Database.Path` via `db.Open`)
2. Loads YAML config if present and inspects DB state via `InspectMigrationStatus(database)`
3. Decides whether to run fresh or resume
4. Seeds roots (if needed), runs traversal (and copy when applicable) using the same DB
5. Runs verification against the DB
6. Returns results

**Note:** This function blocks until the migration completes or is shutdown. It automatically handles SIGINT/SIGTERM signals for graceful shutdown.

**Adapter Lifecycle**: The engine validates that adapters are provided (non-nil) but never creates or closes them. The caller is responsible for adapter lifecycle management.

### StartMigration

For programmatic control over a running migration:

```go
controller := migration.StartMigration(cfg)

// Later, trigger shutdown programmatically:
controller.Shutdown()

// Wait for completion:
result, err := controller.Wait()
```

**MigrationController** provides:
- `Shutdown()` – Triggers force shutdown, checkpoints the database, saves YAML with "suspended" status
- `GetDB()` – Returns the `*db.DB` in use (caller can keep using it when the engine does not close it)
- `Wait()` – Blocks until migration completes, returns result and error
- `Result()` – Returns the migration result (nil if not complete)
- `Error()` – Returns any error that occurred

**Use Cases:**
- Programmatic shutdown control
- Integration with external orchestration systems
- Testing scenarios requiring controlled shutdown

---

## YAML Configuration System

The migration package includes a comprehensive YAML-based configuration system that allows you to serialize and deserialize migration sessions.

### Automatic State Persistence

The config YAML is automatically saved at critical milestones:
- **Root selection** – When `SetRootFolders()` is called
- **Roots seeded** – After root tasks are seeded into the database
- **Traversal started** – When queues are initialized and ready
- **Round advancement** – When source or destination rounds advance
- **Traversal complete** – When migration finishes

### Config file location

By default, the config YAML path is derived from the database path: `{Database.Path}.yaml` (e.g. `migration.duckdb` → `migration.duckdb.yaml`). Override with `DatabaseConfig.ConfigPath`.

### Serialization (Save)

Configs are automatically saved during migration, but you can also save manually:

```go
// Create YAML config from migration.Config
yamlCfg, err := migration.NewMigrationConfigYAML(cfg, status)
if err != nil {
    return err
}

// Save to file
err = migration.SaveMigrationConfig("migration.yaml", yamlCfg)
```

### Deserialization (Load)

#### Option 1: Load YAML Config Only

For inspection or reading state without resuming:

```go
yamlCfg, err := migration.LoadMigrationConfig("migration.yaml")
if err != nil {
    return err
}

// Access config data
fmt.Printf("Status: %s\n", yamlCfg.State.Status)
fmt.Printf("Last Round Src: %d\n", *yamlCfg.State.LastRoundSrc)
```

#### Option 2: Reconstruct Full Config (For Resuming)

To resume a migration, you need to reconstruct a `migration.Config` from the YAML. Adapters must be provided directly - the engine never creates adapters:

```go
// Create adapters (caller is responsible for adapter lifecycle)
srcAdapter, err := createSourceAdapter(...)
if err != nil {
    return err
}

dstAdapter, err := createDestinationAdapter(...)
if err != nil {
    return err
}

// Load and reconstruct the config with provided adapters
cfg, err := migration.LoadMigrationConfigFromYAML("migration.yaml", srcAdapter, dstAdapter)
if err != nil {
    return err
}

// Resume the migration
result, err := migration.LetsMigrate(cfg)
```

**Important**: The engine never creates or closes adapters. The caller is responsible for adapter lifecycle management.

### YAML Config Structure

The YAML config includes:

- **Metadata** - Migration ID, creation time, last modified time
- **State** - Current status, last rounds/levels reached
- **Services** - Source and destination service configurations
- **Service Configs** - Embedded service-specific configs (e.g., spectra.json)
- **Migration Options** - Worker count, retries, coordinator lead, etc.
- **Logging** - Log service address, port, and level
- **Database** – Database file path and settings (path, config path, etc.)
- **Verification** - Verification options
- **Extensions** - Unstructured fields for future extensions

### Key Functions

- `LoadMigrationConfig(path)` - Loads YAML config file
- `SaveMigrationConfig(path, cfg)` - Saves YAML config file
- `LoadMigrationConfigFromYAML(path, factory)` - Loads YAML and reconstructs `migration.Config`
- `ToMigrationConfig(factory)` - Converts `MigrationConfigYAML` to `migration.Config`
- `NewMigrationConfigYAML(cfg, status)` - Creates YAML config from `migration.Config`

---

## Migration Lifecycle

### 1. Fresh Migration

```go
cfg := migration.Config{
    Database: migration.DatabaseConfig{
        Path: "migration.db",
    },
    Source:      sourceService,
    Destination: destinationService,
    SeedRoots:   true,
    // ... other options
}

result, err := migration.LetsMigrate(cfg)
```

### 2. Resume migration

When the database file already exists and/or YAML config exists, `LetsMigrate` inspects state and resumes if there is pending work:

```go
// Same config, but database already exists with state
cfg := migration.Config{
    Database: migration.DatabaseConfig{
        Path: "migration.db", // Existing database
    },
    // ... same config
}

// Automatically resumes from last checkpoint
result, err := migration.LetsMigrate(cfg)
```

### 3. Resume from YAML

Load a saved migration session and resume:

```go
// Load config from YAML
cfg, err := migration.LoadMigrationConfigFromYAML("migration.yaml", adapterFactory)
if err != nil {
    return err
}

// Resume migration
result, err := migration.LetsMigrate(cfg)
```

---

## State Management

### MigrationStatus

Tracks the current state of a migration:

```go
type MigrationStatus struct {
    SrcTotal           int
    DstTotal           int
    SrcPending         int
    DstPending         int
    SrcFailed          int
    DstFailed          int
    MinPendingDepthSrc *int
    MinPendingDepthDst *int
}
```

### InspectMigrationStatus

Query the current migration state from the database (node counts and stats tables in `pkg/db`):

```go
status, err := migration.InspectMigrationStatus(database)
if status.HasPending() {
    fmt.Println("Migration has pending work")
}
if status.IsComplete() {
    fmt.Println("Migration is complete")
}
```

Implementation uses `db.CountNodes`, `database.GetStatsCount`, and `database.GetStatsBreakdown` for SRC and DST.

---

## Verification

After migration completes, verification checks the results:

```go
verifyOpts := migration.VerifyOptions{
    AllowPending:  false,
    AllowNotOnSrc: true,
}

report, err := migration.VerifyMigration(database, verifyOpts)
if report.Success(verifyOpts) {
    fmt.Println("Migration verified successfully")
}
```

Verification reads from the same database: node counts and stats tables to report totals, pending, failed, successful, and (for DST) not-on-src.

---

## Database management

### Relationship with pkg/db

The migration package never opens or closes the database by itself unless you use it in “standalone” mode (no `DatabaseInstance` provided). It expects a DuckDB instance from `pkg/db`: either you pass `Config.DatabaseInstance` (already opened) or you set `Config.Database.Path` and the engine calls `db.Open(db.Options{Path: cfg.Database.Path})`. All traversal, copy, status, and verification use that single `*db.DB` (same node tables, staging, stats, and logs as described in `pkg/db`).

### SetupDatabase

Opens the database at the given path (creates if missing). Caller is responsible for closing it when done.

```go
database, wasFresh, err := migration.SetupDatabase(migration.DatabaseConfig{
    Path:           "migration.duckdb",
    RemoveExisting: false,
})
```

### DatabaseConfig

```go
type DatabaseConfig struct {
    Path           string // DuckDB file path (e.g. migration.duckdb)
    RemoveExisting bool   // If true, delete existing file before creating
    ConfigPath     string // Optional: custom YAML config path; default is {Path}.yaml
    RequireOpen    bool   // If true (API mode), DB instance must already be provided
}
```

---

## Error Handling

The migration package uses structured error handling:

- Database errors are wrapped with context
- Adapter creation errors include service type information
- State inspection errors indicate what operation failed
- Verification errors provide detailed failure reports

Always check errors and handle them appropriately:

```go
result, err := migration.LetsMigrate(cfg)
if err != nil {
    // Handle error - migration may be partially complete
    fmt.Printf("Migration failed: %v\n", err)
    
    // Check verification report for details
    if result.Verification.SrcFailed > 0 {
        fmt.Printf("Source failures: %d\n", result.Verification.SrcFailed)
    }
}
```

---

## Best Practices

1. **Always specify ConfigPath** - Makes it easier to locate and manage config files
2. **Check migration status** - Before resuming, inspect status to understand current state
3. **Handle errors gracefully** - Migrations can be partially complete
4. **Use verification** - Always verify migration results before considering it complete
5. **Save configs explicitly** - For important migrations, save configs at key points
6. **Provide adapters explicitly** - The engine never creates adapters; all adapters must be provided by the caller
7. **Manage adapter lifecycle** - The engine never closes adapters; the caller is responsible for cleanup

---

## Retry sweeps

Retry sweeps allow re-processing of failed or pending nodes to discover new or changed content. This is useful for:
- Recovering from transient failures
- Re-scanning subtrees marked as pending for investigation
- Testing migration behavior with partial tree reprocessing

### How Retry Sweeps Work

1. **Mark Nodes for Retry**: Change node status from `successful` to `pending` (or leave as `failed`)
2. **Delete subtree data**: Remove all descendant nodes and their metadata from the database (via `pkg/db` Writer)
3. **Run Retry Sweep**: Execute migration in retry mode (`QueueModeRetry`)
4. **Re-discover Content**: Queues re-traverse marked subtrees as if doing fresh traversal

### Retry Sweep Flow

**SRC Queue**:
- Scans all known levels for pending/failed tasks
- Re-processes marked nodes (lists children, applies filters)
- On successful completion of SRC folder tasks:
  - Triggers DST cleanup for corresponding DST nodes
  - Marks DST parent as pending
  - Deletes DST children

**DST Queue**:
- Waits for SRC to complete (coordinator gating)
- Processes pending DST tasks
- Loads expected children from SRC via join-lookup tables
- Performs comparison and discovers new/changed content

### DST Cleanup During SRC Completion

To prevent node duplication and maintain consistency, DST cleanup happens **during SRC task completion**, not during pull:

```go
// In completeTask() for SRC folder tasks in retry mode (database is *db.DB):
if q.name == "src" && q.getMode() == QueueModeRetry && task.IsFolder() {
    dstID, err := db.GetDstIDFromSrcID(database, srcNodeID)
    outputBuffer.AddStatusUpdate("DST", dstDepth, oldStatus, db.StatusPending, dstID)
    childIDs, err := db.GetChildrenIDsByParentID(database, "DST", dstID)
    for _, childID := range childIDs {
        outputBuffer.AddNodeDeletion("DST", childID, childDepth, childStatus)
    }
}
```

This ensures:
- DST cleanup is buffered along with other task completion writes
- Cleanup only occurs when SRC task successfully completes
- All operations are atomic within the output buffer
- No race conditions between cleanup and other queue operations

### Testing Retry Sweeps

The `pkg/tests/retry_sweep` package provides a comprehensive test:

1. **Setup**: Runs normal migration to completion (all nodes successful)
2. **Prepare Retry**: 
   - Randomly selects a top-level folder
   - Marks it as pending
   - Deletes its entire subtree (children, status, join tables, stats)
3. **Execute Retry**: Runs retry sweep migration
4. **Verify**: 
   - Confirms node counts match expected (subtree size)
   - Validates no duplicates were created
   - Checks both SRC and DST queues completed successfully

**Run the test**:
```powershell
powershell -File pkg/tests/retry_sweep/run.ps1
```

---

## Examples

See the main package (`main.go`) and test packages for complete examples of:
- Setting up migrations
- Creating service adapters
- Handling migration results
- Resuming from saved state
