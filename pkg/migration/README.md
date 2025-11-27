# Migration Package

The **migration** package provides the core orchestration logic for the Migration Engine, including configuration management, state tracking, and the main migration execution flow.

---

## Overview

The migration package coordinates the entire migration lifecycle:
- **Configuration Management** - YAML-based config files for serializing/deserializing migration state
- **State Tracking** - Monitors migration progress, rounds, and completion status
- **Execution Orchestration** - Coordinates queue setup, traversal, and verification
- **Resume Support** - Ability to pause and resume migrations from saved state

---

## Core Components

### Config

The `Config` struct aggregates all parameters required to run a migration:

```go
type Config struct {
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
    // ... additional fields
}
```

### Service

Represents a filesystem service participating in the migration:

```go
type Service struct {
    Name    string
    Adapter fsservices.FSAdapter
    Root    fsservices.Folder
}
```

### LetsMigrate

The main entry point for executing a migration:

```go
result, err := migration.LetsMigrate(cfg)
```

This function:
1. Sets up or opens the BoltDB database
2. Inspects existing migration state
3. Decides whether to run fresh or resume
4. Executes traversal
5. Runs verification
6. Returns results

---

## YAML Configuration System

The migration package includes a comprehensive YAML-based configuration system that allows you to serialize and deserialize migration sessions.

### Automatic State Persistence

The config YAML is automatically saved at critical milestones:
- **Root Selection** - When `SetRootFolders()` is called
- **Roots Seeded** - After root tasks are seeded into BoltDB
- **Traversal Started** - When queues are initialized and ready
- **Round Advancement** - When source or destination rounds advance
- **Traversal Complete** - When migration finishes

### Config File Location

By default, the config YAML is stored alongside the database file:
- Database: `migration.db`
- Config: `migration.db.yaml`

You can specify a custom path via `DatabaseConfig.ConfigPath`.

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

To resume a migration, you need to reconstruct a `migration.Config` from the YAML. This requires an `AdapterFactory` to create service adapters:

```go
// Define your adapter factory
factory := func(serviceType string, serviceCfg migration.ServiceConfigYAML, serviceConfigs map[string]any) (fsservices.FSAdapter, error) {
    switch strings.ToLower(serviceType) {
    case "spectra":
        // Extract spectra config and create adapter
        spectraFS, err := sdk.New(configPath)
        if err != nil {
            return nil, err
        }
        return fsservices.NewSpectraFS(spectraFS, rootID, world)
    default:
        return nil, fmt.Errorf("unsupported service type: %s", serviceType)
    }
}

// Load and reconstruct the config
cfg, err := migration.LoadMigrationConfigFromYAML("migration.yaml", factory)
if err != nil {
    return err
}

// Resume the migration
result, err := migration.LetsMigrate(cfg)
```

### YAML Config Structure

The YAML config includes:

- **Metadata** - Migration ID, creation time, last modified time
- **State** - Current status, last rounds/levels reached
- **Services** - Source and destination service configurations
- **Service Configs** - Embedded service-specific configs (e.g., spectra.json)
- **Migration Options** - Worker count, retries, coordinator lead, etc.
- **Logging** - Log service address, port, and level
- **Database** - BoltDB file path and settings
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

### 2. Resume Migration

When you open an existing BoltDB database, `LetsMigrate` automatically detects pending work and resumes:

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

Query the current migration state from BoltDB:

```go
status, err := migration.InspectMigrationStatus(boltDB)
if status.HasPending() {
    fmt.Println("Migration has pending work")
}
if status.IsComplete() {
    fmt.Println("Migration is complete")
}
```

The status inspection queries BoltDB bucket counts:
- Iterates all levels for each queue
- Counts nodes in each status bucket (pending, successful, failed)
- Finds minimum pending level

---

## Verification

After migration completes, verification checks the results:

```go
verifyOpts := migration.VerifyOptions{
    AllowPending:  false,
    AllowFailed:   false,
    AllowNotOnSrc: true,
}

report, err := migration.VerifyMigration(boltDB, verifyOpts)
if report.Success(verifyOpts) {
    fmt.Println("Migration verified successfully")
}
```

Verification queries BoltDB to count:
- Total nodes in each queue
- Nodes in each status across all levels
- Nodes successfully moved (excluding root level)

---

## Database Management

### SetupDatabase

Creates a new BoltDB database:

```go
db, wasFresh, err := migration.SetupDatabase(migration.DatabaseConfig{
    Path:           "migration.db",
    RemoveExisting: false,
})
```

### DatabaseConfig

Configures BoltDB behavior:

```go
type DatabaseConfig struct {
    Path           string // BoltDB file path
    RemoveExisting bool   // Delete existing file before creating
    ConfigPath     string // Optional: custom YAML config path
}
```

**Note:** BoltDB uses a single file for the entire database, making cleanup straightforward.

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
6. **Provide proper AdapterFactory** - When loading from YAML, ensure your factory handles all service types

---

## BoltDB Benefits for Migration

1. **Atomic Resumption** - Bucket structure naturally represents current state
2. **Race-Free Operations** - Single-writer guarantees consistent status transitions
3. **Predictable Queries** - Counting and iteration are deterministic
4. **Simple Cleanup** - Just delete the single database file
5. **Crash Resilience** - ACID transactions guarantee consistency

---

## Examples

See the main package (`main.go`) and test packages for complete examples of:
- Setting up migrations
- Creating service adapters
- Handling migration results
- Resuming from saved state
