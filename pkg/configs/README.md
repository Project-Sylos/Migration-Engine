# Configs Package

The **configs** package provides utilities for loading and parsing JSON configuration files used throughout the Migration Engine. It centralizes configuration management for various services and components.

---

## Overview

The configs package defines structured types and loader functions for:

- **Buffer Configuration** – Batch size and flush interval for buffered appender and staging-table writes (see `pkg/db`)
- **Log Service Configuration** – UDP logging address, port, and minimum log level
- **Spectra Configuration** – Spectra filesystem simulator settings (root IDs, instance configuration)

Configuration files are expected under a config directory (e.g. `pkg/configs/`); loaders accept a `configDir` path for flexibility.

---

## Configuration Types

### BufferConfig

Maps table names to batch size and flush interval for buffered writes (e.g. staging and node tables in `pkg/db`):

```go
type BufferTableConfig struct {
    BatchSize        int // Number of items to batch before flushing
    FlushIntervalSec int // Seconds between automatic flushes
}

type BufferConfig map[string]BufferTableConfig
```

**Usage:**
```go
cfg, err := configs.LoadBufferConfig("pkg/configs")
if err != nil {
    return err
}

tableCfg := cfg["logs"] // e.g. config for "logs" table
batchSize := tableCfg.BatchSize
```

**Expected file:** `buffers.json` (optional; not present in repository by default)

---

### LogServiceConfig

Configures the UDP log service for real-time logging:

```go
type LogServiceConfig struct {
    Address string // UDP address (e.g., "127.0.0.1")
    Port    int    // UDP port (e.g., 8081)
    Level   string // Minimum log level ("trace", "debug", "info", "warning", "error", "critical")
}
```

**Usage:**
```go
cfg, err := configs.LoadLogServiceConfig("pkg/configs")
if err != nil {
    return err
}

logAddress := fmt.Sprintf("%s:%d", cfg.Address, cfg.Port)
```

**Configuration file:** `log_service.json`

**Example `log_service.json`:**
```json
{
    "address": "127.0.0.1",
    "port": 8081,
    "level": "info"
}
```

---

### SpectraConfig

Configures the Spectra filesystem simulator:

```go
type SpectraConfig struct {
    ConfigPath     string // Path to Spectra SDK configuration
    SrcRootID      string // Source root node ID
    DstRootID      string // Destination root node ID
    SharedInstance bool   // Whether source and destination share the same Spectra instance
    Description    string // Optional description
}
```

**Usage:**
```go
cfg, err := configs.LoadSpectraConfig("pkg/configs")
if err != nil {
    return err
}

srcAdapter := fsservices.NewSpectraFS(spectraInstance, cfg.SrcRootID, "primary")
```

**Configuration file:** `spectra.json`

The loader expects the Spectra section under a `"spectra"` key:

```json
{
    "spectra": {
        "config_path": "./spectra.json",
        "src_root_id": "root-node-id",
        "dst_root_id": "root-node-id",
        "shared_instance": true,
        "description": "Migration test configuration"
    }
}
```

The `spectra.json` in the repository may also contain seed and API configuration used by the Spectra simulator itself; the Migration Engine only reads the `spectra` block via `LoadSpectraConfig`.

---

## Loader Functions

All loaders follow the same pattern:

| Function | File | Description |
|----------|------|-------------|
| `LoadBufferConfig(configDir string) (BufferConfig, error)` | `buffers.json` | Buffer batch and flush settings per table |
| `LoadLogServiceConfig(configDir string) (LogServiceConfig, error)` | `log_service.json` | UDP log service |
| `LoadSpectraConfig(configDir string) (SpectraConfig, error)` | `spectra.json` | Spectra FS (expects `"spectra"` wrapper) |

---

## Error Handling

Loaders return descriptive errors:

- **File read errors** – Wrapped with which config file failed
- **JSON parse errors** – Wrapped with parse context
- **Missing files** – Reported as file read errors

Example:

```go
cfg, err := configs.LoadLogServiceConfig("pkg/configs")
if err != nil {
    return fmt.Errorf("configuration error: %w", err)
}
```

---

## File Layout

By convention, config files live under `pkg/configs/`:

```
pkg/configs/
├── config.go           # Loader functions and type definitions
├── log_service.json    # Log service UDP configuration
└── spectra.json        # Spectra simulator (and optional "spectra" block for engine)
```

`buffers.json` is optional; when absent, `LoadBufferConfig` will fail unless the caller uses a different config dir or provides the file.

---

## Integration with Migration Engine

This package is used by:

1. **Spectra / migration setup** – `LoadSpectraConfig` for Spectra adapters and root IDs (e.g. in `pkg/migration` config YAML loading)
2. **Log service** – `LoadLogServiceConfig` for UDP log address and level
3. **Buffered writes** – `LoadBufferConfig` (when `buffers.json` is present) to configure batch size and flush interval for staging and node buffers in `pkg/db`

---

## Summary

- **Centralized config** – Single place for JSON config loaders
- **Type-safe** – Structured types for each config
- **Clear errors** – Context on read/parse failures
- **Configurable paths** – `configDir` for tests and deployment
- **Consistent API** – Same loader pattern for all configs
