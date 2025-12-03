# Configs Package

The **configs** package provides utilities for loading and parsing JSON configuration files used throughout the Migration Engine. It centralizes configuration management for various services and components.

---

## Overview

The configs package defines structured types and loader functions for:
- **Buffer Configuration** - Batch size and flush interval settings for buffered operations
- **Log Service Configuration** - UDP logging address, port, and minimum log level
- **Spectra Configuration** - Spectra filesystem simulator settings including root IDs and instance configuration

All configuration files are expected to be located in the `pkg/configs/` directory by default, though loaders accept a config directory path parameter for flexibility.

---

## Configuration Types

### BufferConfig

Manages buffer settings for different tables/operations:

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

// Access buffer config for a specific table
tableCfg := cfg["logs"] // Example: get config for "logs" table
batchSize := tableCfg.BatchSize
```

**Expected file:** `buffers.json` (not currently present in repository)

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

// Use config to initialize log service
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

// Use config to initialize Spectra adapters
srcAdapter := fsservices.NewSpectraFS(spectraInstance, cfg.SrcRootID, "primary")
```

**Configuration file:** `spectra.json`

**Expected structure:**
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

**Note:** The actual `spectra.json` file in the repository contains seed data and API configuration, which appears to be used by the Spectra simulator itself rather than the Migration Engine's config loader.

---

## Loader Functions

All loader functions follow a consistent pattern:

### LoadBufferConfig

```go
func LoadBufferConfig(configDir string) (BufferConfig, error)
```

Loads buffer configuration from `{configDir}/buffers.json`.

### LoadLogServiceConfig

```go
func LoadLogServiceConfig(configDir string) (LogServiceConfig, error)
```

Loads log service configuration from `{configDir}/log_service.json`.

### LoadSpectraConfig

```go
func LoadSpectraConfig(configDir string) (SpectraConfig, error)
```

Loads Spectra configuration from `{configDir}/spectra.json`. The JSON file should wrap the Spectra config in a `"spectra"` key.

---

## Error Handling

All loader functions return descriptive errors:

- **File read errors**: Wrapped with context about which config file failed
- **JSON parse errors**: Wrapped with context about parsing failure
- **Missing files**: Returned as file read errors

**Example error handling:**
```go
cfg, err := configs.LoadLogServiceConfig("pkg/configs")
if err != nil {
    // Error message will be: "failed to read log service config: ..."
    // or "failed to parse log service config: ..."
    return fmt.Errorf("configuration error: %w", err)
}
```

---

## File Locations

By convention, configuration files are stored in `pkg/configs/`:

```
pkg/configs/
├── config.go           # Loader functions and type definitions
├── log_service.json    # Log service UDP configuration
└── spectra.json        # Spectra simulator configuration
```

The `configDir` parameter allows loading configs from alternative locations for testing or deployment scenarios.

---

## Integration with Migration Engine

The configs package is used by:

1. **Log Service** - Loads `log_service.json` to configure UDP logging
2. **Filesystem Services** - Loads `spectra.json` to configure Spectra adapters
3. **Future Buffer Operations** - Will use `buffers.json` for buffered write configurations

---

## Summary

The configs package provides:
- ✅ **Centralized Configuration** - Single location for all JSON config loaders
- ✅ **Type Safety** - Structured Go types for all configurations
- ✅ **Error Handling** - Descriptive errors for debugging
- ✅ **Flexibility** - Configurable directory paths
- ✅ **Consistency** - Uniform loader function patterns

This package simplifies configuration management across the Migration Engine by providing a consistent interface for loading and parsing JSON configuration files.

