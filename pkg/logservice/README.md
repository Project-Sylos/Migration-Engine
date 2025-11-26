# Log Service Package

The **Log Service** package provides asynchronous, dual-channel logging for the migration engine. It combines **real-time UDP logging** for live monitoring with **BadgerDB persistence** for audit trails and analysis.

---

## Overview

The log service uses a two-channel approach:

1. **UDP Channel**: Real-time log streaming to a listener terminal (optional, filtered by log level)
2. **Database Channel**: Persistent storage in BadgerDB via a buffered writer (all logs, always enabled)

This design provides:
- **Low-latency monitoring**: UDP logs appear instantly in a separate terminal
- **Complete audit trail**: All logs are persisted to BadgerDB for later analysis
- **Performance**: Buffered database writes minimize I/O overhead
- **Flexibility**: UDP threshold can be adjusted to control real-time verbosity

---

## Architecture

### Dual-Channel Design

```
Application Code
    ↓
LogService.Sender
    ├─→ UDP Socket (filtered by level) ──→ Listener Terminal
    └─→ LogBuffer ──→ BadgerDB
```

### Components

- **`Sender`**: Transmits logs over UDP and queues them for database writes
- **`LogBuffer`**: Batches log entries and flushes to BadgerDB periodically
- **`RunListener`**: UDP listener that displays logs in real-time
- **`StartListener`**: Spawns a new terminal window running the listener

---

## Usage

### Initialization

Initialize the global logger during application startup:

```go
import "github.com/Project-Sylos/Migration-Engine/pkg/logservice"

// Start listener in a separate terminal (optional)
if err := logservice.StartListener("127.0.0.1:8081"); err != nil {
    log.Printf("Warning: failed to start listener: %v", err)
}
time.Sleep(2 * time.Second) // Give listener time to start

// Initialize global logger
if err := logservice.InitGlobalLogger(badgerDB, "127.0.0.1:8081", "info"); err != nil {
    return fmt.Errorf("failed to initialize logger: %w", err)
}
```

**Parameters:**
- `badgerDB`: BadgerDB instance (for persistence)
- `"127.0.0.1:8081"`: UDP address for real-time logging
- `"info"`: Minimum log level for UDP output (trace/debug/info/warning/error/critical)

### Logging

Use the global `logservice.LS` instance to log messages:

```go
import "github.com/Project-Sylos/Migration-Engine/pkg/logservice"

// Basic log
err := logservice.LS.Log("info", "Worker started", "worker", "worker-1", "src")

// Error log
if err != nil {
    logservice.LS.Log("error", fmt.Sprintf("Task failed: %v", err), "worker", "worker-1", "src")
}
```

**Method Signature:**

```go
func (s *Sender) Log(level, message, entity, entityID string, queues ...string) error
```

**Parameters:**
- `level`: Log level (`"trace"`, `"debug"`, `"info"`, `"warning"`, `"error"`, `"critical"`)
- `message`: Log message text
- `entity`: Entity type (e.g., `"worker"`, `"queue"`, `"fsservices"`)
- `entityID`: Entity identifier (e.g., `"worker-1"`, `"src"`)
- `queues...`: Optional queue name (e.g., `"src"`, `"dst"`)

### Log Levels

Log levels are ordered by priority:

1. **`trace`** (0): Very detailed debugging information
2. **`debug`** (1): Debugging information for development
3. **`info`** (2): General informational messages
4. **`warning`** (3): Warning messages for non-critical issues
5. **`error`** (4): Error messages for failures
6. **`critical`** (5): Critical errors requiring immediate attention

**UDP Filtering:**
- Only logs at or above the threshold level are sent over UDP
- All logs are always written to BadgerDB regardless of level

**Example:**
- Threshold: `"info"` → UDP receives: `info`, `warning`, `error`, `critical`
- Threshold: `"debug"` → UDP receives: `debug`, `info`, `warning`, `error`, `critical`
- Threshold: `"trace"` → UDP receives: all logs

---

## UDP Listener

### Automatic Spawning

`StartListener()` automatically spawns a new terminal window running the listener:

**Windows:**
- Opens a new `cmd.exe` window

**macOS:**
- Opens a new Terminal.app window via AppleScript

**Linux:**
- Tries `x-terminal-emulator`, `gnome-terminal`, or `xterm` (whichever is available)

**Example:**

```go
if err := logservice.StartListener("127.0.0.1:8081"); err != nil {
    fmt.Printf("Warning: failed to start listener: %v\n", err)
}
```

### Manual Listener

Run the listener manually:

```bash
go run pkg/logservice/main/spawn.go 127.0.0.1:8081
```

Or use `RunListener()` programmatically:

```go
if err := logservice.RunListener("127.0.0.1:8081"); err != nil {
    log.Fatal(err)
}
```

**Listener Output:**

```
=== Log Listener ===
2025-01-03 15:30:22 [info] Worker started [src]
2025-01-03 15:30:22 [debug] Pulled 5 tasks for round 0 [src]
2025-01-03 15:30:23 [info] Advanced to round 1 [src]
...
```

---

## BadgerDB Persistence

### Log Buffer

All logs are written to BadgerDB via a buffered writer (`db.LogBuffer`):

**Buffer Configuration:**
- **Batch Size**: 500 entries (configurable)
- **Flush Interval**: 2 seconds (configurable)
- **Auto-Flush**: Flushes when batch size reached OR interval elapsed

**Benefits:**
- Reduces database write frequency
- Improves performance during high-volume logging
- Ensures no logs are lost (flushes on shutdown)

### Key Format

Logs are stored in BadgerDB with keys in the format: `log:{level}:{uuid}`

**Examples:**
- `log:info:550e8400-e29b-41d4-a716-446655440000`
- `log:error:6ba7b810-9dad-11d1-80b4-00c04fd430c8`

Each log entry is stored as JSON with the following structure:

```json
{
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "timestamp": "2025-01-03T15:30:22.123456789Z",
    "level": "info",
    "entity": "worker",
    "entity_id": "worker-1",
    "details": null,
    "message": "Worker started",
    "queue": "src"
}
```

### Querying Logs

Query logs from BadgerDB using prefix iteration:

```go
import "github.com/Project-Sylos/Migration-Engine/pkg/db"

// Get all error logs
prefix := db.PrefixLogByLevel("error")
err := badgerDB.IterateKeyValues(db.IteratorOptions{
    Prefix: prefix,
}, func(key []byte, value []byte) error {
    entry, err := db.DeserializeLogEntry(value)
    if err != nil {
        return err
    }
    fmt.Printf("Error: %s\n", entry.Message)
    return nil
})

// Get all logs
prefix = db.PrefixLogAll()
err = badgerDB.IterateKeyValues(db.IteratorOptions{
    Prefix: prefix,
}, func(key []byte, value []byte) error {
    entry, err := db.DeserializeLogEntry(value)
    if err != nil {
        return err
    }
    fmt.Printf("[%s] %s: %s\n", entry.Level, entry.Entity, entry.Message)
    return nil
})
```

---

## Integration Points

### Queue System

Queues log their lifecycle events:

```go
logservice.LS.Log("info", "Queue initialized", "queue", "src", "src")
logservice.LS.Log("info", fmt.Sprintf("Advanced to round %d", round), "queue", "src", "src")
logservice.LS.Log("info", "Queue exhausted", "queue", "src", "src")
```

### Workers

Workers log task execution:

```go
logservice.LS.Log("info", "Worker started", "worker", w.id, w.queueName)
logservice.LS.Log("debug", fmt.Sprintf("Successfully traversed %s", path), "worker", w.id, w.queueName)
logservice.LS.Log("error", fmt.Sprintf("Failed to traverse %s: %v", path, err), "worker", w.id, w.queueName)
```

### Filesystem Services

Adapters log operations:

```go
logservice.LS.Log("info", fmt.Sprintf("Created folder %s", path), "fsservices", "local")
logservice.LS.Log("error", fmt.Sprintf("Failed to list children: %v", err), "fsservices", "spectra")
```

---

## Thread Safety

The `Sender` is **safe for concurrent use**:

- **UDP Sends**: Protected by `sync.Mutex` (one UDP packet at a time)
- **Database Writes**: `LogBuffer` uses `sync.Mutex` for thread-safe batching
- **Multiple Goroutines**: All workers and queues can log simultaneously

**Example:**

```go
// Safe to call from multiple goroutines
go func() {
    logservice.LS.Log("info", "Worker 1 started", "worker", "worker-1", "src")
}()

go func() {
    logservice.LS.Log("info", "Worker 2 started", "worker", "worker-2", "src")
}()
```

---

## Performance Considerations

### UDP Overhead

- **Minimal Impact**: UDP sends are non-blocking and fast
- **Level Filtering**: Reduces network overhead by filtering low-priority logs
- **JSON Encoding**: Lightweight JSON encoding per message

### Database Overhead

- **Buffered Writes**: Reduces I/O operations significantly
- **Batch Inserts**: Multiple logs inserted in single transaction
- **Async Flushing**: Database writes don't block application code
- **Graceful Shutdown**: Final flush on `Close()` ensures no logs are lost

**Typical Performance:**
- UDP send: < 1ms per message
- Database write (buffered): Amortized over batch size (e.g., 500 entries per 2 seconds)

---

## Error Handling

### UDP Failures

- **Silent Failure**: UDP send errors are returned but don't crash the application
- **UDP is Optional**: Application continues even if UDP listener is unavailable
- **Database is Primary**: All logs are persisted regardless of UDP status

### Database Failures

- **Graceful Degradation**: Database write errors are logged but don't crash the application
- **Buffer Continues**: Log buffer continues accepting entries even if a flush fails
- **No Data Loss**: Buffer attempts to flush on shutdown, but failures are silent

**Example Error Handling:**

```go
if err := logservice.LS.Log("error", "Something failed", "component", "id"); err != nil {
    // Log to stderr as fallback
    fmt.Fprintf(os.Stderr, "Log service error: %v\n", err)
}
```

---

## Cleanup

Always close the log service on application shutdown:

```go
defer func() {
    if logservice.LS != nil {
        logservice.LS.Close() // Flushes remaining logs and closes UDP connection
    }
}()
```

This ensures:
- All buffered logs are flushed to BadgerDB
- UDP connection is closed cleanly
- Background goroutines are stopped

---

## Future Extensions

Potential enhancements:

- **Log Rotation**: Automatic rotation of database logs based on size/age
- **Remote Logging**: UDP listener can be on a remote machine for distributed monitoring
- **Log Aggregation**: Multiple senders sending to a central listener
- **Structured Logging**: Enhanced JSON structure with nested fields
- **Metrics**: Extract metrics from logs (error rates, throughput, etc.)

---

## Summary

The log service provides:
- ✅ **Dual-Channel Logging**: UDP for real-time + BadgerDB for persistence
- ✅ **Performance**: Buffered database writes minimize I/O overhead
- ✅ **Flexibility**: Configurable log level thresholds for UDP
- ✅ **Thread Safety**: Safe for concurrent use from multiple goroutines
- ✅ **Integration**: Seamlessly integrated throughout the migration engine
- ✅ **Queryability**: BadgerDB logs enable powerful analysis and debugging
- ✅ **Zero Configuration**: Automatic terminal spawning on startup

This logging system enables both real-time monitoring during execution and comprehensive post-mortem analysis of migration runs.
