# Log Service Package

The **Log Service** package provides asynchronous, dual-channel logging for the migration engine. It combines **real-time UDP logging** for live monitoring with **BoltDB persistence** for audit trails and analysis.

---

## Overview

The log service provides two independent channels:

1. **UDP Channel**: Real-time network logging for monitoring tools (level-filtered)
2. **Database Channel**: Persistent storage in BoltDB via a buffered writer (all logs, always enabled)

**Key benefits**:
- **Non-blocking**: Buffered writes prevent log operations from slowing down migration
- **Complete audit trail**: All logs are persisted to BoltDB for later analysis
- **Real-time monitoring**: UDP stream enables live tracking via external tools
- **Level filtering**: UDP channel respects minimum log level; database captures everything

---

## Architecture

```
Application Code
    ↓ (log call)
LogService (LS)
    ├─→ UDP Socket ──→ Network (filtered by level)
    └─→ LogBuffer ──→ BoltDB
```

**Components**:
- **`LogService`**: Singleton global logger; routes messages to both channels
- **`LogBuffer`**: Batches log entries and flushes to BoltDB periodically
- **UDP Sender**: Async network writer (non-blocking)

---

## Initialization

### Global Logger (Recommended)

```go
import (
    "github.com/Project-Sylos/Migration-Engine/pkg/logservice"
)

// Initialize global logger
if err := logservice.InitGlobalLogger(boltDB, "127.0.0.1:8081", "info"); err != nil {
    return err
}

// Use global logger
logservice.LS.Log("info", "Migration started", "migration", "main", "src")
```

**Parameters**:
- `boltDB`: BoltDB instance (for persistence)
- `address`: UDP address for network logging (format: "host:port")
- `minLevel`: Minimum level for UDP logging ("trace", "debug", "info", "warning", "error", "critical")

**Behavior**:
- All logs are persisted to BoltDB regardless of level
- Only logs >= `minLevel` are sent via UDP
- If UDP fails, logging continues (non-blocking)
- Sends a test log and clears console on initialization

### Manual Sender Creation

```go
// Create sender manually (if not using global logger)
sender, err := logservice.NewSender(boltDB, "127.0.0.1:8081", "info")
if err != nil {
    return err
}
defer sender.Close()

// Use sender
sender.Log("info", "Message", "entity", "id", "queue")
```

---

## Usage

### Basic Logging

```go
import "github.com/Project-Sylos/Migration-Engine/pkg/logservice"

// Log a message
err := logservice.LS.Log("info", "Migration started", "migration", "main", "")
if err != nil {
    // Handle logging error (rare)
}
```

### With Context

```go
// Log with entity/queue context
logservice.LS.Log(
    "debug",                    // level
    "Processing task",          // message
    "worker",                   // entity (e.g., "worker", "queue", "coordinator")
    "worker-1",                 // entity ID
    "src",                      // queue ("src", "dst", or "")
)
```

### Log Levels

Supported levels (lowest to highest severity):
- `trace` - Verbose debugging
- `debug` - Detailed diagnostic information
- `info` - General informational messages
- `warning` - Warning messages
- `error` - Error messages
- `critical` - Critical failures

---

## Buffering & Persistence

### Write Buffer

Logs are batched and flushed periodically for performance:

```go
// LogBuffer configuration (internal)
logBuffer := db.NewLogBuffer(boltDB, 500, 2*time.Second)
```

**Flush triggers**:
- **Batch size**: Flushes when 500 entries accumulated
- **Time interval**: Flushes every 2 seconds regardless of count
- **Explicit flush**: On service shutdown

---

## BoltDB Persistence

### Storage Structure

All logs are written to BoltDB via a buffered writer (`db.LogBuffer`):

```
/LOGS
  /trace     → {uuid}: LogEntry JSON
  /debug     → {uuid}: LogEntry JSON
  /info      → {uuid}: LogEntry JSON
  /warning   → {uuid}: LogEntry JSON
  /error     → {uuid}: LogEntry JSON
  /critical  → {uuid}: LogEntry JSON
```

**Log Entry Structure**:
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2024-01-15T10:30:45.123456Z",
  "level": "info",
  "entity": "worker",
  "entity_id": "worker-1",
  "message": "Task completed successfully",
  "queue": "src"
}
```

---

## Querying Logs

Query logs from BoltDB for analysis:

### Get All Logs

```go
logs, err := db.GetAllLogs(boltDB)
for _, entry := range logs {
    fmt.Printf("[%s] %s: %s\n", entry.Timestamp, entry.Level, entry.Message)
}
```

### Get Logs by Level

```go
// Get only error logs
errorLogs, err := db.GetLogsByLevel(boltDB, "error")

// Get only info logs
infoLogs, err := db.GetLogsByLevel(boltDB, "info")
```

---

## UDP Network Logging

### Protocol

UDP packets are sent in JSON format using the `LogPacket` structure:

```go
type LogPacket struct {
    Timestamp time.Time `json:"timestamp"`
    Level     string    `json:"level"`
    Message   string    `json:"message"`
    Entity    string    `json:"entity,omitempty"`
    EntityID  string    `json:"entity_id,omitempty"`
    Queue     string    `json:"queue,omitempty"`
}
```

**Example JSON:**
```json
{
  "timestamp": "2024-01-15T10:30:45.123456Z",
  "level": "info",
  "entity": "worker",
  "entity_id": "worker-1",
  "message": "Task completed",
  "queue": "src"
}
```

### Level Filtering

Only logs >= `minLevel` are sent via UDP:

```go
// Example: minLevel = "info"
logservice.LS.Log("debug", "Verbose message", ...)  // ❌ Not sent via UDP
logservice.LS.Log("info", "Standard message", ...)  // ✅ Sent via UDP
logservice.LS.Log("error", "Error message", ...)    // ✅ Sent via UDP
```

**Note**: All logs are still persisted to BoltDB regardless of UDP filtering.

### Starting a Listener

The logservice package provides utilities to start UDP listeners:

#### Automatic Terminal Spawn

Spawns a new terminal window with a listener (cross-platform):

```go
// Spawns a new terminal window running the listener
err := logservice.StartListener("127.0.0.1:8081")
if err != nil {
    return err
}
```

**Platform Support:**
- **Windows**: Opens new `cmd.exe` window
- **macOS**: Opens new Terminal.app window
- **Linux**: Tries `x-terminal-emulator`, `gnome-terminal`, or `xterm`

#### Manual Listener

Run the listener programmatically:

```go
// Run listener in current process
err := logservice.RunListener("127.0.0.1:8081")
if err != nil {
    return err
}
```

**Listener Features:**
- Displays logs in format: `[timestamp] [level] message [queue]`
- Automatically clears console when `<<CLEAR_SCREEN>>` message is received
- Handles malformed packets gracefully

#### Standalone Listener Program

Run the listener as a standalone program:

```bash
# From pkg/logservice/main directory
go run spawn.go 127.0.0.1:8081
```

Or use the spawned terminal window (automatically opened by `StartListener`).

---

## Console Control

### Clear Console

Send a special message to clear the listener's console:

```go
// Clear console (UDP only, not persisted to DB)
err := logservice.LS.ClearConsole()
if err != nil {
    return err
}
```

This sends a `<<CLEAR_SCREEN>>` message that the listener recognizes and uses to clear its display.

---

## Shutdown & Cleanup

```go
// Graceful shutdown (flushes buffers)
if logservice.LS != nil {
    logservice.LS.Close()
}
```

**Shutdown behavior**:
- All buffered logs are flushed to BoltDB (with 2-second timeout)
- UDP connection is closed gracefully
- No logs are lost
- Log buffer stops its flush loop

---

## Error Handling

### Logging Errors

```go
err := logservice.LS.Log("info", "Message", "entity", "id", "queue")
if err != nil {
    // Log operation failed (rare)
    // BoltDB write error or UDP error
}
```

### Non-Blocking Design

- UDP failures don't block application
- BoltDB write failures are logged but don't stop execution
- Logging is best-effort for performance

---

## Best Practices

1. **Use appropriate log levels**
   - `debug`: Development/troubleshooting only
   - `info`: Normal operational messages
   - `warning`: Issues that don't stop execution
   - `error`: Failures requiring attention

2. **Provide context**
   - Always include entity, entity ID, and queue when applicable
   - Makes log analysis easier

3. **Avoid excessive logging**
   - Don't log in tight loops
   - Use `debug` level for verbose output

4. **Query logs strategically**
   - Filter by level for specific analysis
   - Use timestamp-based filtering when needed

---

## Architecture Details

### Log Buffer Configuration

The log buffer is configured with:
- **Batch Size**: 500 entries (flushes when reached)
- **Flush Interval**: 2 seconds (periodic flush)
- **Timeout**: 2 seconds (for graceful shutdown)

These values are hardcoded in `NewSender()` but can be adjusted if needed.

### Port Reuse

The listener does not enforce port exclusivity. Multiple listeners can bind to the same UDP address, which allows:
- Multiple monitoring tools to receive logs simultaneously
- Redundant listeners for reliability
- Development scenarios with multiple test instances

**Note**: For production, ensure only one listener is running per address to avoid confusion.

---

## Summary

The log service provides:
- ✅ **Dual-Channel Logging**: UDP for real-time + BoltDB for persistence
- ✅ **Non-Blocking**: Buffered writes don't slow down migration
- ✅ **Level Filtering**: UDP respects minimum level
- ✅ **Complete Audit Trail**: All logs persisted regardless of level
- ✅ **Queryability**: BoltDB logs organized by level for analysis
- ✅ **Cross-Platform Listener**: Automatic terminal spawning on Windows, macOS, and Linux
- ✅ **Console Control**: Clear console functionality for clean displays
- ✅ **Global Logger**: Convenient singleton pattern for application-wide logging

This design ensures comprehensive logging without impacting migration performance.
