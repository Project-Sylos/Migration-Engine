# Log Service Package

The **Log Service** package provides dual-channel logging for the Migration Engine: **real-time UDP** for live monitoring and optional **persistence to the main database’s `logs` table** via a buffered writer (`db.LogBuffer`).

---

## Overview

Two channels:

1. **UDP** – Level-filtered network logging for monitoring tools.
2. **Database** – When a main DB handle is provided, logs are batched in `db.LogBuffer` and flushed to that DB’s **`logs`** table (see `pkg/db`). The sender does not open or close the DB; it only writes through the buffer.

**Benefits**:
- Non-blocking: buffered DB writes and best-effort UDP.
- Level filtering on UDP; DB receives all levels.
- Optional persistence: pass `nil` for the DB to use UDP only.

---

## Architecture

```
Application
    ↓ Log(…)
Sender
    ├─→ UDP socket (level ≥ threshold)
    └─→ db.LogBuffer (if mainDB != nil) → main DB logs table
```

**Components**:
- **Sender** (`sender.go`): Holds a `*db.DB` reference and a `*db.LogBuffer`. Routes each log to UDP (when level ≥ threshold) and to `logBuffer.Add(…)` when the buffer is present. Does not own the DB; `Close()` stops the buffer and closes the UDP connection only.
- **db.LogBuffer** (`pkg/db`): Batches `db.LogEntry` and flushes to the main DB’s `logs` table. Batch size and interval are set in `NewSender` (20,000 and 10s).
- **Listener** (`listener.go`): UDP receive loop; parses `LogPacket` JSON, prints `timestamp [level] message [queue]`, and clears the console when the message is `<<CLEAR_SCREEN>>`.

---

## Initialization

### Global logger

```go
import "codeberg.org/Sylos/Migration-Engine/pkg/logservice"

// mainDB is *db.DB from db.Open(); may be nil for UDP-only
if err := logservice.InitGlobalLogger(mainDB, "127.0.0.1:8081", "info"); err != nil {
    return err
}

logservice.LS.Log("info", "Migration started", "migration", "main", "src")
```

**Parameters**:
- **mainDB** – `*db.DB`. If non-nil, logs are written to its `logs` table via `db.LogBuffer`. If nil, no DB writes (UDP only).
- **addr** – UDP address, e.g. `"host:port"`.
- **minLevel** – Minimum level for UDP: `"trace"`, `"debug"`, `"info"`, `"warning"`, `"error"`, `"critical"`.

**Behavior**:
- UDP: only logs with level ≥ `minLevel` are sent.
- DB: when `mainDB != nil`, every log is added to the buffer; the buffer flushes on size or interval. `Close()` stops the buffer and flushes; it does not close the DB.

### Manual sender

```go
sender, err := logservice.NewSender(mainDB, "127.0.0.1:8081", "info")
if err != nil {
    return err
}
defer sender.Close()

sender.Log("info", "Message", "entity", "id", "queue")
```

---

## Usage

### Basic logging

```go
logservice.LS.Log("info", "Migration started", "migration", "main", "")
```

### With context

```go
logservice.LS.Log("debug", "Processing task", "worker", "worker-1", "src")
// Signature: Log(level, message, entity, entityID, queues...)
// Only the first queue string is used.
```

### Log levels (lowest → highest)

`trace`, `debug`, `info`, `warning`, `error`, `critical`.

---

## Buffering and persistence

- **Batch size**: 20,000 entries (`defaultBatchSize` in `sender.go`).
- **Flush interval**: 10 seconds (`db.NewLogBuffer(…, 10*time.Second)`).
- **Shutdown**: `Close()` calls `logBuffer.Stop()` (stops flush loop and flushes) and closes the UDP connection.

Persistence is to the main database’s `logs` table; schema and `Writer.InsertLog` are in `pkg/db`. Query logs with SQL against that table.

---

## UDP and listener

### Protocol

UDP packets are JSON. Structure in `listener.go`:

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

Only logs with level ≥ `minLevel` are sent over UDP; the DB buffer receives every log when `mainDB` is set.

### Spawn listener in a new terminal

```go
err := logservice.StartListener("127.0.0.1:8081")
```

Uses a platform-specific terminal: Windows `cmd`, macOS Terminal.app, Linux `x-terminal-emulator` / `gnome-terminal` / `xterm`. Runs `go run …/main/spawn.go <addr>` in that terminal.

### Run listener in process

```go
err := logservice.RunListener("127.0.0.1:8081")
```

Blocks in the receive loop. Prints each log as `timestamp [level] message [queue]`. On message `<<CLEAR_SCREEN>>`, runs a console clear command (e.g. `clear` / `cls`).

### Standalone

```bash
go run ./pkg/logservice/main/spawn.go 127.0.0.1:8081
```

`spawn.go` defaults to `127.0.0.1:8081` if no argument is given.

---

## Console control

```go
logservice.LS.ClearConsole()
```

Sends a single UDP packet with message `<<CLEAR_SCREEN>>` (and level `"info"`). Not written to the DB. The listener clears its display when it receives this.

---

## Shutdown

```go
if logservice.LS != nil {
    logservice.LS.Close()
}
```

Stops the log buffer (flush loop and final flush) and closes the UDP connection. Does not close the main DB.

---

## File layout

```
pkg/logservice/
├── sender.go    # LS, InitGlobalLogger, NewSender, Sender, Log, ClearConsole, Close
├── listener.go  # LogPacket, StartListener, RunListener, getSpawnAbsPath, clearConsole
├── README.md
└── main/
    └── spawn.go  # main: default addr 127.0.0.1:8081, runs RunListener(addr)
```

---

## Summary

- **Dual channel**: UDP (level-filtered) and optional DB persistence via `db.LogBuffer` to the main DB’s `logs` table.
- **Sender** holds a reference to `*db.DB` and creates a `db.LogBuffer` when the DB is non-nil; it does not own or close the DB.
- **Buffer**: 20k batch size, 10s interval; `Close()` stops and flushes.
- **Listener**: `StartListener` spawns a terminal running the listener; `RunListener` runs the loop in-process; `<<CLEAR_SCREEN>>` clears the console.
- **ClearConsole**: UDP-only; no DB write.
