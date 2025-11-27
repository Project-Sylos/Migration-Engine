# Database Package

## Overview

The database package provides a unified interface for storing and retrieving migration data using **BoltDB**, a fast, embedded key-value store. All node metadata, traversal state, and logs are stored using a structured bucket hierarchy.

---

## Why BoltDB?

BoltDB is an embedded key-value store written in Go, designed for simplicity and reliability:

1. **Single-Writer Architecture** – One writer guarantees consistency; no race conditions
2. **Bucket Hierarchies** – Natural support for organizing data by structure
3. **Atomic Transactions** – Single B-tree provides ACID guarantees with simple transaction semantics
4. **Predictable Performance** – No background compaction; deterministic read/write behavior
5. **Embedded** – No external dependencies or services required; runs entirely within the application
6. **Crash Resilience** – Data is persisted to disk with automatic recovery
7. **Simple API** – Clean, straightforward API that fits well with Go's concurrency model

---

## Bucket Structure

BoltDB uses nested buckets to organize data hierarchically:

### Top-Level Buckets

```
/SRC     → Source queue data
/DST     → Destination queue data
/LOGS    → Log entries organized by level
```

### Node Storage (`/SRC` and `/DST`)

Each queue type has three main sub-buckets:

#### 1. Nodes Bucket (`/nodes`)

**Path**: `/SRC/nodes/` or `/DST/nodes/`

Stores the canonical node data. Key is the path hash, value is NodeState JSON.

```
pathHash → NodeState JSON
{
  "id": "node-uuid",
  "parent_id": "parent-uuid",
  "parent_path": "/parent",
  "name": "folder-name",
  "path": "/parent/folder-name",
  "type": "folder",
  "depth": 1,
  "traversal_status": "successful",
  ...
}
```

#### 2. Children Bucket (`/children`)

**Path**: `/SRC/children/` or `/DST/children/`

Stores parent-child relationships. Key is parent path hash, value is array of child path hashes.

```
parentPathHash → []childPathHash JSON
["abc123", "def456", "ghi789"]
```

#### 3. Levels Bucket (`/levels`)

**Path**: `/SRC/levels/` or `/DST/levels/`

Organized by BFS depth level, with status sub-buckets:

```
/levels
  /00000000              → Level 0 (root)
    /pending             → pathHash: empty
    /successful          → pathHash: empty
    /failed              → pathHash: empty
  /00000001              → Level 1
    /pending
    /successful
    /failed
  /00000002              → Level 2
    ...
```

For DST queue, there's an additional status:
```
  /not_on_src            → Items in DST but not in SRC
```

**Status buckets are membership sets**: The presence of a pathHash in a bucket means that node has that status. The value is empty; only the key matters.

### Log Storage (`/LOGS`)

Logs are organized by level in sub-buckets:

```
/LOGS
  /trace     → uuid: LogEntry JSON
  /debug     → uuid: LogEntry JSON
  /info      → uuid: LogEntry JSON
  /warning   → uuid: LogEntry JSON
  /error     → uuid: LogEntry JSON
  /critical  → uuid: LogEntry JSON
```

Each log entry is keyed by its UUID within the appropriate level bucket.

---

## Core Operations

### Opening a Database

```go
import "github.com/Project-Sylos/Migration-Engine/pkg/db"

opts := db.DefaultOptions()
opts.Path = "/path/to/migration.db"

database, err := db.Open(opts)
if err != nil {
    return err
}
defer database.Close()
```

### Node State Operations

```go
// Insert a node (creates entry in nodes, status, and children buckets)
state := &db.NodeState{
    ID:         "node-id",
    ParentID:   "parent-id",
    ParentPath: "/parent",
    Name:       "child",
    Path:       "/parent/child",
    Type:       "folder",
    Depth:      1,
}
err := db.InsertNodeWithIndex(database, "SRC", 1, db.StatusPending, state)

// Update node status (moves between status buckets)
updatedState, err := db.UpdateNodeStatus(database, "SRC", 1, 
    db.StatusPending, db.StatusSuccessful, "/parent/child")

// Get node state
state, err := db.GetNodeStateByPath(database, "SRC", "/parent/child")

// Get children of a node
children, err := db.GetChildrenStates(database, "SRC", "/parent")
```

### Iteration

```go
// Iterate over all nodes in a status bucket
err := database.IterateStatusBucket("SRC", 1, db.StatusPending, 
    db.IteratorOptions{Limit: 100}, 
    func(pathHash []byte) error {
        // Process each pending node at level 1
        return nil
    })

// Count nodes in a status bucket
count, err := database.CountStatusBucket("SRC", 1, db.StatusPending)

// Get all levels that exist
levels, err := database.GetAllLevels("SRC")

// Find minimum level with pending work
minLevel, err := database.FindMinPendingLevel("SRC")
```

### Batch Operations

```go
// Batch insert multiple nodes
ops := []db.InsertOperation{
    {QueueType: "SRC", Level: 1, Status: db.StatusPending, State: state1},
    {QueueType: "SRC", Level: 1, Status: db.StatusPending, State: state2},
}
err := db.BatchInsertNodes(database, ops)

// Batch update node statuses
results, err := db.BatchUpdateNodeStatus(database, "SRC", 1,
    db.StatusPending, db.StatusSuccessful, 
    []string{"/path1", "/path2"})
```

### Log Operations

```go
// Insert a log entry (automatically goes to correct level bucket)
entry := db.LogEntry{
    ID:        db.GenerateLogID(),
    Timestamp: time.Now().Format(time.RFC3339Nano),
    Level:     "info",
    Entity:    "worker",
    EntityID:  "worker-1",
    Message:   "Task completed",
    Queue:     "src",
}
err := db.InsertLogEntry(database, entry)

// Get logs by level
infoLogs, err := db.GetLogsByLevel(database, "info")
errorLogs, err := db.GetLogsByLevel(database, "error")

// Get all logs
allLogs, err := db.GetAllLogs(database)
```

### Buffered Writes

For high-throughput scenarios, use write buffers:

```go
// Create write buffer (batches writes every 1000 items or 3 seconds)
writeBuffer := db.NewWriteBuffer(database, 1000, 3*time.Second)

// Add operations
writeBuffer.Add(db.WriteItem{
    Type:      "insert",
    QueueType: "SRC",
    Level:     1,
    Status:    db.StatusPending,
    State:     nodeState,
})

// Explicit flush (also auto-flushes on timer/size)
err := writeBuffer.Flush()

// Stop buffer (flushes remaining items)
writeBuffer.Stop()
```

---

## Bucket Helper Functions

```go
// Get bucket paths
nodesPath := db.GetNodesBucketPath("SRC")        // ["SRC", "nodes"]
childrenPath := db.GetChildrenBucketPath("SRC")  // ["SRC", "children"]
levelPath := db.GetLevelBucketPath("SRC", 1)     // ["SRC", "levels", "00000001"]
statusPath := db.GetStatusBucketPath("SRC", 1, db.StatusPending)
// ["SRC", "levels", "00000001", "pending"]

// Create level bucket with all status sub-buckets
err := db.EnsureLevelBucket(tx, "SRC", 1)

// Get bucket within transaction
nodesBucket := db.GetNodesBucket(tx, "SRC")
statusBucket := db.GetStatusBucket(tx, "SRC", 1, db.StatusPending)
```

---

## Constants

### Status Constants

```go
const (
    StatusPending    = "pending"
    StatusSuccessful = "successful"
    StatusFailed     = "failed"
    StatusNotOnSrc   = "not_on_src"  // DST only
)
```

### Bucket Names

```go
const (
    BucketSrc  = "SRC"
    BucketDst  = "DST"
    BucketLogs = "LOGS"
)
```

---

## Performance Considerations

### Status Transitions

BoltDB makes status transitions atomic and race-free:

```go
// Atomic within single transaction:
// 1. Update NodeState in /nodes bucket
// 2. Remove from old status bucket
// 3. Add to new status bucket
// All three operations succeed or all fail
```

### Batch Operations

Always use batch operations when inserting multiple nodes:

```go
// Good: Single transaction for 100 nodes
ops := make([]db.InsertOperation, 100)
err := db.BatchInsertNodes(database, ops)

// Bad: 100 separate transactions
for _, state := range states {
    db.InsertNodeWithIndex(database, "SRC", 1, db.StatusPending, state)
}
```

### Write Buffers

Use WriteBuffer for queue operations to batch database writes:

```go
// Batches ~1000 operations into single transaction
buffer := db.NewWriteBuffer(database, 1000, 3*time.Second)
```

---

## Thread Safety

BoltDB operations are safe for concurrent use:

- **Read transactions** (`View`) can run concurrently
- **Write transactions** (`Update`) are serialized by BoltDB
- Single-writer model eliminates MVCC race conditions
- Reads see consistent snapshot within transaction

---

## Error Handling

```go
state, err := database.GetNodeStateByPath("SRC", "/path")
if err != nil {
    return err  // Database error
}
if state == nil {
    // Node not found (not an error)
}
```

---

## Summary

The database package provides:
- ✅ **BoltDB Integration** - Simple, reliable embedded database
- ✅ **Bucket Hierarchies** - Natural structure, not key encoding
- ✅ **Status Transitions** - Atomic, race-free status changes
- ✅ **High-Level Operations** - Convenient functions for common operations
- ✅ **Batch Support** - Efficient bulk operations
- ✅ **Thread Safety** - Safe for concurrent reads, serialized writes
- ✅ **Crash Resilience** - ACID transactions with automatic recovery

This design provides a clean, predictable storage layer for the migration engine's BFS traversal with guaranteed consistency and atomic operations.
