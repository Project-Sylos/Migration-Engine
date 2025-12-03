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
/STATS   → Bucket count statistics (O(1) lookups)
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

### Statistics Bucket (`/STATS`)

**Path**: `/STATS/`

Stores count statistics for all buckets to enable O(1) count lookups without scanning:

```
/STATS
  "SRC/nodes"                    → int64 (8-byte big-endian)
  "SRC/children"                 → int64
  "SRC/levels/00000001/pending"  → int64
  "DST/levels/00000002/successful" → int64
  "LOGS"                         → int64
```

Statistics are automatically maintained during writes via `OutputBuffer` and can be manually synchronized using `SyncCounts()`.

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

**Note:** The database automatically initializes the bucket structure on first open, including the stats bucket for O(1) count operations.

### Path Hashing

Paths are hashed using SHA256 (first 16 bytes, 32 hex characters) for consistent key generation:

```go
pathHash := db.HashPath("/parent/child") // Returns 32-character hex string
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

// Count nodes in a status bucket (uses stats bucket for O(1) lookup)
count, err := database.CountStatusBucket("SRC", 1, db.StatusPending)

// Count total nodes (uses stats bucket)
totalNodes, err := database.CountNodes("SRC")

// Check if bucket has items (O(1) using stats)
hasItems, err := database.HasStatusBucketItems("SRC", 1, db.StatusPending)

// Get all levels that exist
levels, err := database.GetAllLevels("SRC")

// Find minimum level with pending work
minLevel, err := database.FindMinPendingLevel("SRC")

// Lease tasks from status bucket (for worker task distribution)
pathHashes, err := database.LeaseTasksFromStatus("SRC", 1, db.StatusPending, 1000)

// Batch fetch with keys (for task leasing with deduplication)
results, err := db.BatchFetchWithKeys(database, "SRC", 1, db.StatusPending, 100)
for _, result := range results {
    // result.Key is the path hash string
    // result.State is the NodeState
}
```

### Batch Operations

```go
// Batch insert multiple nodes (automatically updates stats)
ops := []db.InsertOperation{
    {QueueType: "SRC", Level: 1, Status: db.StatusPending, State: state1},
    {QueueType: "SRC", Level: 1, Status: db.StatusPending, State: state2},
}
err := db.BatchInsertNodes(database, ops)

// Batch update node statuses
results, err := db.BatchUpdateNodeStatus(database, "SRC", 1,
    db.StatusPending, db.StatusSuccessful, 
    []string{"/path1", "/path2"})

// Batch update copy status
copyResults, err := db.BatchUpdateNodeCopyStatus(database, "SRC", 1,
    db.StatusSuccessful, db.CopyStatusPending,
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

// Get log entry by ID and level
logEntry, err := db.GetLogEntry(database, "info", entry.ID)

// Get logs by level
infoLogs, err := db.GetLogsByLevel(database, "info")
errorLogs, err := db.GetLogsByLevel(database, "error")

// Get all logs across all levels
allLogs, err := db.GetAllLogs(database)
```

### Copy Status Operations

```go
// Update copy status (for future copy phase)
updatedState, err := db.UpdateNodeCopyStatus(database, "SRC", 1,
    db.StatusSuccessful, db.CopyStatusPending, "/path/to/node")

// Batch update copy status
results, err := db.BatchUpdateNodeCopyStatus(database, "SRC", 1,
    db.StatusSuccessful, db.CopyStatusPending,
    []string{"/path1", "/path2"})
```

### Buffered Writes

The package provides two buffering systems for high-throughput scenarios:

#### OutputBuffer

Batches write operations (status updates, inserts, copy status) with automatic coalescing and stats updates:

```go
// Create output buffer
outputBuffer := db.NewOutputBuffer(database, 500, 2*time.Second)
defer outputBuffer.Stop()

// Add status update (coalesces duplicates - last write wins)
outputBuffer.AddStatusUpdate("SRC", 1, db.StatusPending, db.StatusSuccessful, "/path/to/node")

// Add batch insert (merges with existing batch inserts)
ops := []db.InsertOperation{
    {QueueType: "SRC", Level: 1, Status: db.StatusPending, State: state1},
    {QueueType: "SRC", Level: 1, Status: db.StatusPending, State: state2},
}
outputBuffer.AddBatchInsert(ops)

// Add copy status update
outputBuffer.AddCopyStatusUpdate("SRC", 1, db.StatusSuccessful, "/path/to/node", db.CopyStatusPending)

// Force flush (or wait for automatic flush on batch size or interval)
outputBuffer.Flush()

// Pause/resume for controlled flushing
outputBuffer.Pause()
// ... do work ...
outputBuffer.Resume()
```

**Features:**
- Automatic coalescing of duplicate operations (last write wins)
- Batch merging for insert operations
- Automatic stats updates
- Time-based and size-based flush triggers
- Thread-safe

#### LogBuffer

Batches log entries for efficient persistence:

```go
// Create log buffer
logBuffer := db.NewLogBuffer(database, 500, 2*time.Second)
defer logBuffer.Stop()

// Add log entry (automatically flushed when batch size reached)
entry := db.LogEntry{
    ID:        db.GenerateLogID(),
    Timestamp: time.Now().Format(time.RFC3339Nano),
    Level:     "info",
    Entity:    "worker",
    EntityID:  "worker-1",
    Message:   "Task completed",
    Queue:     "src",
}
logBuffer.Add(entry)

// Force flush
logBuffer.Flush()
```

### Direct Write Operations

For immediate writes within transactions, use direct write functions:

```go
// Update node status within existing transaction
err := database.Update(func(tx *bolt.Tx) error {
    return db.UpdateNodeStatusInTx(tx, "SRC", 1, 
        db.StatusPending, db.StatusSuccessful, "/path/to/node")
})

// Batch insert nodes within existing transaction
ops := []db.InsertOperation{
    {QueueType: "SRC", Level: 1, Status: db.StatusPending, State: state1},
    {QueueType: "SRC", Level: 1, Status: db.StatusPending, State: state2},
}
err := database.Update(func(tx *bolt.Tx) error {
    return db.BatchInsertNodesInTx(tx, ops)
})
```

**Note:** Direct writes do not automatically update stats. Use `OutputBuffer` for automatic stats maintenance, or manually update stats after direct writes.

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

### Statistics Bucket (O(1) Counts)

The stats bucket enables O(1) count lookups without scanning buckets:

```go
// Fast: Uses stats bucket (O(1))
count, err := database.CountStatusBucket("SRC", 1, db.StatusPending)

// Slow: Falls back to cursor scan if stats unavailable (O(n))
count, err := database.CountStatusBucket("SRC", 1, db.StatusPending)
```

**Stats Maintenance:**
- Automatically updated by `OutputBuffer` during writes
- Can be manually synchronized: `database.SyncCounts()`
- Useful for recovery or correcting drift

### Buffered Writes

Use `OutputBuffer` for high-throughput scenarios:

```go
// Good: Batched writes with coalescing
outputBuffer := db.NewOutputBuffer(database, 500, 2*time.Second)
outputBuffer.AddStatusUpdate(...)
outputBuffer.AddBatchInsert(...)
// Automatically flushes on batch size or interval

// Bad: Many individual transactions
for _, op := range operations {
    db.UpdateNodeStatus(...) // Each is a separate transaction
}
```

**Benefits:**
- Reduces transaction overhead
- Automatic operation coalescing (duplicate elimination)
- Automatic stats updates
- Configurable flush triggers (size and time)

### Direct Writes

For immediate consistency, use direct write functions within transactions:

```go
// Direct write within transaction (immediate consistency)
err := database.Update(func(tx *bolt.Tx) error {
    return db.UpdateNodeStatusInTx(tx, "SRC", 1, 
        db.StatusPending, db.StatusSuccessful, "/path")
})
```

**Use Cases:**
- When immediate visibility is required
- Within existing transactions
- For critical operations that can't be buffered

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

## Statistics Management

The stats bucket provides O(1) count lookups for all buckets:

```go
// Get count for any bucket path
count, err := database.GetBucketCount([]string{"SRC", "nodes"})
count, err := database.GetBucketCount([]string{"SRC", "levels", "00000001", "pending"})

// Check if bucket has items
hasItems, err := database.HasBucketItems([]string{"SRC", "nodes"})

// Manually synchronize all stats (useful for recovery)
err := database.SyncCounts()

// Ensure stats bucket exists
err := database.EnsureStatsBucket()
```

**Stats are automatically maintained by:**
- `OutputBuffer` - Updates stats during buffered writes
- `BatchInsertNodes` - Updates stats during batch inserts

**Manual sync is useful for:**
- Recovery after crashes
- Correcting drift
- Initial stats population

---

## Summary

The database package provides:
- ✅ **BoltDB Integration** - Simple, reliable embedded database
- ✅ **Bucket Hierarchies** - Natural structure, not key encoding
- ✅ **Status Transitions** - Atomic, race-free status changes
- ✅ **High-Level Operations** - Convenient functions for common operations
- ✅ **Batch Support** - Efficient bulk operations with automatic stats
- ✅ **Buffered Writes** - `OutputBuffer` and `LogBuffer` for high throughput
- ✅ **O(1) Statistics** - Stats bucket for fast count lookups
- ✅ **Path Hashing** - Consistent key generation from paths
- ✅ **Task Leasing** - Efficient task distribution for workers
- ✅ **Thread Safety** - Safe for concurrent reads, serialized writes
- ✅ **Crash Resilience** - ACID transactions with automatic recovery

This design provides a clean, predictable storage layer for the migration engine's BFS traversal with guaranteed consistency, atomic operations, and high performance through intelligent buffering and statistics.
