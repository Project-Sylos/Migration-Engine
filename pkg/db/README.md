# Database Package

## Overview

The database package provides a unified interface for storing and retrieving migration data using **BadgerDB**, a fast, embedded key-value store. All node metadata, traversal state, and logs are stored directly in BadgerDB using a structured key format.

---

## Why BadgerDB?

BadgerDB is an embedded key-value store written in Go, designed for high performance and reliability:

1. **High Performance** – BadgerDB is optimized for fast writes and reads, making it ideal for the queue system's high-throughput operations.
2. **Embedded** – No external dependencies or services required; runs entirely within the application process.
3. **ACID Transactions** – Provides transactional guarantees for data consistency.
4. **Efficient Iteration** – Supports prefix-based iteration for efficient querying of related data.
5. **Crash Resilience** – Data is persisted to disk, enabling recovery after crashes.
6. **Simple API** – Clean, straightforward API that fits well with Go's concurrency model.

---

## Key Structure

BadgerDB uses structured keys to organize data:

### Node Keys

**Format**: `{queueType}:{level:08d}:{traversal_status}:{copy_status}:{path_hash}`

- **`queueType`**: `"src"` or `"dst"`
- **`level`**: BFS depth level (zero-padded to 8 digits)
- **`traversal_status`**: `"pending"`, `"successful"`, `"failed"`, or `"not_on_src"` (dst only)
- **`copy_status`**: `"pending"`, `"successful"`, or `"failed"` (src only, empty for dst)
- **`path_hash`**: SHA256 hash of the normalized path (first 16 bytes, hex-encoded)

**Examples**:
- `src:00000000:pending:pending:abc123...` - Source root node, pending traversal and copy
- `dst:00000001:successful::def456...` - Destination node at level 1, successfully traversed
- `src:00000002:successful:successful:ghi789...` - Source node at level 2, traversal and copy complete

### Log Keys

**Format**: `log:{level}:{uuid}`

- **`level`**: Log level (`"trace"`, `"debug"`, `"info"`, `"warning"`, `"error"`, `"critical"`)
- **`uuid`**: Unique identifier for the log entry (UUID v4)

**Examples**:
- `log:info:550e8400-e29b-41d4-a716-446655440000`
- `log:error:6ba7b810-9dad-11d1-80b4-00c04fd430c8`

### Index Keys

**Format**: `{queueType}:children:{parent_path_hash}:{child_path_hash}`

Used for efficient parent→child relationship lookups. The value is empty (the key encodes the relationship).

---

## Core Operations

### Opening a Database

```go
import "github.com/Project-Sylos/Migration-Engine/pkg/db"

opts := db.DefaultOptions()
opts.Dir = "/path/to/badger/directory"

database, err := db.Open(opts)
if err != nil {
    return err
}
defer database.Close()
```

### Reading Data

```go
// Get a single value
value, err := database.Get([]byte("key"))

// Iterate keys with a prefix
err := database.IterateKeys(db.IteratorOptions{
    Prefix: []byte("src:00000000:"),
}, func(key []byte) error {
    // Process key
    return nil
})

// Iterate key-value pairs
err := database.IterateKeyValues(db.IteratorOptions{
    Prefix: []byte("log:info:"),
}, func(key []byte, value []byte) error {
    // Process key-value pair
    return nil
})
```

### Writing Data

```go
// Single write
err := database.Set([]byte("key"), []byte("value"))

// Transactional write
err := database.Update(func(txn *badger.Txn) error {
    if err := txn.Set([]byte("key1"), []byte("value1")); err != nil {
        return err
    }
    if err := txn.Set([]byte("key2"), []byte("value2")); err != nil {
        return err
    }
    return nil
})
```

### Node State Operations

The package provides high-level operations for working with node states:

```go
// Insert a node with its index entry
key := db.KeySrc(0, db.TraversalStatusPending, db.CopyStatusPending, "/root")
state := &db.NodeState{
    ID:         "node-id",
    ParentID:   "",
    ParentPath: "",
    Name:       "root",
    Path:       "/root",
    Type:       "folder",
    Depth:      0,
}
err := db.InsertNodeWithIndex(database, key, state, db.IndexPrefixSrcChildren)

// Update node status
newState, err := db.UpdateNodeStatus(database, "src", 0, 
    db.TraversalStatusPending, db.TraversalStatusSuccessful, 
    db.CopyStatusPending, "/root")

// Fetch children by parent path
children, err := db.FetchChildrenByParentPath(database, db.IndexPrefixSrcChildren,
    "/root", []string{"src"}, 0)
```

---

## Log Storage

Logs are stored with keys in the format `log:{level}:{uuid}`. The `LogBuffer` automatically batches log writes for performance:

```go
import "github.com/Project-Sylos/Migration-Engine/pkg/db"

// Create log buffer
logBuffer := db.NewLogBuffer(database, 500, 2*time.Second)

// Add log entry
entry := db.LogEntry{
    ID:        db.GenerateLogID(), // UUID
    Timestamp: time.Now().Format(time.RFC3339Nano),
    Level:     "info",
    Entity:    "worker",
    EntityID:  "worker-1",
    Message:   "Task completed",
    Queue:     "src",
}
logBuffer.Add(entry)

// Stop buffer (flushes remaining entries)
logBuffer.Stop()
```

---

## Key Generation Helpers

The package provides helper functions for generating keys:

```go
// Node keys
key := db.KeySrc(level, traversalStatus, copyStatus, path)
key := db.KeyDst(level, traversalStatus, copyStatus, path)

// Prefixes for iteration
prefix := db.PrefixForStatus("src", 0, db.TraversalStatusPending)
prefix := db.PrefixForLevel("src", 0)
prefix := db.PrefixForAllLevels("src")

// Log keys
logKey := db.KeyLog("info", logID)
logPrefix := db.PrefixLogByLevel("info")
allLogsPrefix := db.PrefixLogAll()
```

---

## Constants

### Traversal Status

- `db.TraversalStatusPending` - Node is pending traversal
- `db.TraversalStatusSuccessful` - Node was successfully traversed
- `db.TraversalStatusFailed` - Node traversal failed
- `db.TraversalStatusNotOnSrc` - Node exists in destination but not in source (dst only)

### Copy Status

- `db.CopyStatusPending` - Copy operation is pending
- `db.CopyStatusSuccessful` - Copy operation completed successfully
- `db.CopyStatusFailed` - Copy operation failed

### Index Prefixes

- `db.IndexPrefixSrcChildren` - Index prefix for source node children
- `db.IndexPrefixDstChildren` - Index prefix for destination node children

---

## Performance Considerations

### Batch Operations

For high-throughput scenarios, use batch operations:

```go
// Batch insert nodes
ops := []db.InsertOperation{
    {QueueType: "src", Level: 0, TraversalStatus: db.TraversalStatusPending, 
     CopyStatus: db.CopyStatusPending, State: state1, IndexPrefix: db.IndexPrefixSrcChildren},
    {QueueType: "src", Level: 0, TraversalStatus: db.TraversalStatusPending,
     CopyStatus: db.CopyStatusPending, State: state2, IndexPrefix: db.IndexPrefixSrcChildren},
}
err := db.BatchInsertNodes(database, ops)

// Batch update node status
results, err := db.BatchUpdateNodeStatus(database, "src", 0,
    db.TraversalStatusPending, db.TraversalStatusSuccessful,
    db.CopyStatusPending, []string{"/path1", "/path2"})
```

### Iteration Performance

When iterating large datasets, use appropriate prefixes to limit scope:

```go
// Efficient: Iterate only pending nodes at level 0
prefix := db.PrefixForStatus("src", 0, db.TraversalStatusPending)
err := database.IterateNodeStates(db.IteratorOptions{Prefix: prefix}, ...)

// Less efficient: Iterate all nodes then filter
prefix := db.PrefixForAllLevels("src")
err := database.IterateNodeStates(db.IteratorOptions{Prefix: prefix}, ...)
```

---

## Thread Safety

BadgerDB operations are safe for concurrent use:

- **Read operations** (`View`, `Get`, `Iterate*`) can run concurrently
- **Write operations** (`Update`, `Set`, `Delete`) are serialized by BadgerDB
- **Transactions** provide isolation between concurrent operations

---

## Error Handling

All database operations return errors that should be checked:

```go
value, err := database.Get(key)
if err == badger.ErrKeyNotFound {
    // Key doesn't exist
} else if err != nil {
    // Other error
    return err
}
```

---

## Summary

The database package provides:
- ✅ **BadgerDB Integration** - Fast, embedded key-value store
- ✅ **Structured Keys** - Organized key format for nodes and logs
- ✅ **High-Level Operations** - Convenient functions for common operations
- ✅ **Batch Support** - Efficient batch operations for high throughput
- ✅ **Thread Safety** - Safe for concurrent use
- ✅ **Crash Resilience** - Data persisted to disk

This design enables the migration engine to efficiently store and query traversal state, node metadata, and logs with excellent performance characteristics.
