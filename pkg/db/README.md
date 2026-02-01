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

The database is partitioned into two main areas:

```
/Traversal-Data  → Root bucket for all traversal-related data
  /SRC           → Source queue data
  /DST           → Destination queue data
  /STATS         → Bucket count statistics and queue metrics (O(1) lookups)

/LOGS            → Log entries organized by level (separate island)
```

This partitioning separates traversal operations (discovery/scanning phase) from future copy operations, allowing the copy phase to have its own data structure under a separate root bucket.

### Level-Sharded Storage (`/SRC` and `/DST`)

All traversal data for a given BFS level lives under **level shards**: `Traversal-Data/{SRC|DST}/levels/<level>/`. There are no top-level global `nodes`, `children`, or join buckets. Each level shard contains:

#### 1. Nodes Bucket (`levels/<level>/nodes`)

**Path**: `/Traversal-Data/SRC/levels/00000000/nodes/` (or DST, or another level)

Stores the canonical node data for that level. Key is the ULID, value is NodeState JSON.

```
ULID → NodeState JSON
{
  "id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",  // ULID (used as key)
  "parent_id": "01ARZ3NDEKTSV4RRFFQ69G5FAW",  // Parent's ULID
  "parent_path": "/parent",
  "name": "folder-name",
  "path": "/parent/folder-name",
  "type": "folder",
  "depth": 1,
  "traversal_status": "successful",
  ...
}
```

(For DST nodes, corresponding SRC identity is resolved via the join-lookup table, not stored in NodeState.)

#### 2. Children Bucket (`levels/<level>/children`)

**Path**: `/Traversal-Data/SRC/levels/00000000/children/` (or DST, or another level)

Stores parent-child relationships for nodes at this level. Key is parent ULID, value is array of child ULIDs.

```
parentULID → []childULID JSON
["01ARZ3NDEKTSV4RRFFQ69G5FAV", "01ARZ3NDEKTSV4RRFFQ69G5FAW", "01ARZ3NDEKTSV4RRFFQ69G5FAX"]
```

#### 3. Join-Lookup Tables (`levels/<level>/src-to-dst` and `levels/<level>/dst-to-src`)

**Paths** (per level):
- `/Traversal-Data/SRC/levels/<level>/src-to-dst/`  (SRC only)
- `/Traversal-Data/DST/levels/<level>/dst-to-src/`  (DST only)

Bidirectional mappings between corresponding SRC and DST nodes at that level. These tables replace the legacy `SrcID` field that was previously embedded in DST NodeState.

```
SRC: srcULID → dstULID
DST: dstULID → srcULID
```

**Purpose:**
- Enable efficient correlation between SRC and DST nodes without embedding references in node data
- Support retry sweeps where DST nodes need to be reset when their corresponding SRC nodes are retried
- Populated during DST task completion when children are matched by Type + Name

**Usage:**
```go
// Find DST node corresponding to SRC node (level required)
dstID, err := db.GetDstIDFromSrcID(boltDB, level, srcULID)

// Find SRC node corresponding to DST node (level required)
srcID, err := db.GetSrcIDFromDstID(boltDB, level, dstULID)

// Set bidirectional mapping (each call uses its own transaction; or use OutputBuffer)
err := db.SetSrcToDstMapping(database, level, srcULID, dstULID)
err := db.SetDstToSrcMapping(database, level, dstULID, srcULID)
```

#### 4. Levels Bucket (`/levels`) – status and lookups per level

**Path**: `/Traversal-Data/SRC/levels/` or `/Traversal-Data/DST/levels/`

Each level shard also contains **traversal** and **copy** status sub-buckets (copy exists only for SRC). Full structure per level:

**SRC level shard (e.g. `levels/00000000`):**
```
/levels/00000000
  /nodes              → ULID: NodeState JSON
  /children           → parentULID: []childULID JSON
  /src-to-dst         → srcULID: dstULID
  /traversal
    /pending          → ULID: empty (membership set)
    /successful       → ULID: empty
    /failed           → ULID: empty
    /excluded         → ULID: empty
    /status-lookup    → ULID: status string (reverse index)
  /copy
    /folder           → /pending, /in-progress, /successful, /skipped, /failed
    /file             → (same)
    /status-lookup    → ULID: status string
  /00000001           → Level 1 (same shape)
  ...
```

**DST level shard (e.g. `levels/00000000`):**
```
/levels/00000000
  /nodes              → ULID: NodeState JSON
  /children           → parentULID: []childULID JSON
  /dst-to-src         → dstULID: srcULID
  /traversal
    /pending
    /successful
    /failed
    /not_on_src       → ULID: empty (DST-specific)
    /excluded
    /status-lookup
  /00000001
  ...
```

**Note**: DST nodes do **not** have copy status buckets. Copy status is only relevant for SRC nodes.

**Status buckets are membership sets**: The presence of a ULID in a bucket means that node has that status. The value is empty; only the key matters.

**Traversal Status** (SRC and DST):
- Answers: "Can I descend?", "Should I enqueue children?", "Did listing fail?", "Was this excluded?"
- Used by: traversal engine, retry/exclusion logic, UI tree expansion
- Statuses: `pending`, `successful`, `failed`, `excluded` (DST also has `not_on_src`)

**Copy Status** (SRC only):
- Answers: "Will data move?", "Is this already satisfied?", "Did copy fail?", "Was it skipped?"
- Used by: copy phase queueing, progress bars, audit & reporting, UI action-plan view
- Statuses: `pending`, `successful`, `skipped`, `failed`

**Status-lookup indexes**: Both `traversal/status-lookup` and `copy/status-lookup` buckets provide reverse indexes mapping `ULID → status string`. This enables O(1) lookup to determine which status bucket a node belongs to without scanning all status buckets. The indexes are automatically maintained:
- Created when a level bucket is first created
- Updated on every node insert (with initial status)
- Updated on every status transition (pending → successful, etc.)
- Removed when a node is deleted

### Log Storage (`/LOGS`)

Logs are **count-sharded** (e.g. 1M entries per shard). Each shard holds full log entries, level-index buckets, and a stats bucket for resume. The `_meta` bucket stores the current shard ID and count.

```
/LOGS
  /_meta              → current_shard (int64), current_count (int64) — for resume
  /000000             → shard 0 (6-digit zero-padded shard ID)
    /logs             → uuid: LogEntry JSON (full entries)
    /trace            → uuid: empty (membership by level)
    /debug
    /info
    /warning
    /error
    /critical
    /stats            → "count": int64 (entries in this shard)
  /000001             → shard 1
    ...
```

Each log entry is stored in the current shard's `logs` bucket (keyed by UUID) and referenced in the level bucket (trace, debug, info, etc.) for level-based queries.

### Statistics Bucket (`/STATS`)

**Path**: `/Traversal-Data/STATS/`

Stores count statistics and queue performance metrics:

```
/STATS
  (key: bucket path string) → int64 (8-byte big-endian)
    e.g. "SRC/levels/00000000/nodes" → int64
    e.g. "SRC/levels/00000001/traversal/pending" → int64
    e.g. "DST/levels/00000002/traversal/successful" → int64
  /queue-stats           → queueKey: QueueObserverMetrics JSON
    "src-traversal"      → QueueObserverMetrics JSON
    "dst-traversal"      → QueueObserverMetrics JSON
    "copy"               → QueueObserverMetrics JSON (future)
```

**Count keys**: Stored directly in the STATS bucket (no `totals` sub-bucket). Keys are bucket path strings (e.g. `SRC/levels/00000001/nodes`). Statistics are automatically maintained during writes via `OutputBuffer` and can be manually synchronized using `SyncCounts()`.

**Queue-stats sub-bucket**: Stores real-time queue performance metrics published by the `QueueObserver`:
- Queue statistics (pending, in-progress, workers, round, etc.)
- Average task execution time
- Tasks per second (calculated from last poll)
- Total completed tasks
- Last poll timestamp

Metrics are updated every 200ms (configurable) during active migrations, allowing external APIs to poll for real-time performance data.

---

## Core Operations

### Opening a Database

```go
import "codeberg.org/Sylos/Migration-Engine/pkg/db"

opts := db.DefaultOptions()
opts.Path = "/path/to/migration.db"

database, err := db.Open(opts)
if err != nil {
    return err
}
defer database.Close()
```

**Note:** The database automatically initializes the bucket structure on first open, including the stats bucket for O(1) count operations.

### Deterministic Node IDs

All node IDs are generated deterministically from logical identity:

```go
nodeID := db.DeterministicNodeID(queueType, nodeType, path)
// Returns: "node:<16-char-hex>" (FNV-1a 64-bit hash)
// Example: "node:a1b2c3d4e5f67890"
```

**Canonical format:** `<queueType>|<nodeType>|<normalized_path>`

**Benefits:**
- Eliminates duplicate logical nodes - same path always produces same ID
- Race-safe - multiple workers discovering the same node won't create duplicates
- Idempotent - re-running traversal produces identical IDs
- No external dependencies - uses Go stdlib `hash/fnv`

### Join-Lookup Table Operations

```go
// Get DST ULID from SRC ULID
dstID, err := db.GetDstIDFromSrcID(boltDB, srcULID)

// Get SRC ULID from DST ULID
srcID, err := db.GetSrcIDFromDstID(boltDB, dstULID)

// Set bidirectional mapping (within transaction)
err := database.Update(func(tx *bolt.Tx) error {
    err := db.SetSrcToDstMapping(tx, srcULID, dstULID)
    if err != nil {
        return err
    }
    return db.SetDstToSrcMapping(tx, dstULID, srcULID)
})

// Or use OutputBuffer for automatic batching
outputBuffer.AddLookupMapping(srcULID, dstULID)
```

**Important:** Join-lookup mappings are created during DST task completion when DST children are matched to their corresponding SRC children (by Type + Name). This replaces the legacy `SrcID` field that was previously stored in DST NodeState structs.

### Node State Operations

```go
// Insert a node (creates entry in nodes, status, status-lookup, and children buckets)
state := &db.NodeState{
    ID:         "01ARZ3NDEKTSV4RRFFQ69G5FAV",  // ULID
    ParentID:   "01ARZ3NDEKTSV4RRFFQ69G5FAW",  // Parent's ULID
    ParentPath: "/parent",
    Name:       "child",
    Path:       "/parent/child",
    Type:       "folder",
    Depth:      1,
}
// This automatically:
// 1. Inserts into /nodes bucket (keyed by ULID)
// 2. Adds to traversal status bucket (/levels/{level}/traversal/{status})
// 3. Updates traversal status-lookup index (/levels/{level}/traversal/status-lookup)
// 4. Updates parent's children list
err := db.InsertNodeWithIndex(database, "SRC", 1, db.StatusPending, state)

// Update node traversal status (moves between traversal status buckets and updates status-lookup)
// This automatically:
// 1. Updates NodeState in /nodes bucket
// 2. Removes from old traversal status bucket
// 3. Adds to new traversal status bucket
// 4. Updates traversal status-lookup index
nodeID := "01ARZ3NDEKTSV4RRFFQ69G5FAV"  // ULID of the node
updatedState, err := db.UpdateNodeStatusByID(database, "SRC", 1, 
    db.StatusPending, db.StatusSuccessful, nodeID)

// Get node state by ULID
state, err := db.GetNodeState(database, "SRC", nodeID)

// Get children of a node by parent ULID (parent level required)
parentID := "01ARZ3NDEKTSV4RRFFQ69G5FAW"  // Parent's ULID
parentLevel := 0  // Level of the parent (children live at parentLevel+1)
children, err := db.GetChildrenStatesByParentID(database, "SRC", parentLevel, parentID)

// Query status-lookup index (find which status bucket a node belongs to)
lookupBucket := db.GetTraversalStatusLookupBucket(tx, "SRC", 1)
nodeIDBytes := []byte(nodeID)  // Convert ULID string to bytes
statusBytes := lookupBucket.Get(nodeIDBytes)
status := string(statusBytes) // "pending", "successful", "failed", etc.
```

### Iteration

```go
// Iterate over all nodes in a status bucket
err := database.IterateStatusBucket("SRC", 1, db.StatusPending, 
    db.IteratorOptions{Limit: 100}, 
    func(nodeIDBytes []byte) error {
        // Process each pending node at level 1
        // nodeIDBytes contains the ULID of the node
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

// Lease tasks from traversal status bucket (for worker task distribution)
nodeIDs, err := database.LeaseTasksFromStatus("SRC", 1, db.StatusPending, 1000)

// Batch fetch with keys (for task leasing with deduplication)
results, err := db.BatchFetchWithKeys(database, "SRC", 1, db.StatusPending, 100)
for _, result := range results {
    // result.Key is the ULID string
    // result.State is the NodeState
}
```

### Batch Operations

```go
// Batch insert multiple nodes (automatically updates stats and join-lookup tables)
// If State.SrcID is populated, bidirectional join mappings are created
ops := []db.InsertOperation{
    {QueueType: "SRC", Level: 1, Status: db.StatusPending, State: state1},
    {QueueType: "DST", Level: 1, Status: db.StatusPending, State: state2}, // state2.SrcID set
}
err := db.BatchInsertNodes(database, ops)

// Batch delete nodes (comprehensive cleanup)
// Automatically deletes from (within the node's level shard):
// - levels/<level>/nodes bucket
// - levels/<level>/traversal|copy status buckets and status-lookup index
// - parent's children list (levels/<parentLevel>/children)
// - join-lookup tables (levels/<level>/src-to-dst, dst-to-src)
// - node's own children list (if folder)
// - stats bucket (decrements counts)
deleteOps := []db.DeleteNodeOperation{
    {QueueType: "DST", NodeID: "01ARZ3NDEKTSV4RRFFQ69G5FAV", Level: 1, Status: db.StatusSuccessful},
}
err := db.BatchDeleteNodes(database, deleteOps)

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
// Insert a log entry (buffered; written to current log shard)
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

Batches write operations (status updates, inserts, deletions, copy status, lookup mappings) with automatic coalescing and stats updates:

```go
// Create output buffer
outputBuffer := db.NewOutputBuffer(database, 500, 2*time.Second)
defer outputBuffer.Stop()

// Add status update (coalesces duplicates - last write wins)
outputBuffer.AddStatusUpdate("SRC", 1, db.StatusPending, db.StatusSuccessful, nodeID)

// Add batch insert (merges with existing batch inserts)
ops := []db.InsertOperation{
    {QueueType: "SRC", Level: 1, Status: db.StatusPending, State: state1},
    {QueueType: "SRC", Level: 1, Status: db.StatusPending, State: state2},
}
outputBuffer.AddBatchInsert(ops)

// Add node deletion (comprehensive cleanup including join tables)
outputBuffer.AddNodeDeletion("DST", nodeID, 1, db.StatusSuccessful)

// Add copy status update
outputBuffer.AddCopyStatusUpdate("SRC", 1, "file", db.StatusSuccessful, nodeID, db.CopyStatusPending)

// Add join-lookup mapping (bidirectional: src-to-dst and dst-to-src; level required)
outputBuffer.AddLookupMapping(level, srcULID, dstULID)

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
- Comprehensive node deletion (nodes, status, children, join tables, stats)
- Automatic bidirectional join-lookup mapping creation
- Automatic stats updates
- Time-based and size-based flush triggers
- Thread-safe

#### LogBuffer

Batches log entries for efficient persistence:

```go
// Create log buffer (shardCap: max entries per log shard, e.g. db.DefaultLogShardCap)
logBuffer := db.NewLogBuffer(database, 500, 2*time.Second, db.DefaultLogShardCap)
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
// Get bucket paths (level-sharded)
nodesPath := db.GetNodesBucketPath("SRC", 0)        // ["Traversal-Data", "SRC", "levels", "00000000", "nodes"]
childrenPath := db.GetChildrenBucketPath("SRC", 0)  // ["Traversal-Data", "SRC", "levels", "00000000", "children"]
levelPath := db.GetLevelBucketPath("SRC", 1)        // ["Traversal-Data", "SRC", "levels", "00000001"]
traversalStatusPath := db.GetTraversalStatusBucketPath("SRC", 1, db.StatusPending)
copyStatusPath := db.GetCopyStatusBucketPath(1, "folder", db.CopyStatusPending) // SRC only
lookupPath := db.GetTraversalStatusLookupBucketPath("SRC", 1)
// ["Traversal-Data", "SRC", "levels", "00000001", "traversal", "status-lookup"]

// Create level bucket with nodes, children, join, traversal/copy status and status-lookup index
err := db.EnsureLevelBucket(tx, "SRC", 1)

// Get bucket within transaction (level required for nodes/children/status)
nodesBucket := db.GetNodesBucket(tx, "SRC", 1)
statusBucket := db.GetTraversalStatusBucket(tx, "SRC", 1, db.StatusPending)
lookupBucket := db.GetTraversalStatusLookupBucket(tx, "SRC", 1)

// Update status-lookup index (automatically called by insert/update functions)
nodeID := "01ARZ3NDEKTSV4RRFFQ69G5FAV"  // ULID
nodeIDBytes := []byte(nodeID)
err := db.UpdateTraversalStatusLookup(tx, "SRC", 1, nodeIDBytes, db.StatusSuccessful)
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
// Atomic within single transaction (within levels/<level>/):
// 1. Update NodeState in nodes bucket
// 2. Remove from old traversal status bucket
// 3. Add to new traversal status bucket
// 4. Update traversal status-lookup index
// All four operations succeed or all fail
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

The stats bucket provides O(1) count lookups for all buckets and queue performance metrics:

```go
// Get count for any bucket path (keys stored directly in STATS bucket)
count, err := database.GetBucketCount([]string{"SRC", "levels", "00000000", "nodes"})
count, err := database.GetBucketCount([]string{"SRC", "levels", "00000001", "traversal", "pending"})

// Check if bucket has items
hasItems, err := database.HasBucketItems([]string{"SRC", "levels", "00000001", "nodes"})

// Manually synchronize all stats (useful for recovery)
err := database.SyncCounts()

// Ensure stats bucket exists
err := database.EnsureStatsBucket()

// Get queue statistics (from /STATS/queue-stats)
srcStats, err := db.GetQueueStats(database, "src-traversal")
allStats, err := db.GetAllQueueStats(database)
// Returns QueueObserverMetrics with:
// - QueueStats (pending, in-progress, workers, round, etc.)
// - AverageExecutionTime
// - TasksPerSecond
// - TotalCompleted
// - LastPollTime
```

**Stats are automatically maintained by:**
- `OutputBuffer` - Updates stats during buffered writes
- `BatchInsertNodes` - Updates stats during batch inserts
- `QueueObserver` - Publishes queue metrics to `/STATS/queue-stats` every 200ms

**Manual sync is useful for:**
- Recovery after crashes
- Correcting drift
- Initial stats population

---

## Summary

The database package provides:
- ✅ **BoltDB Integration** - Simple, reliable embedded database
- ✅ **Bucket Hierarchies** - Natural structure, not key encoding
- ✅ **Status Transitions** - Atomic, race-free status changes with status-lookup index
- ✅ **Status-Lookup Index** - O(1) reverse lookup to find node status without scanning
- ✅ **High-Level Operations** - Convenient functions for common operations
- ✅ **Batch Support** - Efficient bulk operations with automatic stats
- ✅ **Buffered Writes** - `OutputBuffer` and `LogBuffer` for high throughput
- ✅ **O(1) Statistics** - Stats bucket for fast count lookups and queue metrics
- ✅ **Queue Observability** - Real-time queue performance metrics in `/STATS/queue-stats`
- ✅ **ULID-Based Keys** - Unique, sortable identifiers for all internal operations
- ✅ **Task Leasing** - Efficient task distribution for workers
- ✅ **Thread Safety** - Safe for concurrent reads, serialized writes
- ✅ **Crash Resilience** - ACID transactions with automatic recovery

This design provides a clean, predictable storage layer for the migration engine's BFS traversal with guaranteed consistency, atomic operations, and high performance through intelligent buffering and statistics.
