# Queue System

The queue layer drives source/destination traversal using BoltDB as the operational database. Two autonomous queues (src/dst) perform breadth-first traversal in rounds, with the destination always staying at least three rounds behind unless the source has completed. Round coordination is handled by a shared `QueueCoordinator`, while persistence, propagation, and retries are handled entirely inside `queue.Queue`.

---

## High-level Flow

1. **Workers** execute tasks leased from BoltDB. They list filesystem children (and for dst, compare against expectations) then return the enriched `TaskBase` to the queue.
2. **Queue completion** writes directly to BoltDB:
   - Traversal-status transitions (pending → successful) for the processed node.
   - Copy-status updates when dst determines src is newer.
   - Child inserts for the next BFS level.
3. **Atomic writes** ensure all BoltDB operations are immediately visible. Tasks are pulled in batches from status buckets, and all writes are atomic within transactions.
4. **Coordinator gating** enforces the lead window. Both queues call `WaitForCoordinatorGate` when bootstrapping and whenever `advanceToNextRound` runs, ensuring dst only proceeds when src is sufficiently ahead (or already done).

The result is predictable, race-free traversal with BoltDB serving as the single source of truth.

---

## Core Components

| File             | Responsibility                                                                 |
| ---------------- | ----------------------------------------------------------------------------- |
| `queue.go`       | BoltDB-backed queue, buffers, leasing, completion, coordinator integration    |
| `worker.go`      | Worker loop (lease → execute → return results)                                |
| `task.go`        | Task definitions shared by both queues                                        |
| `seeding.go`     | Helpers for inserting initial root tasks into BoltDB                          |
| `coordinator.go` | Enforces the src/dst lead window                                              |
| `db_helpers.go`  | Helper functions for querying BoltDB buckets                                  |
| `bolt_helpers.go`| Conversion utilities between tasks and node states                            |

---

## Task Model

```go
type TaskBase struct {
    Type               TaskType
    Folder             fsservices.Folder
    File               fsservices.File
    Locked             bool
    Attempts           int
    Status             string
    DiscoveredChildren []ChildResult
    ExpectedFolders    []fsservices.Folder
    ExpectedFiles      []fsservices.File
    Round              int
}
```

* `Round` identifies the BFS depth. It is used by the coordinator, stats, and resumption logic.
* `DiscoveredChildren` is filled by workers and consumed by the queue to create next-round tasks.
* Destination workers populate `Expected*` when they need to compare against source output.

---

## BoltDB Storage Architecture

All node state is stored in BoltDB using bucket hierarchies:

### Bucket Structure

```
/SRC
  /nodes                  → pathHash: NodeState JSON
  /children               → parentHash: []childHash JSON
  /levels
    /00000001
      /pending            → pathHash: empty (membership)
      /successful         → pathHash: empty
      /failed             → pathHash: empty

/DST
  (same structure + /not_on_src status)
```

### Key Operations

**Completion writes perform three operations atomically:**

1. **Node Inserts** – New children discovered in the current round are inserted into:
   - `/nodes` bucket (full NodeState data)
   - `/levels/{nextRound}/pending` bucket (membership)
   - `/children` bucket (parent-child relationship)

2. **Status Updates** – Parent nodes transition from pending to successful:
   - Update NodeState in `/nodes` bucket
   - Remove from `/levels/{level}/pending`
   - Add to `/levels/{level}/successful`

3. **Copy Updates** – Destination workers signal that src is newer:
   - Update CopyStatus field in NodeState metadata

All writes are:
- **Atomic** – Each transaction succeeds or fails as a unit
- **Immediately Visible** – No MVCC delays; reads see latest committed state
- **Race-Free** – BoltDB's single-writer eliminates key visibility issues

---

## Worker Workflow

```go
task := w.queue.Lease()        // reads from status buckets
err := w.execute(task)         // list children / compare timestamps
if err != nil {
    w.queue.Fail(task)         // requeue or drop based on retry budget
} else {
    w.queue.Complete(task)     // atomic writes: inserts + status updates
}
```

Destination workers compute comparison results and store them in the NodeState metadata (TraversalStatus field).

---

## Task Leasing

Workers lease tasks from the current round's pending bucket:

```go
// Pull up to 1000 tasks from /SRC/levels/{round}/pending
pathHashes, err := db.LeaseTasksFromStatus("SRC", round, db.StatusPending, 1000)

// For each pathHash, get the full NodeState from /nodes
state, err := db.GetNodeState(database, "SRC", pathHash)

// Convert to TaskBase
task := nodeStateToTask(state, TaskTypeSrcTraversal)
```

**Benefits of bucket-based leasing:**
- Direct bucket access (no prefix scans needed)
- Consistent snapshot within transaction
- Race-free status transitions

---

## Coordinator

The `QueueCoordinator` manages the lead window between source and destination queues:

- `CanSrcAdvance` ensures src is never more than `maxLead` rounds ahead of dst (unless dst completed).
- `CanDstAdvance` requires `(dstRound + 3 <= srcRound)` (or src completed) to guarantee the next round's source data is present.
- Completion flags (`UpdateSrcCompleted`, `UpdateDstCompleted`) fire when a queue reaches max depth.

Queues call `WaitForCoordinatorGate(reason)` right after seeding and whenever they advance rounds. This mirrors the steady-state gating and keeps the lead window consistent even during startup/resume.

---

## Retry & State

* `queue.Fail` writes directly to BoltDB so failed tasks return to pending status immediately (or are marked failed after max retries).
* `QueueState` (`Running`, `Paused`, `Completed`) retains its prior meaning. Workers observe these states before leasing.
* Stats (`queue.Stats`) use in-memory counters (pending buffer, in-progress map) for fast reads without database queries.

---

## Benefits of the BoltDB Model

1. **Single source of truth** – Tasks live only in BoltDB; no secondary in-memory structures to reconcile.
2. **Immediate visibility** – Atomic transactions ensure all operations are immediately visible without delays.
3. **Race-free** – Single-writer model ensures consistent state transitions.
4. **Crash resilience** – On resume, we simply scan BoltDB buckets to rebuild state; no volatile queues to reconstruct.
5. **Simplicity** – Bucket-based storage with natural hierarchies.
6. **Atomicity** – Each BoltDB transaction guarantees all-or-nothing consistency.
7. **Performance** – BoltDB's B-tree provides excellent read/write performance without background compaction.

---

## Write Buffering

For performance, the queue uses a `WriteBuffer` that batches operations:

```go
// Buffer batches ~1000 operations or flushes every 3 seconds
writeBuffer := db.NewWriteBuffer(database, 1000, 3*time.Second)

// Add operations (inserts, status updates)
writeBuffer.Add(db.WriteItem{
    Type:      "insert",
    QueueType: "SRC",
    Level:     nextRound,
    Status:    db.StatusPending,
    State:     childState,
})

// Explicit flush (also auto-flushes)
writeBuffer.Flush()
```

**Order of operations:**
1. Status updates processed first (parent transitions)
2. Then inserts (children added)
3. All in single atomic transaction

This ensures readers never see inconsistent state (e.g., parent updated but children missing).

---

## Round Advancement

When a round completes (no pending tasks, no in-progress tasks):

1. Check if next round has tasks: `count, _ := db.CountStatusBucket(queueType, nextRound, db.StatusPending)`
2. If no tasks, mark queue as completed
3. Otherwise, advance to next round
4. Wait for coordinator gate before pulling new tasks
5. Pull tasks from new round's pending bucket

---

## Resumption

To resume a migration:

1. Open existing BoltDB database
2. Scan all level buckets to find minimum pending level for each queue
3. Set queue round to that level
4. Workers start leasing from pending buckets at that level
5. Coordinator enforces lead window as normal

No special resumption logic needed; the bucket structure naturally represents the current state.

---

## Summary

The queue layer uses BoltDB's bucket hierarchies to coordinate BFS traversal. Workers execute tasks; the queue owns persistence, propagation, and coordination. The coordinator gates enforce the src/dst lead window, and external behavior (round advancement, retry semantics, logging) remains unchanged—but with simpler internals, no MVCC races, and guaranteed consistency.

**Key architectural win:** Status is membership in a bucket, not encoded in a key. This makes transitions atomic and intuitive.
