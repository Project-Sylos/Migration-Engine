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
4. **Coordinator gating** enforces the lead window. DST queues check the coordinator gate at the start of each round iteration in the `Run()` loop, ensuring dst only proceeds when src is sufficiently ahead (or already done). Gating happens when STARTING a round, not when advancing.

The result is predictable, race-free traversal with BoltDB serving as the single source of truth.

---

## Core Components

| File                  | Responsibility                                                                 |
| --------------------- | ----------------------------------------------------------------------------- |
| `queue.go`            | BoltDB-backed queue, buffers, leasing, completion, coordinator integration    |
| `worker.go`           | Worker loop (lease → execute → return results)                                |
| `worker_traversal.go` | Traversal-specific worker execution logic                                     |
| `task.go`             | Task definitions shared by both queues                                        |
| `seeding.go`          | Helpers for inserting initial root tasks into BoltDB                          |
| `coordinator.go`      | Enforces the src/dst lead window                                              |
| `db_helpers.go`       | Helper functions for querying BoltDB buckets                                  |
| `bolt_helpers.go`     | Conversion utilities between tasks and node states                            |
| `mode_traversal.go`   | Task pulling logic for normal traversal mode                                  |
| `mode_retry.go`       | Task pulling logic for retry sweep mode                                       |

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
task := w.queue.Lease()                    // reads from status buckets
err := w.execute(task)                     // list children / compare timestamps
if err != nil {
    w.queue.ReportTaskResult(task, Failed)  // requeue or drop based on retry budget
} else {
    w.queue.ReportTaskResult(task, Successful)  // atomic writes: inserts + status updates
}
```

**Task Lifecycle:**
- Tasks are pulled from BoltDB and added to `pendingBuff`
- When leased, tasks move from `pendingBuff` to `inProgress`
- On completion/failure, tasks are removed from `inProgress` and results are written to BoltDB
- `ReportTaskResult()` handles both success and failure cases, and triggers task pulling when the buffer is low

Destination workers compute comparison results and store them in the NodeState metadata (TraversalStatus field).

---

## Task Leasing and Pulling

Workers lease tasks from the current round's pending bucket. Task pulling is coordinated to prevent race conditions and duplicate processing.

### Pulling Architecture

**Pulling Flag Protection**:
Both traversal and retry modes use a `pulling` flag to prevent concurrent pull operations:

```go
// Check if already pulling (prevents concurrent pulls)
if q.getPulling() {
    return
}

// Set pulling flag and defer clearing
q.setPulling(true)
defer func() {
    q.setPulling(false)
}()
```

This ensures only one thread can execute pull logic at a time, preventing:
- Duplicate task pulls from stale views
- Race conditions between buffer writes and task pulls
- Over-pulling beyond the configured batch size

**Buffer Flushing Before Pulls**:
Both modes force-flush the OutputBuffer before pulling tasks:

```go
// Force-flush buffer before pulling tasks
outputBuffer := q.getOutputBuffer()
if outputBuffer != nil {
    outputBuffer.Flush()
}
```

This ensures all pending writes (status updates, child inserts, deletions) are persisted to BoltDB before new tasks are pulled, preventing:
- Re-pulling tasks that were just completed but not yet written to DB
- Stale status views causing duplicate processing
- Non-deterministic behavior based on flush timing

**State Checks**:
Before pulling, both modes verify the queue state:

```go
// Get state snapshot
snapshot := q.getStateSnapshot()

if !force {
    // Only pull if queue is running and buffer is low
    if snapshot.State != QueueStateRunning || snapshot.PendingCount > snapshot.PullLowWM {
        return
    }
} else {
    // Even when forcing, don't pull if paused
    if snapshot.State == QueueStatePaused {
        return
    }
}
```

**Coordinator Gating (DST Queues)**:
DST queues check the coordinator gate before pulling:

```go
// For DST: Check coordinator gate before pulling
coordinator := q.getCoordinator()
if q.name == "dst" && coordinator != nil {
    canStartRound := coordinator.CanDstStartRound(currentRound)
    if !canStartRound {
        // Can't start this round yet - wait for SRC to advance
        return
    }
}
```

This ensures DST stays at least 3 rounds behind SRC (or waits for SRC completion).

### Batch Fetching

```go
// Pull up to 1000 tasks from /SRC/levels/{round}/pending
batch, err := db.BatchFetchWithKeys(boltDB, queueType, currentRound, db.StatusPending, defaultLeaseBatchSize)

// For each item in batch:
for _, item := range batch {
    // Skip ULIDs already leased (prevents duplicate pulls)
    if q.isLeased(item.Key) {
        continue
    }
    
    // Mark as leased
    q.addLeasedKey(item.Key)
    
    // Convert to TaskBase
    task := nodeStateToTask(item.State, taskType)
    
    // Enqueue for workers
    q.enqueuePending(task)
}
```

**Benefits of this architecture:**
- Direct bucket access (no prefix scans needed)
- Consistent snapshot within transaction
- Race-free status transitions
- Prevents duplicate processing via pulling flag and lease tracking
- Deterministic behavior via buffer flushing
- Proper coordination between SRC and DST queues

---

## Coordinator

The `QueueCoordinator` manages the lead window between source and destination queues:

- `CanSrcAdvance` ensures src is never more than `maxLead` rounds ahead of dst (unless dst completed).
- `CanDstAdvance` requires `(dstRound + 3 <= srcRound)` (or src completed) to guarantee the next round's source data is present.
- Completion flags (`UpdateSrcCompleted`, `UpdateDstCompleted`) fire when a queue reaches max depth.

Queues call `WaitForCoordinatorGate(reason)` right after seeding and whenever they advance rounds. This mirrors the steady-state gating and keeps the lead window consistent even during startup/resume.

---

## Queue Modes

The queue system supports two operating modes:

### 1. Traversal Mode (Normal Operation)

**Mode**: `QueueModeTraversal`

**Purpose**: Standard BFS traversal discovering nodes level-by-level.

**Behavior**:
- Pulls pending tasks from current round's status buckets
- Advances rounds when all pending tasks at current level are complete
- DST queue waits for SRC to be at least 3 rounds ahead (coordinator gating)
- Expected children for DST tasks are loaded from SRC via join-lookup tables

**Implementation**: `mode_traversal.go` - `PullTraversalTasks()`

### 2. Retry Mode (Retry Sweeps)

**Mode**: `QueueModeRetry`

**Purpose**: Re-traverse failed or marked pending subtrees to discover new/changed content.

**Behavior**:
- Scans all known levels (up to `maxKnownDepth`) for pending/failed tasks
- Re-processes marked subtrees as if doing fresh traversal
- For SRC tasks: On successful completion, triggers DST cleanup
  - Marks corresponding DST parent nodes as pending
  - Deletes DST children to allow fresh re-discovery
- For DST tasks: Loads expected children from SRC (same as traversal mode)
- Uses same coordinator gating as traversal mode

**Implementation**: `mode_retry.go` - `PullRetryTasks()`

**DST Cleanup Logic**:
When a SRC folder task completes successfully in retry mode:
1. Lookup corresponding DST node using join-lookup table (`src-to-dst`)
2. Mark DST parent as `pending` (queued via OutputBuffer)
3. Query DST children bucket for child node IDs
4. Delete all DST children (queued via OutputBuffer using `AddNodeDeletion`)
5. DST queue will re-process the parent and discover fresh children

This ensures DST stays synchronized with SRC during retry sweeps without duplicate nodes.

---

## Retry & State

* `ReportTaskResult()` handles both successful and failed task completion, writing directly to BoltDB. Failed tasks return to pending status immediately (or are marked failed after max retries).
* `QueueState` (`Running`, `Paused`, `Completed`, `Waiting`) controls queue behavior. Workers observe these states before leasing.
* Stats (`queue.Stats`) use in-memory counters (pending buffer, in-progress map) for fast reads without database queries.

## Completion Checking

Completion checks are performed by the `Run()` polling loop, not event-driven from task completion. This avoids race conditions where multiple workers complete tasks simultaneously.

**Centralized Logic:**
- `checkCompletion()` is a unified method that handles both round completion and DST queue completion
- Uses `CompletionCheckOptions` to configure behavior (flush buffer, advance round, mark DST complete, etc.)
- All completion checks force-flush the output buffer before querying the database to ensure writes are persisted

**DST Completion:**
- DST queues check for completion at multiple points: before pulling tasks (for rounds > 0), after getting coordinator gate approval, and periodically in the polling loop
- Round 0 is skipped for DST completion checks (bootstrap round)
- Uses `RequireFirstPull` flag to prevent false positives on the first pull attempt

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

## Direct Writes

The queue writes directly to BoltDB when tasks complete or fail:

```go
// On task completion, writes are atomic:
// 1. Update parent status: pending → successful
// 2. Insert all discovered children
err := database.Update(func(tx *bolt.Tx) error {
    // Both operations in single transaction
    updateNodeStatusInTx(tx, queueType, level, oldStatus, newStatus, path)
    batchInsertNodesInTx(tx, childOperations)
    return nil
})
```

This ensures immediate consistency without buffering delays.

**Order of operations:**
1. Status updates processed first (parent transitions)
2. Then inserts (children added)
3. All in single atomic transaction

This ensures readers never see inconsistent state (e.g., parent updated but children missing).

---

## Round Advancement

Round advancement is controlled by the `Run()` polling loop, which checks for completion every 100ms. This polling-based approach avoids race conditions that can occur with event-driven completion checks.

**Completion Detection:**
- The `Run()` loop periodically checks if a round is complete using the condition: `inProgress == 0 && pendingBuff == 0 && lastPullWasPartial == true`
- `lastPullWasPartial` is set when a pull operation returns fewer tasks than requested (`len(batch) < defaultLeaseBatchSize`), indicating the round may be exhausted
- When these conditions are met, the loop calls `checkCompletion()` which:
  1. Force-flushes the output buffer to ensure all writes are persisted
  2. Verifies the round is truly complete (no in-progress, no pending, last pull was partial)
  3. Advances to the next round if complete
  4. Pulls tasks from the new round's pending bucket

**Buffer Flushing:**
- Before any completion check queries the database, the output buffer is force-flushed to ensure all pending writes (status updates, child inserts) are persisted
- This prevents false positives where tasks exist in the buffer but haven't been written to the database yet

**Coordinator Gating:**
- DST queues check the coordinator gate at the start of each round iteration (before pulling tasks)
- SRC queues advance freely without gating
- Gating only happens when STARTING a round, not when advancing

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
