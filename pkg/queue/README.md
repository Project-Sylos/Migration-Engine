# Queue System

The queue layer drives source/destination traversal using BadgerDB as the operational database. Two autonomous queues (src/dst) perform breadth‑first traversal in rounds, with the destination always staying at least three rounds behind unless the source has completed. Round coordination is handled by a shared `QueueCoordinator`, while persistence, propagation, and retries are handled entirely inside `queue.Queue`.

---

## High-level Flow

1. **Workers** execute tasks leased from BadgerDB. They list filesystem children (and for dst, compare against expectations) then return the enriched `TaskBase` to the queue.
2. **Queue completion** writes directly to BadgerDB:
   - Traversal-status transitions (pending → successful) for the processed node.
   - Copy-status transitions when dst determines src is newer.
   - Child inserts for the next BFS level.
3. **Direct writes** ensure all BadgerDB operations are immediately visible. Tasks are pulled in batches from BadgerDB, but all writes are synchronous and atomic.
4. **Coordinator gating** enforces the lead window. Both queues call `WaitForCoordinatorGate` when bootstrapping and whenever `advanceToNextRound` runs, ensuring dst only proceeds when src is sufficiently ahead (or already done).

The result is the same observable behavior as the old RoundQueue system, but without separate in-memory structures—BadgerDB serves as the single source of truth.

---

## Core Components

| File             | Responsibility                                                                 |
| ---------------- | ----------------------------------------------------------------------------- |
| `queue.go`       | BadgerDB-backed queue, buffers, leasing, completion, coordinator integration    |
| `worker.go`      | Worker loop (lease → execute → return results)                                |
| `task.go`        | Task definitions shared by both queues                                        |
| `seeding.go`     | Helpers for inserting initial root tasks into BadgerDB                       |
| `coordinator.go` | Enforces the src/dst lead window                                              |
| `db_helpers.go` | Helper functions for querying BadgerDB                                        |
| `badger_helpers.go` | Conversion utilities between tasks and node states                          |

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

## BadgerDB Persistence

All node state is stored directly in BadgerDB with structured keys:

**Key Format**: `{queueType}:{level:08d}:{traversal_status}:{copy_status}:{path_hash}`

Completion logic performs three categories of operations:

1. **Node Inserts** – new children discovered in the current round are inserted for the next round (Badger key `src:level:pending:copy_status:path_hash` plus index entries).
2. **Status Updates** – parent nodes transition from `pending` to `successful` once a worker completes.
3. **Copy Updates** – destination workers signal that src is newer; the copy-status is updated in BadgerDB so copy phases know the item requires action.

All writes are:
- **Synchronous** – written immediately to BadgerDB
- **Atomic** – each write is a transaction
- **Immediately Visible** – no buffering delays

---

## Worker Workflow

```go
task := w.queue.Lease()        // reads BadgerDB in batches
err := w.execute(task)         // list children / compare timestamps
if err != nil {
    w.queue.Fail(task)         // requeue or drop based on retry budget
} else {
    w.queue.Complete(task)     // direct writes: inserts + status updates
}
```

Destination workers still compute copy decisions, and they report them via `enqueueCopyUpdate` which writes directly to BadgerDB.

---

## Coordinator

The `QueueCoordinator` is unchanged:

- `CanSrcAdvance` ensures src is never more than `maxLead` rounds ahead of dst (unless dst already completed).
- `CanDstAdvance` requires `(dstRound + 3 <= srcRound)` (or src completed) to guarantee the next round's source data is present.
- Completion flags (`UpdateSrcCompleted`, `UpdateDstCompleted`) fire when a queue reaches max depth.

Queues call `WaitForCoordinatorGate(reason)` right after seeding and whenever they advance rounds. This mirrors the steady-state gating and keeps the lead window consistent even during startup/resume.

---

## Retry & State

* `queue.Fail` writes directly to BadgerDB so failed tasks return to pending keys immediately.
* `QueueState` (`Running`, `Paused`, `Completed`) retains its prior meaning. Workers observe these states before leasing.
* Stats (`queue.Stats`) use in-memory counters (pending buffer, in-progress map) for fast reads without BadgerDB queries.

---

## Benefits of the BadgerDB Model

1. **Single source of truth** – tasks live only in BadgerDB; there is no secondary in-memory structure to reconcile.
2. **Immediate visibility** – direct writes ensure all operations are immediately visible in BadgerDB without buffering delays.
3. **Crash resilience** – on resume, we simply scan BadgerDB to rebuild state; no volatile task queues to reconstruct.
4. **Simplicity** – direct writes eliminate buffer management complexity and potential race conditions.
5. **Atomicity** – each BadgerDB write is a transaction, ensuring consistency without batching complexity.
6. **Performance** – BadgerDB's optimized storage provides excellent read/write performance for queue operations.

---

## Summary

The queue layer uses BadgerDB with direct writes to coordinate BFS traversal. Workers only execute tasks; the queue owns persistence, propagation, and coordination. Coordinator gates still enforce the src/dst lead window, and external behavior (round advancement, retry semantics, logging) remains unchanged—just with simpler internals and immediate write visibility.
