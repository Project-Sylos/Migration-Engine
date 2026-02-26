# Queue Package

The queue layer drives source/destination traversal and copy using the database (`pkg/db`). Two queues (src and dst) perform breadth-first traversal in rounds, with the destination gated by a shared `QueueCoordinator`. Task state lives in the database’s node and stats tables; the queue pulls via keyset queries and writes via the database’s buffered staging and node APIs.

---

## High-level flow

1. **Workers** lease tasks from an in-memory buffer that is refilled by **pulling** from the database: keyset queries by depth and status (e.g. `db.ListNodesByDepthKeyset`, `db.ListDstBatchWithSrcChildren`, `db.ListNodesCopyKeyset`).
2. **Completion** writes go through the database’s buffers: `database.AddToStaging`, `database.AddCopyToStaging`, `database.AddNode`, `database.AddNodes`, and (in retry) `database.AddNodeDeletions`. The DB batches these and flushes via DuckDB Appender; the queue registers an `onFlush` callback so leased keys are removed when a batch is persisted.
3. **Before each pull** the queue calls `database.FlushTablesForQueue(queueType)` so pending writes are persisted and pulls see up-to-date status.
4. **Round advancement** happens when the current round is complete (no in-progress, no pending, last pull was partial). At **seal**, the migration layer (or queue) merges staging into the live node tables and recomputes stats (see `pkg/db`).
5. **Coordinator gating**: DST checks the coordinator before starting a round; SRC may be gated so it does not run more than a few rounds ahead of DST.

---

## Relationship with pkg/db

- **Database**: The queue holds a `*db.DB` reference (`q.database`). It does not open or close the DB. All reads and writes use that single instance (node tables, staging, stats, `queue_stats`).
- **Pulls**: Use `db.ListNodesByDepthKeyset`, `db.ListDstBatchWithSrcChildren`, and `db.ListNodesCopyKeyset` with depth, status filter, and keyset cursor. Results are converted to `TaskBase` and added to the queue’s `pendingBuff`.
- **Writes**: Queue calls `database.AddToStaging`, `database.AddCopyToStaging`, `database.AddNode`, `database.AddNodes`, `database.AddNodeDeletions`. The DB owns the buffers and flushes to staging/node tables; the queue’s `SetOnFlush` callback removes leased keys when a batch is flushed.
- **Completion checks**: Use `database.GetCopyCountAtDepth` (copy mode) and in-memory state (in-progress, pending count, last pull partial). Stats for expected/completed come from `database.GetStatsCountAtDepth` and the stats tables.
- **Observer**: Reads pending/failed from stats tables (`GetStatsCountAtDepth`); writes metrics to the `queue_stats` table via `database.RunUpdateWriterTx` and `Writer.WriteQueueStats`.

---

## Core components

| File                  | Responsibility |
|-----------------------|----------------|
| `queue.go`            | Queue struct, Run loop, completion checks, buffer flush before pull, coordinator integration, leasing |
| `queue_accessors.go`  | Thread-safe getters/setters, keyset cursors, expected-from-stats, round stats |
| `queue_batch.go`      | BuildExpectedMapsFromDstWithChildren, BatchLoadExpectedChildrenByDSTIDs (expected children for DST tasks) |
| `mode_traversal.go`   | PullTraversalTasks (keyset pull by depth + pending), traversal completion writes (AddToStaging, AddNode, etc.) |
| `mode_retry.go`       | PullRetryTasks (pending/failed across levels), same write path as traversal; DST cleanup on SRC folder complete |
| `mode_copy.go`        | PullCopyTasks (keyset pull by depth + copy_status pending, optional type filter), copy completion (AddCopyToStaging, AddNode for DST) |
| `worker_traversal.go` | TraversalWorker: lease → list children / compare → ReportTaskResult |
| `worker_copy.go`      | CopyWorker: lease → create folder / copy file → ReportTaskResult |
| `worker/interface.go` | Worker interface |
| `task.go`             | TaskBase, ChildResult, task types |
| `seeding.go`          | SeedRootTask, SeedRootTasks (insert root nodes via db.InsertRootNode, BootstrapRootStats) |
| `coordinator.go`      | QueueCoordinator: lead window, CanDstStartRound, CanSrcAdvance, completion flags |
| `observer.go`         | Polls queues, reads stats from DB, writes queue_stats for external metrics |

---

## Task model

```go
type TaskBase struct {
    ID                 string
    Type               string   // e.g. TaskTypeSrcTraversal, TaskTypeDstTraversal, TaskTypeCopyFolder
    Folder             types.Folder
    File               types.File
    Locked             bool
    Attempts           int
    Status             string
    ExpectedFolders    []types.Folder   // DST: expected from SRC
    ExpectedFiles      []types.File
    ExpectedSrcIDMap   map[string]string
    ExpectedSrcNodeMeta map[string]SrcNodeMeta
    RetryDstCleanup   *RetryDstCleanup  // Retry mode: DST counterpart + children for cleanup
    DiscoveredChildren []ChildResult
    Round              int
    LeaseTime          time.Time
    // ...
}
```

- **Round** is the BFS depth; used by coordinator, stats, and pull queries.
- **DiscoveredChildren** is filled by workers and used by the queue to build node inserts and staging updates.
- DST tasks get **Expected*** and **ExpectedSrcIDMap** from the pull (e.g. `ListDstBatchWithSrcChildren` or batch load by parent path).

---

## Storage (database tables)

Task state is not stored in the queue; it lives in `pkg/db`:

- **Node tables** (`src_nodes`, `dst_nodes`): One row per node; columns include `id`, `path`, `depth`, `traversal_status`, `copy_status`. Pulls filter by `depth` and status; keyset pagination uses `id > cursor ORDER BY id LIMIT n`.
- **Staging tables** (`src_staging`, `dst_staging`): Pending status updates (traversal and copy); merged into node tables at **seal** (round advance).
- **Stats tables** (`src_stats`, `dst_stats`): Per-depth counts by status (e.g. traversal/pending, traversal/successful). Used for completion detection and observer.
- **queue_stats**: Observer writes per-queue metrics JSON here for external APIs.

There are no level-sharded buckets; the queue uses the same schema as described in `pkg/db`.

---

## Worker workflow

```go
task := w.queue.Lease()           // from pendingBuff (refilled by pull from DB)
err := w.execute(task)           // list children or compare / copy
if err != nil {
    w.queue.ReportTaskResult(task, Failed)
} else {
    w.queue.ReportTaskResult(task, Successful)  // AddToStaging, AddNode, etc.
}
```

- **Lease**: Task is taken from `pendingBuff` and added to `inProgress`; its key is tracked so it is not pulled again until `onFlush` removes it after the DB flushes that node’s update.
- **ReportTaskResult**: Queue calls `database.AddToStaging` (and optionally `AddCopyToStaging`, `AddNode`, `AddNodes`, `AddNodeDeletions`). Writes are buffered in the DB; `FlushTablesForQueue` is used before pulls and at completion checks so the DB is up to date.

---

## Task pulling

- **Pulling flag**: Only one pull runs at a time (`getPulling` / `setPulling`).
- **Flush before pull**: `database.FlushTablesForQueue(getQueueType(q.name))` so pending staging/node writes are visible.
- **State checks**: Pull only when queue is running and pending count is at or below the low-water mark (or when forced).
- **Coordinator (DST)**: Before pulling, DST checks `coordinator.CanDstStartRound(currentRound)`.
- **SRC gating**: SRC may skip pull if it is more than `MaxSrcDstGap` rounds ahead of DST.

**Traversal**: `db.ListNodesByDepthKeyset(database, queueType, currentRound, cursor, db.StatusPending, batchSize)` (and for DST, `db.ListDstBatchWithSrcChildren` to get DST batch plus SRC children in one query).

**Copy**: `db.ListNodesCopyKeyset(database, depth, nodeType, cursor, limit)` for `copy_status = 'pending'`.

**Retry**: Same as traversal but over multiple depths (up to `maxKnownDepth`); pull pending/failed from each level.

---

## Coordinator

- **QueueCoordinator** keeps SRC and DST round numbers and enforces the lead window.
- **CanDstStartRound**: DST may start a round only when SRC is far enough ahead (or SRC is complete).
- **CanSrcAdvance**: SRC may not run arbitrarily far ahead of DST.
- Completion flags are updated when a queue reaches max depth.

Queues call `WaitForCoordinatorGate` after seeding and when advancing rounds.

---

## Queue modes

### Traversal (`QueueModeTraversal`)

- Pull: pending nodes at current round (keyset by depth + `traversal_status = 'pending'`).
- Advance round when round is complete; at seal, staging is merged into live and stats recomputed.
- DST uses coordinator gate and gets expected children from SRC (e.g. via `ListDstBatchWithSrcChildren`).

### Retry (`QueueModeRetry`)

- Pull: pending/failed across known depths (for re-processing marked subtrees).
- On SRC folder task success: DST cleanup (mark DST parent pending, delete DST children via `AddNodeDeletions`) so DST can re-discover.
- Same staging/node write path as traversal.

### Copy (`QueueModeCopy`)

- Pull: `copy_status = 'pending'` at current depth (optional filter by node type for folder vs file pass).
- Completion: `AddCopyToStaging` for SRC copy status; `AddNode` for DST when creating folders/files.
- Round advancement uses a hard check: `GetCopyCountAtDepth(round, nodeType, pending)` and `GetCopyCountAtDepth(round, nodeType, in_progress)` must be 0 for the current pass.

---

## Completion checking

- **Run() loop** polls; completion is not event-driven from task completion.
- **checkCompletion** (with options): If `FlushBuffer` is true, calls `FlushTablesForQueue` first. Then checks in-memory state (in-progress, pending, last pull partial) and, for copy mode, DB counts for pending/in_progress at the current round and pass.
- **Round complete**: Advance round (e.g. `advanceToNextRound`); seal is performed by the migration layer (merge staging, recompute stats).
- **Final completion**: When the first pull of a round returns 0 items and in-progress and pending are 0, the queue is marked completed (mode-specific logic in `CheckTraversalCompletion` / `CheckCopyCompletion`).

---

## Observer

- Polls queues periodically and reads stats from the database (`GetStatsCountAtDepth`, etc.) to compute pending/failed totals.
- Writes aggregated metrics to the `queue_stats` table via `database.RunUpdateWriterTx` and `Writer.WriteQueueStats` (keyed e.g. by `src-traversal`, `dst-traversal`, `copy`).

---

## Resumption

- Open the existing database (same file as before).
- **InspectMigrationStatus** (in `pkg/migration`) reads node counts and stats from the DB to get pending/failed and min pending depth.
- Set queue round from that state; workers pull from the same keyset queries at the resumed round. No separate resumption format; the node and stats tables are the source of truth.

---

## File layout

```
pkg/queue/
├── queue.go           # Queue struct, Run, checkCompletion, Lease, ReportTaskResult, InitializeWithContext
├── queue_accessors.go # Getters/setters, keyset cursors, setExpectedFromStatsBucket
├── queue_batch.go     # BuildExpectedMapsFromDstWithChildren, BatchLoadExpectedChildrenByDSTIDs
├── mode_traversal.go  # PullTraversalTasks, traversal completion writes
├── mode_retry.go      # PullRetryTasks
├── mode_copy.go       # PullCopyTasks, copy completion, CheckCopyCompletion
├── worker_traversal.go
├── worker_copy.go
├── worker/
│   └── interface.go
├── task.go
├── seeding.go
├── coordinator.go
├── observer.go
└── README.md
```

---

## Summary

- The queue uses **one database** (`*db.DB`): node tables, staging, and stats (see `pkg/db`). No separate store.
- **Pull**: Keyset queries by depth and status; results go into `pendingBuff` and are leased to workers.
- **Writes**: Staging and node inserts via the DB’s buffer API; flush before pulls and at completion checks; staging is merged at seal by the migration layer.
- **Coordinator** enforces the SRC/DST lead window. **Observer** reads stats from the DB and writes queue metrics to `queue_stats`.
- **Modes**: Traversal (BFS by round), Retry (re-process pending/failed across depths, with DST cleanup), Copy (by copy_status and optional type).
