# Queue Package

The queue layer drives source/destination traversal and copy using the database (`pkg/db`). Two queues (src and dst) perform breadth-first traversal in rounds, with the destination gated by a shared `QueueCoordinator`. The **memory-first** flow keeps per-level state in `NodeCache`/`LevelCache`; the DB is only written at **seal** (round advance). The coordinator enforces a configurable SRC-ahead gate (default 3 rounds).

---

## High-level flow

1. **Workers** lease tasks from an in-memory buffer refilled by **pulling**: when `NodeCache` is set, pull from the level cache first (e.g. `LevelCache.ListPending`, `ListPendingCopy`); otherwise from the DB via keyset queries (`db.ListNodesByDepthKeyset`, `db.ListDstBatchWithSrcChildren`, `db.ListNodesCopyKeyset`).
2. **Completion** updates: when cache is set, the queue updates the level cache (status, children, copy status) and per-level stats; no DB write until seal. When cache is not set, completion goes through the DB’s staging/node buffers as before.
3. **Round advancement**: when the current round is complete, the queue calls **seal** (in `advanceToNextRound`): snapshot the level from cache, call `database.SealLevel(...)` to bulk-append nodes and write stats for that depth, then drop the level and (for traversal) promote N+1 to N. No staging merge on the memory-first path.
4. **Coordinator gating**: DST may start a round only when SRC is far enough ahead; SRC may not run more than `MaxSrcAhead` rounds ahead of DST (default 3, configurable via `MigrationConfig.MaxSrcAhead`).

---

## Relationship with pkg/db

- **Database**: The queue holds a `*db.DB` reference. When **NodeCache** is set (memory-first), hot-path reads and writes use the cache; the DB is only used at seal (`SealLevel`: bulk append nodes + stats snapshot) and for resume rehydration (`RehydrateLevelFromDB`). When cache is not set, pulls and writes use the DB as before (staging/node buffers, flush before pull).
- **Pulls**: From cache: `LevelCache.ListPending`, `ListPendingCopy`, `ListChildrenByParentPath`. From DB: `db.ListNodesByDepthKeyset`, `db.ListDstBatchWithSrcChildren`, `db.ListNodesCopyKeyset`. Results become `TaskBase` and are added to `pendingBuff`.
- **Seal**: `database.SealLevel(table, depth, nodes, pending, successful, failed, completed, copyP, copyS, copyF)` persists one level to the DB and writes per-depth stats. Copy stats (copyP, copyS, copyF) are used for SRC in copy phase; pass -1 for traversal-only.
- **Completion checks**: When cache is set, completion uses in-memory level stats and cache state; when not, `database.GetCopyCountAtDepth` and stats tables. Observer reads from stats tables and writes `queue_stats`.

---

## Core components

| File                  | Responsibility |
|-----------------------|----------------|
| `queue.go`            | Queue struct, Run loop, seal (advanceToNextRound → SealLevel), coordinator gates, leasing |
| `level_cache.go`      | LevelCache, NodeCache: per-level nodes and stats; ListPending, ListPendingCopy, DropLevel, PromoteLevel |
| `queue_accessors.go`  | Thread-safe getters/setters, keyset cursors, syncLevelStatsFromDB, RehydrateLevelFromDB |
| `queue_batch.go`      | BuildExpectedMapsFromDstWithChildren, batch load expected children for DST tasks |
| `mode_traversal.go`   | PullTraversalTasks (cache-first or DB keyset), traversal completion (cache or AddToStaging/AddNode) |
| `mode_retry.go`       | PullRetryTasks; DST cleanup on SRC folder complete |
| `mode_copy.go`        | PullCopyTasks (cache or DB), copy completion (cache or AddCopyToStaging/AddNode), CheckCopyCompletion |
| `worker_traversal.go` | TraversalWorker: lease → list children / compare → ReportTaskResult |
| `worker_copy.go`      | CopyWorker: lease → create folder / copy file → ReportTaskResult |
| `worker/interface.go` | Worker interface |
| `task.go`             | TaskBase, ChildResult, task types |
| `seeding.go`          | SeedRootTask, SeedRootTasks (insert root nodes, BootstrapRootStats) |
| `coordinator.go`      | QueueCoordinator: CanDstStartRound, CanSrcStartRound, SetMaxSrcAhead (default 3) |
| `observer.go`         | Polls queues, reads stats from DB, writes queue_stats |

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

## Storage

- **Memory-first**: When `NodeCache` is set, active levels (current and next) live in `LevelCache` per queue. Task completion updates the cache and in-memory `LevelStats`. At seal, the level is bulk-appended to the DB and stats are written; the level is then dropped (and N+1 promoted for traversal).
- **Database** (`pkg/db`): Node tables (`src_nodes`, `dst_nodes`), stats tables (`src_stats`, `dst_stats`), and optional staging tables. Seal writes nodes and stats in one transaction. Resume uses `RehydrateLevelFromDB` to refill the cache from the DB.
- **queue_stats**: Observer writes per-queue metrics JSON for external APIs.

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

- **Lease**: Task is taken from `pendingBuff` and added to `inProgress`; its key is tracked so it is not pulled again (cache path: status updated in cache; DB path: until `onFlush` after buffer flush).
- **ReportTaskResult**: With cache, the queue updates the level cache and stats; with DB path, it calls `AddToStaging`/`AddCopyToStaging`/`AddNode`/etc. Seal (memory-first) or staging merge (legacy) persists at round advance.

---

## Task pulling

- **Pulling flag**: Only one pull runs at a time (`getPulling` / `setPulling`).
- **Cache-first**: When `NodeCache` is set, pull uses `LevelCache.ListPending` / `ListPendingCopy` for the current round; DB is used only when the level is missing (e.g. resume) or for parent/expected lookups. When cache is not set, `FlushTablesForQueue` is called before pull.
- **State checks**: Pull only when queue is running and pending count is at or below the low-water mark (or when forced).
- **Coordinator**: DST checks `CanDstStartRound(currentRound)`; SRC checks `CanSrcStartRound(currentRound)` (SRC may not run more than `MaxSrcAhead` rounds ahead of DST).

**Traversal**: From cache: `GetLevel(round).ListPending(cursor, batchSize)`; from DB: `db.ListNodesByDepthKeyset` (and for DST, `ListDstBatchWithSrcChildren` or expected from other cache).

**Copy**: From cache: `GetLevel(round).ListPendingCopy(cursor, limit, nodeType)`; from DB: `db.ListNodesCopyKeyset`.

**Retry**: Same as traversal but over multiple depths (up to `maxKnownDepth`).

---

## Coordinator

- **QueueCoordinator** keeps SRC and DST round numbers and enforces gating.
- **CanDstStartRound**: DST may start round N only when SRC has completed rounds N and N+1 (SRC round >= N+2) or SRC is complete.
- **CanSrcStartRound**: SRC may run round R only when R <= dstRound + MaxSrcAhead (default 3); configurable via `SetMaxSrcAhead` / `MigrationConfig.MaxSrcAhead`.
- Completion is marked when a queue reaches max depth. Round updates are reported via `UpdateRound` when advancing.

---

## Queue modes

### Traversal (`QueueModeTraversal`)

- Pull: from cache (`ListPending`) or DB keyset by depth + `traversal_status = 'pending'`.
- Completion: update cache (status, children, stats) or staging/node buffers. At seal: `SealLevel` bulk-appends level and writes stats; level dropped, N+1 promoted.
- DST uses coordinator gate and gets expected children from SRC (cache or `ListDstBatchWithSrcChildren`).

### Retry (`QueueModeRetry`)

- Pull: pending/failed across known depths. On SRC folder success: DST cleanup (mark DST parent pending, `AddNodeDeletions` for children). Same cache or staging write path as traversal.

### Copy (`QueueModeCopy`)

- Pull: from cache (`ListPendingCopy`) or DB by `copy_status = 'pending'` and optional type (folder/file pass).
- Completion: update SRC copy status and DST node in cache (or `AddCopyToStaging`/`AddNode`). At seal, both SRC and DST levels are sealed with copy stats for SRC.
- Completion check: no pending/in-progress for current pass (from cache stats or `GetCopyCountAtDepth`).

---

## Completion checking

- **Run() loop** polls; completion is not event-driven from task completion.
- **checkCompletion**: Checks in-memory state (in-progress, pending, last pull partial) and, for copy mode, cache stats or DB counts for pending/in_progress. When cache is set, `FlushBuffer` is not used.
- **Round complete**: `advanceToNextRound` runs seal (bulk append + stats snapshot via `SealLevel`, then drop/promote level) or legacy staging merge.
- **Final completion**: When the first pull of a round returns 0 items and in-progress and pending are 0, the queue is marked completed (mode-specific in `CheckTraversalCompletion` / `CheckCopyCompletion`).

---

## Observer

- Polls queues periodically and reads stats from the database (`GetStatsCountAtDepth`, etc.) to compute pending/failed totals.
- Writes aggregated metrics to the `queue_stats` table via `database.RunUpdateWriterTx` and `Writer.WriteQueueStats` (keyed e.g. by `src-traversal`, `dst-traversal`, `copy`).

---

## Resumption

- Open the existing database (same file as before).
- **InspectMigrationStatus** (in `pkg/migration`) reads node counts and stats from the DB to get pending/failed and min pending depth.
- Set queue round from that state. When **NodeCache** is set, `RehydrateLevelFromDB(depth)` is called for each queue so the level cache is refilled from the DB and level stats are synced; workers then pull from cache. Otherwise workers pull from keyset queries at the resumed round. The DB is the source of truth for sealed state.

---

## File layout

```
pkg/queue/
├── queue.go           # Queue struct, Run, advanceToNextRound (seal), Lease, ReportTaskResult, InitializeWithContext
├── level_cache.go     # LevelCache, NodeCache (per-level nodes and stats)
├── level_cache_test.go
├── coordinator.go     # QueueCoordinator, CanDstStartRound, CanSrcStartRound, SetMaxSrcAhead
├── coordinator_test.go
├── queue_accessors.go # Getters/setters, syncLevelStatsFromDB, RehydrateLevelFromDB
├── queue_batch.go     # BuildExpectedMapsFromDstWithChildren, BatchLoadExpectedChildrenByDSTIDs
├── mode_traversal.go  # PullTraversalTasks, traversal completion (cache or staging)
├── mode_retry.go      # PullRetryTasks
├── mode_copy.go       # PullCopyTasks, copy completion, CheckCopyCompletion
├── worker_traversal.go
├── worker_copy.go
├── worker/
│   └── interface.go
├── task.go
├── seeding.go
├── observer.go
└── README.md
```

---

## Summary

- **Memory-first**: When `NodeCache` is set, hot-path reads/writes use per-level caches; the DB is written only at seal (`SealLevel`) and used for resume rehydration. Legacy path uses staging/node buffers and merge at seal.
- **Pull**: From cache (ListPending / ListPendingCopy) or DB keyset queries; results go into `pendingBuff` and are leased to workers.
- **Seal**: At round advance, sealed level is bulk-appended to the DB and stats snapshot written; level is dropped (and N+1 promoted for traversal).
- **Coordinator** enforces DST lead (SRC ahead by N+2) and SRC-ahead cap (default 3). **Observer** reads stats from the DB and writes `queue_stats`.
- **Modes**: Traversal (BFS by round), Retry (re-process pending/failed, DST cleanup), Copy (by copy_status and type, two passes).
