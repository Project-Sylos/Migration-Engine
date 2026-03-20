# Queue Package

The queue layer drives **source** and **destination** traversal and **copy** using **`pkg/db`**. Two queues (`src` and `dst`, plus `copy` in copy mode) perform breadth-first work in **rounds** (depth). The destination is gated by **`QueueCoordinator`**.  

**Frontier today is database-backed:** pending work is **pulled from DuckDB** in batches (~10k rows, ID keyset pagination) into an in-memory **`pendingBuff`**; workers **lease** tasks from that buffer. Completed work is flushed via **`db.SealLevel`** (optionally through **`SealBuffer`** for async seal). There is **no** separate `NodeCache` / `LevelCache` source file—the model is “DB + pending buffer + seal,” not a pure memory-first level cache.

---

## High-level flow

1. **Pull** – `PullTraversalTasks` / `PullRetryTasks` / `PullCopyTasks` refill `pendingBuff` from SQL (`ListNodesByDepthKeyset`, `ListDstBatchWithSrcChildren`, copy keysets, etc.). Only one pull runs at a time (`getPulling` / `setPulling`).
2. **Lease** – Workers take tasks from `pendingBuff` into `inProgress`.
3. **Complete** – `ReportTaskResult` updates state and enqueues **seal** work (nodes + per-depth stats) through the DB layer.
4. **Coordinator** – DST may start round *N* only when SRC has completed rounds *N* and *N+1* (or SRC is done). SRC may stay at most **`maxSrcAhead`** rounds ahead of DST (queue default **2**; set **`MigrationConfig.MaxSrcAhead`** in `RunMigration` to override).

---

## Core components

| File | Responsibility |
|------|----------------|
| `queue.go` | `Queue`, `Run`, round advancement, seal handoff, `Lease`, `ReportTaskResult`, `InitializeWithContext` |
| `queue_accessors.go` | Thread-safe getters/setters, keyset cursors, traversal “cache loaded” flags |
| `queue_batch.go` | `BuildExpectedMapsFromDstWithChildren`, batch expected children for DST |
| `mode_traversal.go` | `PullTraversalTasks` (DB keyset → tasks) |
| `mode_retry.go` | `PullRetryTasks`; DST cleanup on SRC folder complete in retry mode |
| `mode_copy.go` | `PullCopyTasks`, copy completion, `CheckCopyCompletion` |
| `worker_traversal.go` | List children / compare → `ReportTaskResult` |
| `worker_copy.go` | Folder/file copy → `ReportTaskResult` |
| `worker/interface.go` | `Worker` interface |
| `task.go` | `TaskBase`, task types, `ChildResult` |
| `seeding.go` | Root seeding helpers used with `pkg/db` |
| `coordinator.go` | `QueueCoordinator`, `CanDstStartRound`, `CanSrcStartRound`, `SetMaxSrcAhead` |
| `observer.go` | Polls stats / queues, writes `queue_stats` |
| `queue_watchdog.go` / `progress_watchdog.go` | Timeouts / progress |

---

## Modes

- **`QueueModeTraversal`** – Normal BFS; pull pending traversal tasks by depth.
- **`QueueModeRetry`** – Pending/failed across depths; SRC folder success triggers DST child cleanup (see `mode_retry.go`).
- **`QueueModeCopy`** / **`QueueModeCopyRetry`** – Copy phase and failed-only retry; pulls by copy status and node type.

---

## Relationship with `pkg/db`

- **Reads:** Keyset lists, joins (`ListDstBatchWithSrcChildren`), stats helpers for completion / observer.
- **Writes:** **`SealLevel`** (bulk node append + per-depth stats snapshot), plus transactional writers for status events, review, deletes, etc. (`RunWrite` / `Writer`).

See **`pkg/db/README.md`** for schema (**`src_status_events`**, **`dst_status_events`**, node tables, `stats`, `migrations`, …).

---

## Resumption

Resume uses the same DuckDB file: `initializeQueues` in `pkg/migration/run.go` restores rounds and cursors from DB state so pulls continue from the correct frontier.

---

## Summary

- **DB-backed pulls** into `pendingBuff`; **seal** persists levels/stats.
- **Coordinator** enforces DST lag and SRC-ahead cap (**default 2**, configurable).
- **Modes:** traversal, retry, copy, copy-retry.
