# Queue Package

The queue layer drives **source** and **destination** traversal and **copy** using **`pkg/db`**. Two queues (`src` and `dst`, plus `copy` in copy mode) perform breadth-first work in **rounds** (depth). The destination is gated by **`QueueCoordinator`**.  

**Frontier today is database-backed:** pending work is **pulled from DuckDB** in batches (~10k rows, ID keyset pagination) into an in-memory **`pendingBuff`**; workers **lease** tasks from that buffer. Completed work is flushed via **`db.SealLevel`** (optionally through **`SealBuffer`** for async seal). There is **no** separate `NodeCache` / `LevelCache` source file—the model is “DB + pending buffer + seal,” not a pure memory-first level cache.

---

## High-level flow

1. **Pull** – `PullTraversalTasks` / `PullRetryTasks` / `PullCopyTasks` refill `pendingBuff` from SQL (`ListNodesByDepthKeyset`, `ListDstBatchWithSrcChildren`, copy keysets, etc.). Only one pull runs at a time (`getPulling` / `setPulling`). Pull watermarks use **pendingBuff length only** (in-progress leases are not treated as available depth). When workers are idle and pending is below the live pool size, a **starve nudge** raises the effective WM so underfed pools refill sooner.
2. **Lease** – Workers take tasks from `pendingBuff` into `inProgress`. Multi-task batch turns (folder create, file upload, delete) use **`LeaseGroupBudget`**: live AIMD pool size (`len(pool.handles)`) drives a fair **count ceiling** (`ceil(pending/workers)`) and, for files, a soft **byte budget** (`sum(pending sizes)/workers` with one-item overflow). This prevents one worker from hoovering the whole adapter max batch.
3. **Complete** – `ReportTaskResult` updates state and enqueues **seal** work (nodes + per-depth stats) through the DB layer.
4. **Coordinator** – DST may start round *N* only when SRC has completed rounds *N* and *N+1* (or SRC traversal is done). SRC is not round-gated against DST; work is pulled and sealed to DuckDB in batches, so SRC can advance as fast as workers allow.

### Transfer checkpoint / stop-resume (file copy)

Mid-transfer progress is stored on **`src_nodes`** (`xfer_offset`, `xfer_src_size`, `xfer_src_mtime`, `xfer_dst_ref`); **`copy_status` stays `pending`**. Sessions are never handed off: the retiring worker closes its live FS session, persists offset + fingerprint, then either **requeues** to `pendingBuff` (autoscaler scale-down) or leaves **DB-only** (stop / soft-suspend). The next worker opens a **fresh** session and seeks SRC when the destination adapter’s **`FSTransferRestartPolicy`** supports resumable transfer; otherwise ME applies delete-if-required and full restart.

### Spin-down grace (15s)

On scale-down with busy workers, the queue enters a **provisional freeze**: `SetTargetWorkerCount` no-ops (scale up and down) while AIMD rate-limit counters keep accumulating. After **15s**, all tracked busy **FS leases** are force-checked out (files smallest-first by byte size; non-file work at size 0). File transfers cooperatively checkpoint + requeue; other ops abort via canceled `workerCtx` (batches/list/delete prefer that parent). Soft-suspend uses the same grace, then cancels busy worker contexts and uses DB-only abandon.

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
| `worker_copy.go` | Folder/file copy → `ReportTaskResult`; optional dst list precheck when resuming partial copy (one anchor round only) |
| `worker/interface.go` | `Worker` interface |
| `task.go` | `TaskBase`, task types, `ChildResult` |
| `seeding.go` | Root seeding helpers used with `pkg/db` |
| `coordinator.go` | `QueueCoordinator`, `CanDstStartRound`, round tracking for SRC/DST |
| `observer.go` | Polls stats / queues, writes `queue_stats` |
| `queue_watchdog.go` / `progress_watchdog.go` | Timeouts / progress |
| `queue_soft_suspend.go` | Pause-side helpers: stop watchdog, clear pending buffer, wait for in-flight zero |

---

## Modes

- **`QueueModeTraversal`** – Normal BFS; pull pending traversal tasks by depth.
- **`QueueModeRetry`** – Pending/failed across depths; SRC folder success triggers DST child cleanup (see `mode_retry.go`).
- **`QueueModeCopy`** / **`QueueModeCopyRetry`** – Copy phase and failed-only retry; pulls by copy status and node type.
- **`QueueModeDelete`** / **`QueueModeDeleteRetry`** – Reverse-BFS delete; autoscaler uses the **dst** provider’s `delete` operation profile (falls back to that provider’s Default worker count when unset).

Autoscaler worker Min/Default/Max are resolved **per FS operation** from `pkg/scaling/operation_profile.go` (list vs create vs delete vs transfer). Providers that do not override an op use a single Default for everything; Dropbox/Drive set tighter list/create/delete caps.

---

## Relationship with `pkg/db`

- **Reads:** Keyset lists, joins (`ListDstBatchWithSrcChildren`), stats helpers for completion / observer.
- **Writes:** **`SealLevel`** (bulk node append + per-depth stats snapshot), plus transactional writers for status events, review, deletes, etc. (`RunWrite` / `Writer`).

See **`pkg/db/README.md`** for schema (**`src_status_events`**, **`dst_status_events`**, node tables, `stats`, `migrations`, …).

---

## Resumption

Resume uses the same DuckDB file: `initializeQueues` in `pkg/migration/run.go` restores rounds and cursors from DB state so pulls continue from the correct frontier.

**Soft suspend** (`Pause`, clear **`pendingBuff`**, drain **`inProgress`**, flush/checkpoint at the migration layer) does **not** persist leased or pending task IDs; after **`traversal-suspended`** / **`copy-suspended`**, **`pkg/migration`** restarts with persisted **`suspend_v1`** (worker count, retries, optional **`QueueSizing`** lease/refill batches, observer/progress tick hints, max depth, last rounds) and rebuilds work from DuckDB.

**`QueueSizing`** (optional last argument to **`NewQueue`**) overrides default lease and traversal refill batch sizes so suspend/resume can reproduce the same pull behavior.

The **queue watchdog** treats **`QueueStatePaused`** as non-stall (no dump spam during intentional suspend).

---

## Summary

- **DB-backed pulls** into `pendingBuff`; **seal** persists levels/stats.
- **Coordinator** enforces DST lag behind SRC (two-round lead); SRC has no coordinator throttle.
- **Modes:** traversal, retry, copy, copy-retry.
