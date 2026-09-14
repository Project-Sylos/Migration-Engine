# Queue Package

The queue layer drives **source** and **destination** traversal and **copy** using **`pkg/db`**. Two queues (`src` and `dst`, plus `copy` in copy mode) perform breadth-first work in **rounds** (depth). The destination is gated by **`QueueCoordinator`**.  

**Frontier today is database-backed:** pending work is **pulled from DuckDB** in batches (~10k rows, ID keyset pagination) into an in-memory **`pendingBuff`**; workers **lease** tasks from that buffer. Completed work is flushed via **`db.SealLevel`** (optionally through **`SealBuffer`** for async seal). There is **no** separate `NodeCache` / `LevelCache` source file—the model is “DB + pending buffer + seal,” not a pure memory-first level cache.

---

## High-level flow

1. **Pull** – `PullTraversalTasks` / `PullRetryTasks` / `PullCopyTasks` refill `pendingBuff` from SQL (`ListNodesByDepthKeyset`, `ListDstBatchWithSrcChildren`, copy keysets, etc.). Only one pull runs at a time (`getPulling` / `setPulling`). Pull watermarks use **pendingBuff length only** (in-progress leases are not treated as available depth). When workers are idle and pending is below the live pool size, a **starve nudge** raises the effective WM so underfed pools refill sooner.
2. **Lease** – Workers take tasks from `pendingBuff` into `inProgress`. Multi-task batch turns (folder create, file upload, delete) use **`LeaseGroupBudget`**: live AIMD pool size (`len(pool.handles)`) drives a fair **count ceiling** (`ceil(pending/workers)`) and, for files, a soft **byte budget** (`sum(pending sizes)/workers` with one-item overflow). This prevents one worker from hoovering the whole adapter max batch.
3. **Complete** – `ReportTaskResult` updates state and enqueues **seal** work (nodes + per-depth stats) through the DB layer.
4. **Coordinator** – DST may start round *N* only when SRC has completed rounds *N* and *N+1* (or SRC traversal is done). SRC is not round-gated against DST; work is pulled and sealed to Badger in batches, so SRC can advance as fast as workers allow.

### Seal hot path (trusted batch)

Queue completions enqueue discovery nodes and status events into **`badger_seal`**, which flushes via **`WriteSealBatchTrusted`** without Badger existence probes. Callers supply **`InsertOnly`**, **`PendWasSet`**, and **`PrevStatus`** from task context. SRC folder completes after child discovery enqueue an authoritative **`kids:src:{parent}`** replace via **`AppendKidsPackReplace`** (not per-child merge during node writes).

Bulk subtree mutations (`PropagateCopyFailureUnderPath`, `CascadeDeleteUnderPath`) use the same trusted batch with **`PrevStatus`** from subtree scan chunks.

### Transfer checkpoint / stop-resume (file copy)

Mid-transfer progress is stored on **`src_nodes`** (`xfer_offset`, `xfer_src_size`, `xfer_src_mtime`, `xfer_dst_ref`, `xfer_resume_token`); **`copy_status` stays `pending`**. `xfer_dst_ref` is the **attempt marker** (written at `OpenWrite`, cleared only on successful copy). `xfer_resume_token` holds an opaque provider resume handle (e.g. Graph `uploadUrl`).

Resume/attempt state is consulted **before** any DST existence precheck. When the destination’s **`FSTransferRestartPolicy`** supports resumable transfer and the fingerprint matches, the next worker resumes (Graph: `OpenWriteFromResumeToken` with authoritative `nextExpectedRanges`). Otherwise, if an attempt marker exists, ME deletes that DST ref and restarts. Blind “exists → already_exists” is unreachable for a task this migration already attempted.

On cooperative abandon (scale-down force-checkout), writers prefer **`Suspend()`** (no finalize) over `Close()` when available. Only the retiring worker’s lease is checkpointed and requeued; there is no bulk `ReleaseInFlightOnThrottle`.

Copy uses a small buffer loop (`OpenRead` → `Write` → … → `Close`). Destination **`OpenWrite` must stream** (fragments/parts during `Write`, or a pipe upload goroutine). ME prefers **`OpenWriteWithSize`** when the adapter implements it so providers that need a declared length (Box sessions) get `file.Size`. Each successful write chunk beats the progress + queue watchdogs so long uploads do not false-stall.

### Spin-down grace (15s)

On scale-down, idle workers are retired immediately (`retire` + cancel). Remaining owed retirements prefer the **smallest active leases**: those workers get `retire` set (no new leases) but keep their context through a **15s provisional freeze**. The first deferred retiree to go idle is cancelled and exits cleanly; on timeout only the deferred set is force-checked out (smallest-first), not every busy worker. Soft-suspend still force-checkouts all busy leases and uses DB-only abandon.

---

## Package map

Same pyramid as `pkg/db`: **children import `queue`; root never imports children**. Migration blank-imports `mode` / `worker` / `observe` so `init()` registers hooks. `gpl` is a leaf helper package (no hooks).

| Package | Role |
|---------|------|
| **`pkg/queue`** (root) | `Queue`, coordinator, task types, pull/lease/seal surface, scaling pool, rate-limit / grace / soft-suspend, stall dump, hook registries |
| **`pkg/queue/mode`** | Mode pull + completion (`PullTraversalTasks`, copy/delete/GPL/retry) |
| **`pkg/queue/worker`** | Worker loops (traversal / copy / delete / batch) + copy path helpers |
| **`pkg/queue/observe`** | `QueueObserver`, stall watchdog, `ProgressWatchdog` |
| **`pkg/queue/gpl`** | Path-linter eval helpers used by mode, worker, and migration sweeps |

Wiring: `RegisterModeHooks` / `RegisterWorkerHooks` / `RegisterObserveHooks`. `pkg/queue` does **not** import `pkg/scaling`; `*Queue` satisfies `scaling.QueueActuator` from migration.

## Core components (root)

| File | Responsibility |
|------|----------------|
| `queue.go` | `Queue`, `Run`, round advancement, seal handoff, `Lease`, `ReportTaskResult`, `InitializeWithContext` |
| `queue_accessors.go` | Thread-safe getters/setters, keyset cursors, traversal “cache loaded” flags |
| `queue_scaling.go` | **`ScalingContext`**, dynamic worker pool, `SetTargetWorkerCount`, lease/refill/list-page knobs for autoscaler |
| `queue_batch.go` | `BuildExpectedMapsFromDstWithChildren`, batch expected children for DST |
| `queue_lease_batch.go` | Lease batch sizing helpers |
| `list_fill.go` / `pull_result.go` | List-fill and pull result plumbing |
| `transfer_checkpoint.go` / `copy_work_finalize.go` | Mid-transfer checkpoint + copy finalize |
| `dst_match_key.go` | DST child match key / name |
| `mode_hooks.go` / `worker_hooks.go` / `observe_hooks.go` | Hook registries for child packages |
| `task.go` | `TaskBase`, task types, `ChildResult` |
| `seeding.go` | Root seeding helpers used with `pkg/db` |
| `coordinator.go` | `QueueCoordinator`, `CanDstStartRound`, round tracking for SRC/DST |
| `stall_dump.go` | Stall / completion dump helpers used by observe |
| `queue_rate_limit.go` | FS throttle tracking surfaced to observer/autoscaler |
| `queue_grace.go` | Spin-down grace (15s) before force checkout on scale-down |
| `queue_soft_suspend.go` | Pause-side helpers: stop watchdog, clear pending buffer, wait for in-flight zero |
| `queue_force_stop.go` | Force-stop / abandon helpers |

---

## Modes

- **`QueueModeTraversal`** – Normal BFS; pull pending traversal tasks by depth.
- **`QueueModeRetry`** – Pending/failed across depths; SRC folder success triggers DST child cleanup (see `mode/mode_retry.go`).
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

First start / root-prep uses `initializeQueues` in `pkg/migration/run.go` (seed roots, starting rounds). Soft-stop **Resume** does not go through that path: it reloads **`runtime_state_json.suspend_v1`** (last rounds plus src/dst/copy keyset cursors) and continues the same queue mode. Retry sweep is separate and still starts at round 0.

**Soft suspend** (`Pause`, clear **`pendingBuff`**, drain **`inProgress`**, flush/checkpoint at the migration layer) does **not** persist leased or pending task IDs; after **`traversal-suspended`** / **`copy-suspended`**, **`pkg/migration`** restores **`suspend_v1`** (worker count, retries, optional **`QueueSizing`** lease/refill batches, observer/progress tick hints, max depth, last rounds, keyset cursors) and continues pulls from DuckDB.

**`QueueSizing`** (optional last argument to **`NewQueue`**) overrides default lease and traversal refill batch sizes so suspend/resume can reproduce the same pull behavior.

### DST traversal pull quotas

DST traversal (and DST retry) pulls hydrate **expected SRC children** per folder. To cap RSS, `PullTraversalTasks` / `PullRetryTasks` use dual limits via `pull.ListDstBatchWithSrcChildrenQuota`:

- **Task quota:** `EffectiveRefillBatchSize()` (traversal) or `EffectiveLeaseBatchSize()` (retry). DST traversal defaults to **1000** folders locally (SRC stays at profile default).
- **Child quota:** `taskQuota * EffectiveDstPullChildMultiplier()` (default multiplier **10**, so ~10k children at default task quota).

Whichever binds first stops the pull; the keyset cursor advances only through **enqueued** folders. Under memory pressure the autoscaler halves `RefillBatchSize` / `LeaseBatchSize` and, for the `dst` queue, `DstPullChildMultiplier` (min 2).

The **queue watchdog** treats **`QueueStatePaused`** as non-stall (no dump spam during intentional suspend).

---

## Summary

- **DB-backed pulls** into `pendingBuff`; **seal** persists levels/stats.
- **Coordinator** enforces DST lag behind SRC (two-round lead); SRC has no coordinator throttle.
- **Modes:** traversal, retry, copy, copy-retry, delete, delete-retry.
