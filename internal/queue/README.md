# Task Queue System

The **Task Queue System** orchestrates parallel traversal and migration workloads within Sylos.
It provides fault-tolerant leasing, retry management, and coordinated round control for both **source (src)** and **destination (dst)** traversal processes.

---

## Overview

Traversal in Sylos is breadth-first (BFS), performed in **rounds** that correspond to depth levels in each node tree.
Each queue represents a pipeline of discrete, stateless work units (**tasks**) executed by worker goroutines.
Two queues run concurrently:

* **Source Queue (src)** – Drives discovery; lists children from the source file system.
* **Destination Queue (dst)** – Validates or creates destination nodes, always operating at least one round behind the source.

This design is *semi-coupled*:
the destination’s progression depends on the source’s completion of the previous round, but the source can advance freely.

---

## Core Components

| File             | Purpose                                                                                           |
| ---------------- | ------------------------------------------------------------------------------------------------- |
| `task.go`        | Defines base task structures and specialized types (e.g. traversal, upload).                      |
| `queue.go`       | Maintains pending/in-progress tasks, manages workers, pulls tasks from DB, and buffers results.   |
| `worker.go`      | Executes tasks concurrently using shared filesystem adapters; reports results.                    |
| `coordinator.go` | Manages queue lifecycles, monitors queue states, and coordinates round advancement.               |

---

## Task Model

Every unit of work derives from `TaskBase`:

```go
type TaskBase struct {
    Type               string          // "src-traversal", "dst-traversal", "upload", etc.
    Folder             fsservices.Folder
    File               fsservices.File
    Locked             bool
    Attempts           int
    Status             string          // "successful" or "failed"
    DiscoveredChildren []ChildResult   // Children found during traversal
    ExpectedFolders    []Folder        // Expected child folders (for dst comparison)
    ExpectedFiles      []File          // Expected child files (for dst comparison)
}
```

Workers lease tasks from the queue, mark them `Locked`, and attempt execution.
On success or final failure, tasks are added to the queue's buffer with their execution results.
Workers populate `DiscoveredChildren` during traversal, and the queue handles batch database writes during flush.

Tasks are identified by **absolute paths** (`Id`) but reconciled logically by **root-relative paths** (`LocationPath`) shared between `src_nodes` and `dst_nodes` tables.

---

## Leasing and Retry Behavior

* Tasks are **leased** when a worker begins execution and remain locked until the worker reports completion.
* Failed tasks are retried up to a configurable limit (default: 3 attempts).
* Leased task identifiers remain in the queue’s `TrackedIDs` list until completion, preventing re-querying of in-flight work.
* This allows the coordinator to fetch new tasks using lightweight SQL such as
  `WHERE path NOT IN (tracked_ids)`.

---

## Round Coordination

Traversal proceeds in layers of depth:

```
src round 0  → dst round 0
src round 1  → dst waits
src round 2  → dst round 1
...
```

* The **source queue** advances immediately when its current round completes.
* The **destination queue** begins a round only after the source has fully completed (and flushed) the previous one.
* When the source finishes all rounds, the destination runs freely to completion.

### Queue State Tracking

Each queue maintains a `QueueState` attribute to distinguish between different operational phases:

* **`NeedsPull`** – Round just advanced; needs to query DB for tasks
* **`Running`** – Has active tasks; currently processing
* **`RoundComplete`** – All tasks in current round finished; ready to advance
* **`Exhausted`** – No tasks found in DB; traversal complete

This state tracking prevents premature round advancement and enables the coordinator to make intelligent decisions about when to progress through BFS layers.

---

## Buffering and Checkpointing

Each queue owns its own **`db.Buffer`** instance for batching database operations.
The buffer system provides:

* **Multi-channel batching** – Queues register sub-buffers for different operation types (e.g., "completed" tasks).
* **Automatic flushing** – Buffers flush when item threshold is reached or when the flush timer expires.
* **Clean separation** – Workers just report results; the buffer handles all batch writes asynchronously.

### Queue Buffer Operations

When a task completes (successfully or after max retries), the queue adds it to the buffer via `buffer.Add("completed", task)`.
The buffer automatically batches completed tasks and flushes them using a custom `FlushFunc` that:

1. Groups parent tasks by status (`successful` vs `failed`)
2. Performs batch `UPDATE` operations for parent traversal statuses
3. Performs batch `INSERT` operations for all discovered children

This design maximizes write efficiency while keeping the queue logic clean and maintainable.

### Round Boundaries

Queue state transitions (`NeedsPull` → `Running` → `RoundComplete`) enable the coordinator to advance rounds deterministically.
When a queue stops, its buffer performs a final flush, ensuring every BFS layer boundary corresponds to a durable state in DuckDB.
If the system restarts, it can resume traversal from the last completed round.

---

## Concurrency Model

* Each queue maintains its own worker pool.
* Workers share common `fsservices.FS` adapters for efficient rate-limiting and caching.
* Coordinators may scale worker count dynamically based on queue depth or throughput metrics.
* Logging is fully asynchronous via the `logservice.Sender`, which mirrors messages to UDP and DuckDB.

---

## Future Extensions

The queue framework is designed to generalize beyond traversal:

* **Upload / Copy Tasks** will reuse the same leasing and retry logic.
* **Dynamic Scaling Policies** may adjust worker counts automatically.
* **Pluggable Backends** (e.g., cloud storage adapters) can slot into the same worker interface.

---

The queue package forms the backbone of Sylos’ concurrency model:
a deterministic, recoverable, high-throughput engine for orchestrating two independent BFS traversals in a single migration pipeline.
