# Queue System

The **Queue System** orchestrates parallel traversal and migration workloads within Sylos. It provides a self-managing, fault-tolerant task execution framework with automatic retry logic, coordinated round progression, and autonomous worker management.

---

## Overview

Traversal in Sylos follows a **breadth-first search (BFS)** pattern, performed in **rounds** that correspond to depth levels in the filesystem tree. The system runs two queues concurrently:

* **Source Queue (src)** – Discovers and lists children from the source filesystem
* **Destination Queue (dst)** – Validates destination nodes by comparing against source, always staying at least one round behind

The queues are **self-managing**: they create their own workers, pull tasks from the database when needed, coordinate round advancement, and handle their complete lifecycle without external orchestration.

---

## Architecture

### Self-Managing Design

The queue system uses a **direct instantiation** pattern with no coordinator:

```go
// Create source queue - workers start immediately
srcQueue := queue.NewQueue("src", maxRetries, batchSize, workerCount)
srcQueue.Initialize(database, "src_nodes", srcAdapter, nil, nil)

// Create destination queue - workers start immediately, coordinated with src
dstQueue := queue.NewQueue("dst", maxRetries, batchSize, workerCount)
dstQueue.Initialize(database, "dst_nodes", dstAdapter, srcContext, srcQueue)

// Wait for completion - queues manage themselves
for !(srcQueue.IsExhausted() && dstQueue.IsExhausted()) {
    srcStats := srcQueue.Stats()
    dstStats := dstQueue.Stats()
    fmt.Printf("Src: Round %d (P:%d IP:%d) | Dst: Round %d (P:%d IP:%d)\n",
        srcStats.Round, srcStats.Pending, srcStats.InProgress,
        dstStats.Round, dstStats.Pending, dstStats.InProgress)
    time.Sleep(500 * time.Millisecond)
}
```

**Key Benefits:**
1. No coordinator or setup wrapper needed
2. Queues are created and immediately operational
3. Workers start autonomously and manage their own lifecycle
4. Direct access to queue state and operations

---

## Core Components

| File          | Purpose                                                                                |
| ------------- | -------------------------------------------------------------------------------------- |
| `queue.go`    | Self-managing queue with task leasing, event-driven DB pulling, and round coordination |
| `worker.go`   | Autonomous workers that poll their queue, execute tasks, and write results to DB       |
| `task.go`     | Task definitions and specialized types (traversal, upload, copy)                       |
| `seeding.go`  | Initial task seeding functions for starting traversal                                  |

---

## Task Model

Every unit of work is represented by `TaskBase`:

```go
type TaskBase struct {
    Type               TaskType         // TaskTypeSrcTraversal or TaskTypeDstTraversal
    Folder             fsservices.Folder
    File               fsservices.File
    Locked             bool
    Attempts           int
    Status             string           // "successful" or "failed"
    DiscoveredChildren []ChildResult    // Children found during traversal
    ExpectedFolders    []Folder         // Expected child folders (dst comparison)
    ExpectedFiles      []File           // Expected child files (dst comparison)
}
```

**Task Lifecycle:**
1. Task is pulled from DB and added to queue's pending list
2. Worker calls `queue.Lease()` - task moves to in-progress
3. Worker executes task (lists children via filesystem adapter)
4. Worker writes results to DB:
   - Updates parent status to "Successful"
   - Batch inserts all discovered children
5. Worker calls `queue.Complete(task)` - task removed from tracking
6. If round is empty, queue auto-advances to next round

---

## Queue Lifecycle States

Each queue maintains a `QueueState` to track its operational phase:

| State           | Description                                                |
| --------------- | ---------------------------------------------------------- |
| `Running`       | Queue is active and processing tasks                       |
| `Paused`        | Queue is paused; workers will not lease new tasks          |
| `Stopped`       | Queue is stopped (reserved for explicit shutdown)          |
| `Completed`     | Traversal complete (max depth reached, no more work)       |

**State Transitions:**
```
Running → Paused (manual pause)
Paused → Running (manual resume)
Running → Completed (no tasks found after round advance)
```

---

## Event-Driven Task Pulling

Tasks are pulled from the database only when necessary, not on a continuous polling schedule:

### Pull Triggers
1. **Queue Initialization** – Initial pull when queue is created (src only; dst waits)
2. **Running Low** – When `pending + inProgress < batchSize` during task leasing
3. **Round Advancement** – After completing a round and advancing to the next

### Concurrency Control
- A `pulling` flag prevents concurrent DB queries
- If a pull is already in progress, subsequent triggers are ignored
- This ensures efficient DB usage without redundant queries

---

## Round Coordination

Traversal proceeds in synchronized depth layers:

```
src round 0  → dst round 0
src round 1  → dst waits...
src round 2  → dst round 1 (after src finishes round 1)
...
```

### Source Queue
- Advances immediately when current round completes (no pending, no in-progress)
- Pulls tasks for new round from DB
- If no tasks found, marks queue as `Completed`

### Destination Queue
- **Waits for source** before advancing each round
- Uses `waitForSrcRound()` to block until `srcQueue.Round() > dstQueue.Round()`
- This ensures dst always has the expected children from src to compare against

### Completion Logic
After advancing rounds, if no tasks are found:
```go
func (q *Queue) checkForCompletion(round int) {
    if q.state == QueueStateRunning {
        q.state = QueueStateCompleted
        // Max depth reached - traversal complete
    }
}
```

This ensures at least one round of work happens before declaring completion.

---

## Workers

### Autonomous Workers

Workers are created during `queue.Initialize()` and run independently:

```go
func (w *TraversalWorker) Run() {
    for {
        if w.queue.IsPaused() {
            time.Sleep(100 * time.Millisecond)
            continue
        }
        
        if w.queue.IsExhausted() {
            return // Worker exits when queue completes
        }
        
        task := w.queue.Lease()
        if task == nil {
            time.Sleep(50 * time.Millisecond)
            continue
        }
        
        // Execute and write results
        err := w.execute(task)
        if err != nil {
            w.queue.Fail(task) // Retry or mark failed
        } else {
            w.writeResultsToDB(task) // Immediate DB writes
            w.queue.Complete(task)
        }
    }
}
```

**Worker Characteristics:**
- Poll their queue continuously
- Respect queue lifecycle (pause/complete states)
- Exit gracefully when queue is exhausted
- No external start/stop management needed

### Database Writes

Workers perform **immediate database operations** after task execution:

1. **Update Parent Status**:
   ```sql
   UPDATE src_nodes SET traversal_status = 'Successful' WHERE id = ?
   ```

2. **Batch Insert Children**:
   ```sql
   INSERT INTO src_nodes VALUES (?, ?, ...), (?, ?, ...), ...
   ```

This direct write pattern ensures:
- No buffering delays
- Immediate visibility of results
- Simple error handling
- Clear causality (task → DB write → completion)

---

## Leasing and Retry Behavior

### Task Leasing
- Workers call `queue.Lease()` to obtain work
- Task moves from `pending` to `inProgress`
- Task's `Locked` flag is set to `true`
- Identifier is added to `trackedIDs` to prevent re-querying

### Retry Logic
Failed tasks are automatically retried:
```go
func (q *Queue) Fail(task *TaskBase) bool {
    task.Attempts++
    if task.Attempts < q.maxRetries {
        q.pending = append(q.pending, task) // Re-queue
        return true // Will retry
    }
    // Max retries exceeded - task permanently failed
    return false
}
```

### Tracked IDs
The `trackedIDs` map prevents pulling tasks that are already in-flight:
```sql
SELECT ... FROM src_nodes
WHERE traversal_status = 'Pending'
  AND depth_level = ?
  AND id NOT IN ('tracked_id_1', 'tracked_id_2', ...)
LIMIT ?
```

---

## Task Types

### Source Traversal (`TaskTypeSrcTraversal`)
- Lists children of a folder from the source filesystem
- All discovered children get status:
  - Folders: `"Pending"` (need traversal)
  - Files: `"Successful"` (no traversal needed)

### Destination Traversal (`TaskTypeDstTraversal`)
- Lists children from destination filesystem
- Compares against expected children from source
- Assigns comparison status:
  - `"Pending"` – Folder exists on both src and dst
  - `"Successful"` – File exists on both src and dst
  - `"Missing"` – Item exists on src but not dst
  - `"NotOnSrc"` – Item exists on dst but not src

---

## Concurrency Model

- Each queue owns its own worker pool (configured at creation)
- Workers share filesystem adapters for efficient rate-limiting
- All logging is asynchronous via `logservice.Sender` (UDP + DuckDB buffer)
- Database writes are immediate but use batch inserts for children

**Thread Safety:**
- Queue operations protected by `sync.RWMutex`
- Task leasing is atomic
- State transitions are synchronized
- No data races between workers

---

## Future Extensions

The queue framework is designed to generalize beyond traversal:

* **Upload/Copy Tasks** – Will reuse the same leasing and retry logic
* **Dynamic Worker Scaling** – Adjust worker count based on queue depth
* **Pluggable Adapters** – Cloud storage backends (S3, Azure, GCS)
* **Priority Queues** – High-priority tasks for critical files
* **Progress Checkpointing** – Resume from specific round after crash

---

## Summary

The queue system provides:
- ✅ **Self-managing** – No coordinator needed, queues own their lifecycle
- ✅ **Autonomous workers** – Poll continuously, respect state, exit gracefully
- ✅ **Event-driven pulling** – Tasks pulled only when needed
- ✅ **Coordinated rounds** – DST automatically waits for SRC
- ✅ **Immediate DB writes** – No buffering delays, clear causality
- ✅ **Fault tolerance** – Automatic retries, graceful failure handling
- ✅ **Simple API** – Direct queue instantiation and interaction

This design forms the backbone of Sylos' concurrency model: a deterministic, recoverable, high-throughput engine for orchestrating parallel BFS traversals in a single migration pipeline.
