# Queue System

The **Queue System** orchestrates parallel traversal and migration workloads within Sylos. It provides a self-managing, fault-tolerant task execution framework with automatic retry logic, coordinated round progression, and autonomous worker management.

---

## Overview

Traversal in Sylos follows a **breadth-first search (BFS)** pattern, performed in **rounds** that correspond to depth levels in the filesystem tree. The system runs two queues concurrently:

* **Source Queue (src)** – Discovers and lists children from the source filesystem
* **Destination Queue (dst)** – Validates destination nodes by comparing against source, always staying at least 3 rounds behind (via coordinator)

The queues are **self-managing**: they create their own workers, manage tasks in-memory via the RoundQueue system, coordinate round advancement, and handle their complete lifecycle without external orchestration.

---

## Architecture

### Self-Managing Design with RoundQueue System

The queue system uses a **coordinated instantiation** pattern with a shared `QueueCoordinator` and round-scoped in-memory queues:

```go
// Create shared coordinator
maxLead := 4  // Must be at least 3 (DST needs SRC to be 3 rounds ahead)
coordinator := queue.NewQueueCoordinator(maxLead)

// Create source queue - workers start immediately
srcQueue := queue.NewQueue("src", maxRetries, workerCount, coordinator)
srcQueue.Initialize(database, "src_nodes", srcAdapter, nil)

// Create destination queue - workers start immediately, coordinated with src
dstQueue := queue.NewQueue("dst", maxRetries, workerCount, coordinator)
dstQueue.Initialize(database, "dst_nodes", dstAdapter, srcQueue)

// Seed initial root tasks directly into queues
// (Database seeding should be done separately before starting queues)
srcRootTask := &queue.TaskBase{
    Type:   queue.TaskTypeSrcTraversal,
    Folder: srcRootFolder,
    Round:  0,
}
srcQueue.Add(srcRootTask)

// Wait for SRC round 0 to complete, then seed DST root
// ... wait logic ...
srcRootCompleted := srcQueue.GetSuccessfulTask(0, "/")
dstRootTask := &queue.TaskBase{
    Type:            queue.TaskTypeDstTraversal,
    Folder:          dstRootFolder,
    ExpectedFolders: extractFolders(srcRootCompleted.DiscoveredChildren),
    ExpectedFiles:   extractFiles(srcRootCompleted.DiscoveredChildren),
    Round:           0,
}
dstQueue.Add(dstRootTask)

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
1. Explicit round coordination via `QueueCoordinator` - eliminates timing dependence
2. RoundQueue system provides in-memory task propagation between queues
3. Queues are created and immediately operational
4. Workers start autonomously and manage their own lifecycle
5. Direct access to queue state and operations
6. Database is write-ahead log only - task flow happens in memory

---

## Core Components

| File             | Purpose                                                                                |
| ---------------- | -------------------------------------------------------------------------------------- |
| `queue.go`       | Self-managing queue with task leasing, round coordination, and RoundQueue management  |
| `round_queue.go` | Round-scoped in-memory queues (pending/successful sub-queues per round)               |
| `worker.go`      | Autonomous workers that poll their queue, execute tasks, and write results to DB       |
| `task.go`        | Task definitions and specialized types (traversal, upload, copy)                       |
| `seeding.go`     | Initial task seeding functions for database initialization                             |
| `coordinator.go` | Round synchronization between src and dst queues                                       |

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
    Round              int              // The round this task belongs to (for RoundQueue coordination)
}
```

**Task Lifecycle:**
1. Task is added to queue's round 0 (initial seeding) or propagated from previous round
   - Task's `Round` field is set when created
2. Worker calls `queue.Lease()` - task moves from `pending` to `inProgress`
3. Worker executes task (lists children via filesystem adapter)
4. Worker writes results to DB immediately:
   - Updates parent status to "Successful"
   - Batch inserts all discovered children
5. Worker calls `queue.Complete(task)` - task moves to `successful` sub-queue
6. Queue propagates children to next round:
   - SRC: Creates new tasks for discovered folders, adds to next round's `pending`
   - DST: Retrieves corresponding SRC task, creates DST tasks with expected children
7. If round is empty, queue auto-advances to next round

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

## RoundQueue System

Tasks are managed in round-scoped in-memory queues. Each queue maintains a `map[int]*RoundQueue` where the key is the round number:

### RoundQueue Structure

Each `RoundQueue` contains:
- **`pending []*TaskBase`** – Tasks waiting to be leased by workers
- **`successful []*TaskBase`** – Tasks that completed successfully (used for cross-queue propagation)

**Note**: `RoundQueue` has no internal mutex. All operations must be called while holding `Queue.mu`. This eliminates nested locks and simplifies the locking model.

### Task Propagation

- **SRC Queue**: When a task completes, discovered folder children are added to the next round's `pending` sub-queue
- **DST Queue**: When a task completes, it retrieves the corresponding SRC task from the same round's `successful` sub-queue to get expected children, then creates new DST tasks for the next round
- **Memory Management**: When DST advances rounds, it cleans up both its own and SRC's corresponding completed round queues

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
- Tasks are propagated in-memory to next round via RoundQueue system
- If no tasks found in new round, marks queue as `Completed`

### Destination Queue
- **Waits for source** before advancing each round
- Uses `QueueCoordinator.CanDstAdvance()` to ensure src is at least 3 rounds ahead (or src is completed)
- When DST is on round N, it can only advance to round N+1 if SRC is at least on round N+3
- This ensures that when DST completes round N and queries SRC tasks from round N+1, those tasks are guaranteed to exist (SRC round N+1 completed when SRC advanced to round N+2)
- Retrieves corresponding SRC tasks from `srcQueue.GetSuccessfulTask(round, path)` when creating next round tasks
- This ensures dst always has deterministic visibility of src children to compare against

### Completion Logic
After completing tasks, the queue checks if the round is complete using `checkRoundComplete()`:
```go
func (q *Queue) checkRoundComplete(currentRound int) bool {
    // Thread-safe check: acquires RLock, checks all conditions atomically
    // Returns true if round is complete (no pending, no in-progress)
}
```

If a round is complete, the queue advances to the next round. If no tasks are found in the new round, the queue is marked as `Completed` and the coordinator is notified.

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

The database serves as a write-ahead log for persistence and logging. Task propagation happens entirely through in-memory RoundQueues, ensuring:
- No buffering delays - DB writes are immediate
- Deterministic visibility via RoundQueue system eliminates timing issues
- Simple error handling - DB writes are independent of task propagation
- Clear causality (task → DB write → queue propagation → completion)

---

## Leasing and Retry Behavior

### Task Leasing
- Workers call `queue.Lease()` to obtain work
- Task moves from current round's `pending` sub-queue to `inProgress`
- Task's `Locked` flag is set to `true`

### Retry Logic
Failed tasks are automatically retried:
```go
func (q *Queue) Fail(task *TaskBase) bool {
    task.Attempts++
    if task.Attempts < q.maxRetries {
        // Re-queue to same round's pending sub-queue
        currentRoundQueue.AddPending(task)
        return true // Will retry
    }
    // Max retries exceeded - task is dropped from memory (already written to DB)
    return false
}
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

## Coordinator and RoundQueue System

### QueueCoordinator

The `QueueCoordinator` manages explicit round synchronization between src and dst queues:

- **Bounded Concurrency**: Src can be at most `maxLead` rounds ahead of dst
- **Blocking Behavior**: Src waits when too far ahead; dst waits until src advances
- **Completion Tracking**: Maintains `srcCompleted` and `dstCompleted` flags for queue lifecycle coordination
- **Thread-Safe**: All operations are atomic via `sync.RWMutex`

```go
// Src advances only if within maxLead distance
if !coordinator.CanSrcAdvance() {
    // Poll until dst catches up
}

// Dst advances only when src is ahead by at least one round, or src is completed
// Dst cannot advance if it has already completed
if !coordinator.CanDstAdvance() {
    // Poll until src advances or completes
}
```

**Coordination Logic:**
- `CanSrcAdvance()`: Returns true if `(srcRound - dstRound) < maxLead` and dst has not completed
- `CanDstAdvance()`: Returns true if `(dstRound + 3 <= srcRound || srcCompleted) && !dstCompleted`
  - DST on round N can only advance to round N+1 if SRC is at least on round N+3
  - This ensures SRC round N+1 is complete (and all tasks in successful queue) before DST queries it
- Completion flags are set when queues reach max depth and have no more work
- `maxLead` must be at least 3 for proper coordination (DST needs 3 rounds ahead)

### RoundQueue System

The RoundQueue system provides in-memory task propagation between queues:

- **Round-Scoped Queues**: Each queue maintains `map[int]*RoundQueue` indexed by round number
- **Sub-Queues Per Round**: Each `RoundQueue` has `pending` and `successful` sub-queues
- **Cross-Queue Access**: DST queue can retrieve SRC tasks from `srcQueue.GetSuccessfulTask(round, path)`
- **Automatic Cleanup**: When DST advances rounds, both its own and SRC's completed round queues are deleted

**Data Flow:**
1. Initial tasks seeded into round 0's `pending` sub-queue
2. Workers lease from `pending`, execute, write to DB, then move to `successful`
3. On completion, `handleSrcComplete()` / `handleDstComplete()` creates next round tasks
4. DST tasks retrieve expected children from SRC's `successful` sub-queue of the same round
5. Memory bounded by `maxLead` - old rounds are cleaned up as DST advances

**Locking Pattern:**
- All RoundQueue operations (`AddPending()`, `PopPending()`, `AddSuccessful()`, etc.) must be called while holding `Queue.mu`
- Simple attribute access uses accessor methods (`getRound()`, `getState()`, etc.) that handle locking internally
- Complex operations that need multiple attributes atomically hold the lock themselves
- No nested locks - eliminates deadlock risk

**Benefits:**
- Eliminates database query dependencies for task propagation
- Removes timing-dependent race conditions
- Provides predictable, deterministic task flow
- Efficient memory usage - only active rounds are kept
- DB remains write-ahead log only

---

## Concurrency Model

- Each queue owns its own worker pool (configured at creation)
- Shared `QueueCoordinator` ensures bounded round synchronization
- RoundQueue system provides in-memory task propagation
- Workers share filesystem adapters for efficient rate-limiting
- All logging is asynchronous via `logservice.Sender` (UDP + SQLite)
- Database writes are immediate but use batch inserts for children

**Thread Safety:**
- **Single Mutex Model**: `Queue.mu` (RWMutex) protects all queue state including RoundQueue operations
- RoundQueue has no internal mutex - all operations must be called while holding `Queue.mu`
- Coordinator state protected by `sync.RWMutex`
- Attribute accessors (`getRound()`, `getState()`, etc.) handle locking internally with defer unlock
- Methods like `getOrCreateRoundQueue()` are called from within locked sections (no internal lock)
- Task leasing is atomic
- State transitions are synchronized
- No nested locks - eliminates deadlock risk
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
- ✅ **Self-managing** – Queues own their lifecycle with explicit coordination
- ✅ **Autonomous workers** – Poll continuously, respect state, exit gracefully
- ✅ **RoundQueue system** – In-memory task propagation eliminates DB query dependencies
- ✅ **Coordinated rounds** – Explicit `QueueCoordinator` ensures bounded concurrency (DST stays 3 rounds behind)
- ✅ **Single mutex model** – Queue.mu protects all state, eliminating nested locks and deadlock risk
- ✅ **Attribute accessors** – Thread-safe getters/setters handle locking internally
- ✅ **Deterministic visibility** – RoundQueue system eliminates DB timing dependencies
- ✅ **Immediate DB writes** – No buffering delays, clear causality
- ✅ **Fault tolerance** – Automatic retries, graceful failure handling
- ✅ **Simple API** – Direct queue instantiation and interaction

This design forms the backbone of Sylos' concurrency model: a deterministic, recoverable, high-throughput engine for orchestrating parallel BFS traversals in a single migration pipeline.
