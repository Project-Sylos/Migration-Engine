## Explanation of the Algorithms for Each Major Mode of Operation

### BFS vs DFS

For traversal, retry, and copy phases, the engine uses a **Breadth-First Search (BFS)** strategy rather than Depth-First Search (DFS).

BFS was chosen because the engine relies on a semi-coupled source–destination traversal model. Level-synchronized processing simplifies:

* Parent/child alignment between source and destination
* **DB-backed frontier** (batched pulls from DuckDB) with bounded in-memory **lease buffers**, rather than unbounded deep stacks
* Deterministic phase progression
* Level-based copy execution (folders first, then files)

DFS would complicate synchronization between source and destination trees and would increase the risk of unbounded memory pressure due to deep recursive traversal before corresponding destination state is known.

---

## Traversal Phase

Traversal consists of two distinct but coordinated processes:

* **Source traversal (src)**
* **Destination traversal (dst)**

Both operate in BFS rounds (levels).

---

### Source Traversal (src)

Algorithm:

1. Enqueue a folder task.
2. Worker processes dequeue folder tasks and list **only immediate children** (non-recursive).
3. Files are marked `traversal_status = successful` (files are terminal nodes).
4. Folders are marked `traversal_status = pending`.
5. Child folders are inserted into the level cache rather than traversed immediately. They are processed when the engine advances to the next BFS level.

Source traversal is largely independent, it can advance to whatever round it wants unlike dst traversal which is dependent on src's current completed round number.

---

### Destination Traversal (dst)

Destination traversal follows a similar BFS structure, with one additional synchronization requirement.

When preparing a destination traversal task:

1. Pull the destination parent folder.
2. Query the corresponding source parent folder.
3. Preload the destination task with the source parent’s discovered children (retrieved from the cache).
4. Perform src–dst comparison logic.

#### Comparison Rules

For each item under a destination parent:

1. **Exists only on source**
   → `copy_status = pending`

2. **Exists on both source and destination**

   * If folder → `copy_status = successful`
   * If file:

     * If source timestamp > destination timestamp → `copy_status = pending`
     * Otherwise → `copy_status = successful`

3. **Exists only on destination**
   → No `copy_status` assigned (ignored in one-way sync model)

The engine intentionally ignores destination-only subtrees to avoid traversing potentially large, irrelevant structures. The tradeoff is reduced visibility during review for items that exist exclusively on the destination.

---

### Macro-Level Queue Differences

**Source Traversal**

* Independent except for filters.
* May traverse entire source tree.
* Hard bounded to remain within three levels of destination traversal.

**Destination Traversal**

* Only traverses items that exist on both source and destination.
* Cannot advance to a BFS level until the source traversal is at least **two levels deeper** than the destination’s current level.

This ensures the necessary source parent nodes and children are already discovered and cached before destination comparison tasks are constructed.

---

## Traversal Completion Criteria

Traversal is considered complete only when all of the following are satisfied:

1. The active queue is empty.
2. The node cache contains no pending items for the current level.
3. At least one pull attempt has occurred for the current level.
4. A pull attempt returned zero tasks.

Operationally:

If the engine advances to a new BFS level, attempts to pull tasks, and finds none available for that level, the queue is considered complete because the maximum depth has already been exhausted in prior rounds.

---

## Traversal Retry Mode

Traversal Retry Mode reuses the core traversal algorithm with two modifications:

1. Only items marked as retry-eligible during review are traversed. The full tree is not walked again.
2. Completion detection must account for historical depth.

Before applying the standard completion checks (#3 and #4 above), the engine queries the database for the **maximum known depth** from the previous traversal run.

Reason:
A BFS level may have zero retry items even though deeper levels still contain retry-eligible nodes. Therefore, completion logic is gated by:

```
if current_level >= max_known_depth:
    apply standard completion checks
```

This prevents premature termination of the retry pass.

---

## Copy Phase

The copy phase executes in two BFS passes. Each pass performs an O(n) sweep over the relevant subset of nodes.

At each level:

* Query the database for `copy_status = pending`
* Filter by node type (folders or files depending on pass)
* Load tasks into the node cache
* Execute operations level by level (BFS order)

### Pass 1: Folders

All pending folders are created on the destination filesystem level by level.

This guarantees parent directories exist before file writes begin.

### Pass 2: Files

All pending files are processed level by level:

* Read/download from source
* Write/upload to destination

Because traversal already determined copy eligibility, the copy phase is strictly executional.

---

## Copy Retry Mode

Copy Retry Mode mirrors traversal retry mechanics:

* Only failed or retry-flagged copy items are processed.
* BFS level discipline is preserved.
* Completion logic follows the same guarded termination pattern as traversal retry.

No additional structural differences exist beyond scope restriction.

---

## Delete Phase

The delete phase removes successfully copied source (SRC) content after copy review. It mirrors the copy phase structurally but runs in **reverse BFS** (deepest depth first) and uses the **opposite pass order** (files before folders) so children are removed before parents.

### Scope

* **SRC only** — one delete queue; destination nodes are not deleted.
* **Operational depths:** `maxKnownDepth` down to **1** (from `GetMaxDepth("SRC")`).
* **Depth 0 (root) is never processed** — the root row is metadata-only (same as copy: seeded `copy_status = successful`, no delete work). `PrepareSourceCleanup` does not assign `delete_status` to depth-0 nodes.
* **Eligible nodes:** `copy_status = successful`, not excluded, and `delete_status = pending` (set during copy-review cleanup planning via `PrepareSourceCleanup`).

### Two global passes (copy-shaped)

Like copy, delete exhausts **all depths in pass 1** before switching to pass 2. Unlike copy, depth decreases each round (reverse BFS).

| Pass | Node type | Depth sweep |
|------|-----------|-------------|
| 1 | Files | `maxKnownDepth` → … → 1 |
| 2 | Folders | `maxKnownDepth` → … → 1 |

**Pass switch** occurs only after pass 1 has fully exhausted depth 1 (the bottom of the reverse sweep), with the same in-memory gates as round completion: pending buffer empty, in-progress zero, `lastPullWasPartial = true`, and at least one counted pull for the round. The queue then resets to `maxKnownDepth` for pass 2.

**Phase complete** when pass 2 has exhausted depth 1 under the same round-completion gates. Completion trusts per-round exhaustion (like copy); it does not re-query global `delete_status = pending` across the tree.

### Per-depth round advancement

Within a pass, when a depth’s keyspace is exhausted:

1. `advanceToNextRound` flushes the seal buffer.
2. `AdvanceDeleteRound` decrements depth (`currentRound - 1`) and **keeps the same pass**.
3. When depth 1 completes, `CheckDeleteCompletion` runs (pass switch or phase complete).

Round completion gates (shared with traversal/copy) require:

* In-memory pending buffer empty
* In-progress count zero
* `lastPullWasPartial = true` (terminal keyset pull for this depth/pass)
* `PullCount > 0` for the round
* Not currently pulling

### Pull and folder gate

`PullDeleteTasks` loads SRC nodes at the current depth with the current pass filter (`pending` for normal delete, `failed` for delete-retry). Depth 0 pulls are skipped (`currentRound == 0` → no pull).

Before enqueueing folder tasks (pass 2), the engine calls `FolderDeleteBlockedIDs`: a folder is not deletable until **every direct non-excluded child** has `delete_status = deleted`. Blocked folders are marked failed with a `copy_blocked` error rather than wedging the queue.

Workers call `DeleteNode` on the source adapter; success emits `delete_status = deleted` via the seal buffer.

### Delete retry mode

Delete retry reuses the same reverse-BFS and two-pass structure. It only pulls nodes whose current `delete_status = failed`. `findDeleteStartRound` scans depths from high to low (skipping depth 0) to find the highest depth with failed work.

### Comparison to copy

| | Copy (forward BFS) | Delete (reverse BFS) |
|---|-------------------|----------------------|
| Pass order | Folders → files | Files → folders |
| Depth direction | 1 → maxKnownDepth | maxKnownDepth → 1 |
| Pass switch after | All depths in pass 1 | All depths in pass 1 (ends at depth 1) |
| Restart depth on pass 2 | 1 | maxKnownDepth |
| Root (depth 0) | Never copied (pre-successful) | Never deleted (null delete_status) |

---
