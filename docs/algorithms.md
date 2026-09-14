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

### Root seeding and optional round-0 injection

At **Start discovery** (`AddRoots`), the engine always writes SRC/DST root rows into DuckDB (not earlier). Classic seeding inserts each root as `traversal_status = pending` so workers run **round 0** (`ListChildren` on the root).

When the UI has already reviewed root children (root-pick review), `RootPreparation` is passed into `AddRoots`:

* Prepared side: root is seeded as `traversal_status = successful`, reviewed children are inserted as a **sparse forest** (nested `Children` when the UI visited deeper folders). Included folders without nested children stay `pending` (unrestricted subtree). Partial parents get `src_nodes.include_only` (JSON child service IDs); unchecked siblings are **omitted** (silent), not counted `excluded`.
* Queues start at `ComputeSourceStartRound` (minimum depth of still-pending included folders), typically 1 for classic depth-1 prep.
* Unprepared side (for example destination-first with no child review): classic pending root and **round 0**.
* Destination never excludes; dst-only children get `traversal_status = not_on_src`.
* Traversal workers honor `include_only` after `ListChildren` (drop non-members before seal).

The `filters-set` phase (between `AddRoots` and `StartTraversal`) remains available for binding a ruleset snapshot. Filter **evaluation** runs after discovery in path review (SQL over DuckDB), not during BFS. Full-tree counted excludes happen in awaiting-traversal-review via exclude-by-search / manual exclude.

---

### Source Traversal (src)

Algorithm:

1. Enqueue a folder task.
2. Worker processes dequeue folder tasks and list **only immediate children** (non-recursive).
3. Files are marked `traversal_status = successful` (files are terminal nodes).
4. Folders are marked `traversal_status = pending`.
5. Child folders are inserted into the level cache rather than traversed immediately. They are processed when the engine advances to the next BFS level.

Source traversal is largely independent, it can advance to whatever round it wants unlike dst traversal which is dependent on src's current completed round number.

On round advance, seal Flush updates Badger frontier indexes and pending keys. Round **Expected** is read from per-depth stats in Badger.

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

   * If folder → `copy_status = already_existed`
   * If file:

     * If source timestamp > destination timestamp → `copy_status = pending`
     * Otherwise → `copy_status = already_existed`

3. **Exists only on destination**
   → No `copy_status` assigned (ignored in one-way sync model)

The SRC root is seeded with `copy_status = already_existed` (never a copy task). Only the copy phase writes `copy_status = successful` when it actually creates/updates an item.

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

* Read/download from source (`OpenRead`)
* Write/upload to destination (`OpenWrite` / optional `OpenWriteWithSize`)

**Byte streaming (required):** ME copies with a small read/write loop (tens of KiB). Destination adapters must accept those writes as a live stream — upload session fragments / parts go out during `Write` (or via a pipe consumer that uploads concurrently). **Do not** stage the whole file in RAM or spill to a temp file and only upload on `Close`. Session start/finish RPCs (e.g. Dropbox upload session) are fine; `Close` should finalize the session, not carry the bulk transfer.

Progress heartbeats: each successful `Write` that moves bytes should allow ME to beat both the per-task progress watchdog and the queue watchdog. Close-only bulk upload looks like a queue stall even when the network is busy.

Optional `FSOpenWriteWithSize` lets adapters that need a declared length up front (e.g. Box chunked sessions) receive `file.Size` on overwrite paths where the pending id has no size.

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

The delete phase removes successfully copied source (SRC) content after copy review. It mirrors the copy phase structurally: **forward BFS** (depth 1 → max) and **folders before files**. Only `pending_explicit` roots are pulled; providers perform recursive folder deletes, and `pending_inherited` descendants are marked deleted when their covering root succeeds.

### Scope

* **SRC only** — one delete queue; destination nodes are not deleted.
* **Operational depths:** **1** through `maxKnownDepth` (from `GetMaxDepth("SRC")`).
* **Depth 0 (root) is never processed** — the root row is metadata-only (same as copy: seeded `copy_status = already_existed`, no delete work). `PrepareSourceCleanup` does not assign `delete_status` to depth-0 nodes.
* **Eligible pull nodes:** copy-complete (`successful` or `already_existed`), not excluded, and `delete_status = pending_explicit`. Covered children use `pending_inherited` and are not pulled.
* **When `delete_status` is assigned:** discovery leaves it unset. On copy-complete (`CompleteCopyTask` or SRC `already_existed` from DST match), the engine writes `pending_explicit` unless a strict parent is already pending* (then `pending_inherited`). `PrepareSourceCleanup` remains a safety net (seed unset copy-complete + normalize forest) and applies skip/keep edits.
* **Work totals / metrics** count both `pending_explicit` and `pending_inherited` (nodes and file bytes that will leave the source). Live progress credits cascaded descendants when an explicit folder delete succeeds.

### Two global passes (copy-shaped)

Like copy, delete exhausts **all depths in pass 1** before switching to pass 2. Depth increases each round (forward BFS).

| Pass | Node type | Depth sweep |
|------|-----------|-------------|
| 1 | Folders | 1 → … → `maxKnownDepth` |
| 2 | Files | 1 → … → `maxKnownDepth` |

**Pass switch** occurs only after pass 1 has fully exhausted `maxKnownDepth`, with the same in-memory gates as round completion: pending buffer empty, in-progress zero, `lastPullWasPartial = true`, and at least one counted pull for the round. The queue then resets to depth 1 for pass 2.

**Phase complete** when pass 2 has exhausted `maxKnownDepth` under the same round-completion gates. Completion trusts per-round exhaustion (like copy); it does not re-query global pending* across the tree.

### Per-depth round advancement

Within a pass, when a depth’s keyspace is exhausted:

1. `advanceToNextRound` flushes the seal buffer.
2. `AdvanceDeleteRound` increments depth (`currentRound + 1`) and **keeps the same pass**.
3. When past `maxKnownDepth`, `CheckDeleteCompletion` runs (pass switch or phase complete).

Round completion gates (shared with traversal/copy) require:

* In-memory pending buffer empty
* In-progress count zero
* `lastPullWasPartial = true` (terminal keyset pull for this depth/pass)
* `PullCount > 0` for the round
* Not currently pulling

### Pull and recursive delete

`PullDeleteTasks` loads SRC nodes at the current depth from the `pend:del` frontier (enrolled only for `pending_explicit`, or `failed` in delete-retry). `pending_inherited` nodes are never enrolled; they are covered by ancestor recursive deletes. Depth 0 pulls are skipped (`currentRound == 0` → no pull). There is no folder emptiness gate: children of an explicit folder are `pending_inherited` and are not deleted individually.

Skip/unskip during copy review maintains the forest incrementally (subtree mark, ancestor `skipped_descendant_count`, sibling promote/demote). `NormalizeDeleteForestOps` runs as a safety net at `StartDelete` / prepare, not on every click.

Workers call `DeleteNode` on the source adapter (folders = recursive provider delete). Success cascades `delete_status = deleted` to the explicit root and all pending* under its path.

### Delete retry mode

Delete retry reuses the same forward-BFS and two-pass structure. It only pulls nodes whose current `delete_status = failed`. `findDeleteStartRound` scans depths from low to high (skipping depth 0) to find the lowest depth with failed work.

### Comparison to copy

| | Copy (forward BFS) | Delete (forward BFS) |
|---|-------------------|----------------------|
| Pass order | Folders → files | Folders → files |
| Depth direction | 1 → maxKnownDepth | 1 → maxKnownDepth |
| Pull filter | `copy_status = pending` | `delete_status = pending_explicit` |
| Root (depth 0) | Never copied (`already_existed`) | Never deleted (null delete_status) |

---
