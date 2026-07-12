## Purpose of this doc
The purpose of this doc is to explain the various item statuses and schema.

**Implementation note:** In DuckDB, the **current** traversal/copy values for a node come from the latest row in **`src_status_events`** / **`dst_status_events`** (append-only, `arg_max` by `id`). The `src_nodes` / `dst_nodes` tables hold path/metadata; queries join events for live status. See **`pkg/db/README.md`**.

### The main status fields
Sylos SRC nodes track three lifecycle status fields (all event-derived from `src_status_events`):

* **Traversal status** — discovery phase
* **Copy status** — copy phase (SRC only)
* **Delete status** — source cleanup / delete phase (SRC only)

DST nodes have traversal status only; copy and delete statuses apply to SRC.

### Possible Traversal Status Values

- successful -- the item was successfully traversed
- failed -- the attempt to traverse the item has failed
- pending -- the item has not yet been attempted to traverse (or maybe it was at one point but set to retry traversal since the last review phase)
- not_on_src -- This is exclusive to destination items, if the item only exists on the destination but 'not on the src FS tree' then we don't care because it's a 1 way sync and these are treated as successful but a different kind of successful. 


### Possible Copy Status Values
(Again as a reminder these are only for src items not dst items)

- successful -- the item was successfully copied over to the destination
- failed -- the item was not successfully copied over to the destination
- pending -- the item either does not exist on the destination, or the src copy is newer. 
- excluded_explicit -- the item has been explicitly marked to be excluded from copying over by the user
- excluded_inherited -- a child of an item that was excluded explicitly by the user. (see point just above this)

### Why copy status only exists on src items
Mainly because it doesn't make sense to double store it, we only care about items that are ONLY on the src that haven't been copied over yet. So we only need to store it there.

### Possible Delete Status Values
(SRC only; stored in `src_status_events`, same append-only model as traversal/copy)

* **pending** — selected for source removal during cleanup planning (`PrepareSourceCleanup` after copy review). The delete phase will attempt to remove this node from the source filesystem.
* **deleted** — successfully removed from the source (or recorded as such after a successful `DeleteNode` worker result).
* **failed** — delete was attempted and failed (including folder-gate blocks when direct children are not yet deleted).
* **skipped** — user opted this node out of source removal during cleanup planning (deselected in review UI).
* **(null / absent)** — no delete event for this node. Normal for depth-0 root (metadata anchor, never deleted) and for nodes not yet involved in cleanup planning.

Delete status is independent of copy status but delete work only applies to nodes with `copy_status = successful` that are not excluded.

### Root node (depth 0) delete behavior

The SRC root (`path = "/"`, `depth = 0`) is seeded at traversal start with `copy_status = successful` and **no** `delete_status` event — it is never a copy or delete task.

During `PrepareSourceCleanup`, depth-0 nodes are skipped entirely (no `pending` or `skipped` event). This mirrors copy and prevents the delete phase from waiting on a node that is intentionally never removed from the source filesystem.

### Cleanup planning (`PrepareSourceCleanup`)

Before delete runs, copy review can align delete status with user selection:

* **Default (all selected):** every successfully copied SRC node at depth ≥ 1 gets `delete_status = pending`.
* **Keep list / deselect list:** selected nodes → `pending`; deselected → `skipped`.
* Nodes already `deleted` or `failed` are left unchanged when re-marking pending.

Delete phase pulls only `pending` (or `failed` in delete-retry) nodes at depths 1 through `maxKnownDepth`.

## Explanation of path review actions
Please note that user actions during path review phases will vary depending on which review we're on. 
For example, a node that is 'pending retry' in discovery / traversal review means that we need to traverse that item, whereas 'pending' in that stage would mean it's pending to be copied over (copy status pending). It's important to keep these distinctions in mind when knowing which status types to reference to explain API / UI behavior by the engine.

During **copy review**, `delete_status = pending` means the user has selected that node for source removal in the upcoming delete phase; `skipped` means it will remain on the source. During **delete review**, `deleted` / `failed` reflect outcomes of the delete run. 
