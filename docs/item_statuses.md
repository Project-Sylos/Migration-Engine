## Purpose of this doc
The purpose of this doc is to explain the various item statuses and schema.

**Implementation note:** In Badger, the **current** traversal/copy/delete values for a node live on the status overlay keys **`st:trav|copy|del:src|dst:{id}`** (hot path; legacy **`st:{side}:{id}`** is a read fallback). Catalog metadata is under **`node:src|dst:{id}`**. Seal batches update status + frontier together. See **`pkg/db/README.md`**.

### The main status fields
Sylos SRC nodes track three lifecycle status fields (on the SRC status overlay):

* **Traversal status** — discovery phase
* **Copy status** — copy phase (SRC is the copy-queue hot path)
* **Delete status** — source cleanup / delete phase (SRC only)

DST nodes track traversal status and a **bookkeeping copy status** (`st:copy:dst:{id}`) for review search and overwrite planning. Delete status remains SRC-only. The copy frontier still pulls `pend:src:copy:*` from SRC only.

### Possible Traversal Status Values

- successful -- the item was successfully traversed
- failed -- the attempt to traverse the item has failed
- pending -- the item has not yet been attempted to traverse (or maybe it was at one point but set to retry traversal since the last review phase)
- not_on_src -- Legacy DST-only marker (older migrations). New discovery does **not** write this: DST-only folders are trav `pending` (walked like SRC) and DST-only files are terminal trav `successful`, with DST copy `destination_only`. Review detects destination-only rows via DST copy `destination_only` (indexed as `idx:copy:dst:destination_only:{id}`).
- excluded -- SRC only: the user excluded this item at root-pick (folders). The engine does not traverse its subtree. It counts as **one** discovered item in excluded totals (not the unknown subtree under it). Excluded **files** keep `traversal_status = successful` and use `copy_status = excluded_explicit` instead, so they remain listed and can be unexcluded in copy review. 


### Possible Copy Status Values (SRC)

- successful -- this migration's copy phase actually created/updated the item on the destination (or an in-run resume precheck confirmed a copy this run already performed)
- already_existed -- exists on both sides without a copy task for this migration (SRC root seed, or DST traversal name/mtime match)
- failed -- the item was not successfully copied over to the destination
- pending -- the item either does not exist on the destination, or the src copy is newer. 
- excluded_explicit -- the item has been explicitly marked to be excluded from copying over by the user
- excluded_inherited -- a child of an item that was excluded explicitly by the user. (see point just above this)

Progress, delete eligibility, and UI "exists on both" treat SRC `successful` and `already_existed` as **copy-complete**. Resume detection (dst existence window) keys only on true `successful` counts so fresh post-traversal runs are not mistaken for copy resume.

### Possible Copy Status Values (DST bookkeeping)

Written during DST traversal comparison and kept in sync with SRC review mutations. Indexed under `idx:copy:dst:{status}:{id}` for selective list search.

- destination_only -- unpaired DEST (no SRC peer); never enters the copy queue. Trav may still be `pending` for folders so nested DST-only content is discovered.
- already_existed -- paired; keep DEST / no overwrite (SRC peer is also `already_existed`).
- needs_overwrite -- paired; SRC will replace this DEST object (SRC peer is `pending`, including force-replace). Copy worker deletes then creates.
- successful -- this migration's copy phase wrote/replaced the DEST object.

### Why SRC still owns the copy hot path
The copy queue leases SRC nodes. DST copy status mirrors pairing/overwrite intent for search and for delete-before-create, without a second frontier.

### Possible Delete Status Values
(SRC only; stored on `st:del:src:{id}`, same overlay model as traversal/copy)

* **pending** — selected for source removal during cleanup planning (`PrepareSourceCleanup` after copy review). The delete phase will attempt to remove this node from the source filesystem.
* **deleted** — successfully removed from the source (or recorded as such after a successful `DeleteNode` worker result).
* **failed** — delete was attempted and failed (including folder-gate blocks when direct children are not yet deleted).
* **skipped** — user opted this node out of source removal during cleanup planning (deselected in review UI).
* **(null / absent)** — no delete event for this node. Normal for depth-0 root (metadata anchor, never deleted) and for nodes not yet involved in cleanup planning.

Delete status is independent of copy status but delete work only applies to nodes with copy-complete status (`successful` or `already_existed`) that are not excluded.

### Root node (depth 0) delete behavior

The SRC root (`path = "/"`, `depth = 0`) is seeded at traversal start with `copy_status = already_existed` and **no** `delete_status` event — it is never a copy or delete task.

During `PrepareSourceCleanup`, depth-0 nodes are skipped entirely (no `pending` or `skipped` event). This mirrors copy and prevents the delete phase from waiting on a node that is intentionally never removed from the source filesystem.

### Cleanup planning (`PrepareSourceCleanup`)

Before delete runs, copy review can align delete status with user selection:

* **Default (empty body):** idempotent init only. Copy-complete SRC nodes at depth ≥ 1 with **unset** `delete_status` become `pending`. Existing `pending` / `skipped` / `failed` / `deleted` are left alone so remounting the source-cleanup page does not wipe the user's skip plan.
* **Keep list / deselect list:** selected nodes → `pending`; deselected → `skipped`.
* Nodes already `deleted` or `failed` are left unchanged when re-marking pending.

Delete phase pulls only `pending` (or `failed` in delete-retry) nodes at depths 1 through `maxKnownDepth`.

## Explanation of path review actions
Please note that user actions during path review phases will vary depending on which review we're on. 
For example, a node that is 'pending retry' in discovery / traversal review means that we need to traverse that item, whereas 'pending' in that stage would mean it's pending to be copied over (copy status pending). It's important to keep these distinctions in mind when knowing which status types to reference to explain API / UI behavior by the engine.

During **copy review**, `delete_status = pending` means the user has selected that node for source removal in the upcoming delete phase; `skipped` means it will remain on the source. During **delete review**, `deleted` / `failed` reflect outcomes of the delete run.

### Exclude / acknowledge by surface

"Exclude" is phase-aware (same API entrypoint; engine branches on phase + current status):

| Surface | Exclude means | Status change |
|---------|---------------|---------------|
| Discovery **results** (awaiting-traversal-review, default review UI) | Acknowledge failure / do not traverse further | `traversal_status` failed → excluded (`exclusion_source = ack_failed_traversal`). Successful discovered items are not excludable here. |
| Copy **plan** (same phase, copy-plan UI) | Do not copy | `copy_status` pending → `excluded_explicit` / inherited (unchanged) |
| Copy **results** | Acknowledge copy failure | `copy_status` failed → `excluded_explicit` with `retry_excluded_from_failed` (also works from retry-marked pending). Unexclude restores failed. |
| Source cleanup **plan** | Keep on source | `delete_status` → skipped |
| Delete **results** | Acknowledge delete failure | `delete_status` failed → skipped |

Acknowledged exclusions are intentional ignores: discovery/copy/delete **retry pulls must not re-select** these nodes. Unresolved `failed` (neither marked for retry nor acknowledged) should block Path Review continue until the user resolves them.

Mark-for-retry from an acknowledged traversal exclusion still works and behaves like retrying a failure (restores via the normal failed→pending retry path).

### Destination name collisions (GPL)

When destination path checks are on, a SRC basename that cleans into (or case-folds into, on Windows-like targets) an existing DST sibling becomes a `manual_review` GPL issue. Cleaned-name rematch no longer auto-pairs those hits.

Resolution actions:

| Action | Who | Effect |
|--------|-----|--------|
| **Unique** | Files or folders | Allocates `base`, `base (1)`, … under Windows-aware sibling rules, then Accept rename. Folders get a cascade dry-run (nested file overwrite preview) against any DST sibling that already uses the intended name. |
| **Manual rename** | Files or folders | Existing remap/accept path. |

Folder merge / unmerge (pairing a SRC folder into a colliding DST folder) is deferred and not available. Path Review stays on SRC `id_path`; paired children come from `id_map`. Unpaired SRC-only / DST-only rows stay two rows with `(src)` / `(dst)` labels on Copy Plan. GPL / naming UI is Copy Plan only (hidden on Discover).

**Browse layout:** Discovery and copy review default to an independent dual-pane tree/list (SRC and DST browsed separately via `side=src|dst`). Merged browse remains available. Source-cleanup / delete review is SRC-only. Dual browse is presentation only; Badger pairing and statuses are unchanged.

**File overwrite policy (exists on both):** chosen at Start discovery (`newest_mtime` default, `decide`, or `source_wins`). Default is newest `LastUpdated` / `mtime` (DST newer or equal → `already_existed`; SRC newer → copy `pending`). Not a content merge. Optional HashVerify streams SHA-256 of both sides for name-matched files (costly). Override per file (or folder subtree) with force-replace: **Use source instead** / **Keep destination**. Start Copying on Copy Plan stays blocked while non-ignored `manual_review` issues remain.

