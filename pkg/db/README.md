# db Package

The **db** package is the persistence layer for the Migration Engine. It owns the DuckDB file, schema, and read/write paths used by **`pkg/queue`** and **`pkg/migration`**.

Traversal and copy **current status** for each node are derived from append-only **`src_status_events`** / **`dst_status_events`** (latest event per `id` via `arg_max`), not from columns on `src_nodes` / `dst_nodes` (those tables store path, depth, type, size, etc.).

Node **`id`** is a UUID v5 minted from `(side, parent_id, type, basename)` ([`MintNodeID`](node_id.go)). SRC↔DST pairing uses append-only **`id_map`**. Path review / remaps use sparse **`gpl_issues`**; compact GPL findings also ride on **`src_nodes.gpl_state`**. Joins use `parent_id` / `id_map` — not path hashes.

Seal: **`SealLevel`** bulk-appends node rows and writes per-depth stats; when **`Options.SealBuffer`** is set, seal payloads are flushed asynchronously (**`seal_buffer.go`**).

---

## Overview

- **`db.go`** – Open/close, schema init, **`SealLevel`**, **`AddNodeDeletions`**, checkpoint, **`RunWrite`** / **`WriteSession`**, optional **`SealBuffer`**.
- **`encryption.go`** – DuckDB file open path helpers (directory create, `:memory:`).
- **`seal_buffer.go`** – Buffers seal jobs; flush on interval, thresholds, `Stop`/`Flush`; **`SealBufferTelemetry`** for autoscaler seal-backpressure reads.
- **`writer.go`** – Transactional **`Writer`**: status events, appender inserts, review deltas, logs, `queue_stats`, `task_errors`.
- **`writer_stats.go`** – Universal **`stats`** / review snapshot writers; depth stats recompute helpers.
- **`writer_subtree.go`** – Subtree mutations (exclude/unexclude, delete propagation, GPL restore/status, copy-work counts under a root).
- **`schema.go`** – DDL: node tables, **status event** tables, **`stats`**, **`gpl_issues`**, **`id_map`**, **`migrations`**, `fs_credential_binding`, `oauth_credentials`, `src_stats`/`dst_stats`, `logs`, `queue_stats`, `task_errors`.
- **`current_status.go`** – Shared SQL for event-derived **current status** (single source for rebuilds and heavy joins).
- **`queries.go`** – Status join helpers, inline current-status CTEs, pull scan-window constants.
- **`queries_pull.go`** – Frontier reads: **`GetNodeByID`**, keyset lists (traversal, copy, delete, DST batch+children), counts.
- **`queries_review.go`** – Review API list/filter queries per side.
- **`merged_review_search.go`** – Merged SRC/DST review search (independent side scans + zipper merge in Go).
- **`gpl_queries.go`** – GPL pending keyset pulls and related reads.
- **`stats.go`** – Review snapshot, stats keys, copy/traversal counts from events.
- **`copy_work_stats.go`** – Append-only **`copy_work/*`** / **`delete_work/*`** denominator keys for observer progress.
- **`transfer_checkpoint.go`** – Mid-transfer checkpoint columns on **`src_nodes`** (`xfer_*`).
- **`failure_log.go`** – Attach failure detail on status events at seal; read latest logs by node ID.
- **`path_events.go`** – Legacy **`PathEvent`** shape bridged to **`gpl_issues`**; **`id_map`** source/status constants.
- **`oauth_creds.go`** – Seal/open OAuth credential JSON (`enc:v1:` when a token key is set).
- **`node_id.go`** – UUID v5 node ID minting.
- **`constants.go`**, **`nodestate.go`**, **`logs.go`**, **`seeding.go`**, **`indexes.go`** – Supporting types and indexes (including status-event indexes).

---

## Tables (high level)

| Table | Purpose |
|-------|---------|
| `src_nodes`, `dst_nodes` | Node metadata (path, depth, type, size, `name`, …). Status from events. SRC also has `xfer_*` and `gpl_state`. |
| `src_status_events`, `dst_status_events` | Append-only status history; SRC includes `copy_status` / `delete_status`. |
| `src_stats`, `dst_stats` | Per-depth keyed counts (traversal/copy buckets). |
| `stats` | Global key → count (canonical **review** counters, copy/delete work totals, etc.). |
| `gpl_issues` | Sparse path-review / GPL issue rows (`src_id`, status, proposed name, `dst_action`, …). |
| `id_map` | Append-only SRC↔DST identity mappings. |
| `migrations` | One row per logical migration (id, name, **phase**, JSON blobs) when using **`MigrationManager`**. |
| `fs_credential_binding` | SRC/DST connection id, optional creds path, service id, serialized root folder. |
| `oauth_credentials` | OAuth refresh material by connection id (`enc:v1:` when a token key is set). |
| `logs` | Buffered log rows. |
| `queue_stats` | Queue metrics JSON. |
| `task_errors` | Task error records. |

---

## Write paths

1. **Seal** – Queue completes a round/level → **`SealLevel`** (sync or via **`SealBuffer`** flush).
2. **Transactional** – **`RunWrite(ctx, fn)`** → **`WithTx(Writer)`** for status events, review mutations, deletes, logs, etc.

---

## Read paths

- **`GetDB()`** – Single connection (`MaxOpenConns(1)`).
- Frontier pulls and node lookups: **`queries_pull.go`**.
- Review lists and merged search: **`queries_review.go`**, **`merged_review_search.go`**.
- GPL pulls: **`gpl_queries.go`**.
- Aggregates and review snapshots: **`stats.go`**, **`copy_work_stats.go`**.
- Failure detail: **`failure_log.go`** (`LatestFailureLogIDsByNodeIDs`, `GetFailureLogsByIDs`).

---

## Concurrency

- **`writeMu`** serializes writes; checkpoint uses **`checkpointMu`** where applicable.

---

## File layout

Production `.go` files (tests omitted):

```
pkg/db/
├── db.go                    # Open, schema, SealLevel, RunWrite, SealBuffer hookup
├── encryption.go            # DuckDB connection open helpers
├── schema.go                # DDL and table constants
├── constants.go
├── nodestate.go
├── node_id.go               # MintNodeID (UUID v5)
├── indexes.go
├── logs.go
├── seeding.go
├── current_status.go        # Event-derived current-status SQL fragments
├── queries.go               # Status join helpers, pull window constants
├── queries_pull.go          # Frontier keyset pulls, GetNodeByID
├── queries_review.go        # Per-side review list queries
├── merged_review_search.go  # Merged review search (zipper merge)
├── gpl_queries.go           # GPL pending keyset pulls
├── stats.go                 # Review snapshot, event-based counts
├── copy_work_stats.go       # copy_work/* and delete_work/* stats keys
├── transfer_checkpoint.go   # Mid-transfer xfer_* columns on src_nodes
├── failure_log.go           # Task failure attach at seal + log reads
├── path_events.go           # PathEvent → gpl_issues bridge, id_map constants
├── oauth_creds.go           # OAuth credential seal/open
├── writer.go                # Core transactional Writer
├── writer_stats.go          # stats / review snapshot writers
├── writer_subtree.go        # Subtree exclude, delete, GPL mutations
├── seal_buffer.go           # Async seal buffer + SealBufferTelemetry
├── developer_note.md
└── README.md
```

---

## Integration

- **`pkg/queue`** – Pulls via queries; persists via **`SealLevel`** and writers.
- **`pkg/migration`** – **`MigrationManager`** / domain **`Migration`**; review and lifecycle use the same **`*db.DB`**.

---

## Summary

- **Single DuckDB connection** for operational queries.
- **Status** = latest row per node in **status event** tables.
- **Seal** + **SealBuffer** for bulk level writes; **`stats`** holds universal review aggregates.
