# db Package

The **db** package is the persistence layer for the Migration Engine. It owns the DuckDB file, schema, and read/write paths used by **`pkg/queue`** and **`pkg/migration`**.

Traversal and copy **current status** for each node are derived from append-only **`src_status_events`** / **`dst_status_events`** (latest event per `id` via `arg_max`), not from columns on `src_nodes` / `dst_nodes` (those tables store path, depth, type, size, etc.).

Seal: **`SealLevel`** bulk-appends node rows and writes per-depth stats; when **`Options.SealBuffer`** is set, seal payloads are flushed asynchronously (**`seal_buffer.go`**).

---

## Overview

- **`db.go`** – Open/close, schema init, **`SealLevel`**, **`AddNodeDeletions`**, checkpoint, **`RunWrite`** / **`WriteSession`**, optional **`SealBuffer`**.
- **`seal_buffer.go`** – Buffers seal jobs; flush on interval, thresholds, `Stop`/`Flush`.
- **`writer.go`** – Transactional **`Writer`**: status events, appender inserts, stats, review deltas, subtree ops, logs, `queue_stats`, `task_errors`.
- **`schema.go`** – DDL: node tables, **status event** tables, **`stats`** (universal key/count, e.g. review aggregates), **`migrations`** (lifecycle row per migration when using manager), `src_stats`/`dst_stats`, `logs`, `queue_stats`, `task_errors`.
- **`queries.go`** – Read helpers: nodes, keyset lists, merged review, children, counts.
- **`stats.go`** – Review snapshot, stats keys, copy/traversal counts from events.
- **`constants.go`**, **`nodestate.go`**, **`logs.go`**, **`seeding.go`**, **`indexes.go`** – Supporting types and indexes (including status-event indexes).

---

## Tables (high level)

| Table | Purpose |
|-------|---------|
| `src_nodes`, `dst_nodes` | Node metadata (path, depth, type, size, …). Status from events. |
| `src_status_events`, `dst_status_events` | Append-only status history; SRC includes `copy_status`. |
| `src_stats`, `dst_stats` | Per-depth keyed counts (traversal/copy buckets). |
| `stats` | Global key → count (canonical **review** counters, etc.). |
| `migrations` | One row per logical migration (id, name, **phase**, JSON blobs) when using **`MigrationManager`**. |
| `logs` | Buffered log rows. |
| `queue_stats` | Queue metrics JSON. |
| `task_errors` | Task error records. |

---

## Write paths

1. **Seal** – Queue completes a round/level → **`SealLevel`** (sync or via **`SealBuffer`** flush).
2. **Transactional** – **`RunWrite(ctx, fn)`** → **`WithTx(Writer)`** for status events, review mutations, deletes, logs, etc.

---

## Read paths

- **`GetDB()`** / **`GetDBForPulls(queueType)`** – Single connection (`MaxOpenConns(1)`).
- Keyset pulls, `GetNodeByID`, merged review queries, stats readers—see **`queries.go`** / **`stats.go`**.

---

## Concurrency

- **`writeMu`** serializes writes; checkpoint uses **`checkpointMu`** where applicable.

---

## File layout

```
pkg/db/
├── db.go           # Open, schema, SealLevel, RunWrite, SealBuffer hookup
├── seal_buffer.go
├── writer.go
├── schema.go
├── queries.go
├── stats.go
├── constants.go
├── nodestate.go
├── logs.go
├── seeding.go
├── indexes.go
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
