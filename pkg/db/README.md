# db Package

The **db** package is the persistence layer for the Migration Engine. It owns the database file, schema, and all read/write operations. The queue uses a **memory-first** flow: **NodeCache is required**. Hot-path updates go to in-memory level caches; the DB is written only at **seal** (round advance) via bulk append and stats snapshot.

---

## Overview

- **DB** (`db.go`): Opens DuckDB (file or `:memory:`), creates schema, holds one `*sql.DB` and transaction runners. Exposes **seal** API `SealLevel(...)` for persistence; `AddNodeDeletions` for retry DST cleanup; `Checkpoint` for durability. When `Options.SealBuffer` is non-nil, seal writes go through **SealBuffer** (async); otherwise seal is synchronous.
- **SealBuffer** (`seal_buffer.go`): Buffers seal jobs (table, depth, nodes, stats); flushes to `src_nodes`/`dst_nodes` and stats tables on interval, row/batch threshold, and `Stop`/`Flush`. Used for write-ahead-log behavior at round seal without blocking the queue.
- **Writer** (`writer.go`): Used inside `RunWrite` via `WriteSession.WithTx`. `AppenderInsert` bulk-inserts nodes into live tables; `WriteLevelStatsSnapshot` writes per-depth stats (traversal and, for SRC, copy).
- **Schema** (`schema.go`): DDL for live tables (`src_nodes`, `dst_nodes`, `src_stats`, `dst_stats`, `stats`, `logs`, `queue_stats`, `task_errors`).
- **Queries** (`queries.go`): Read-only helpers: node by id/path, root, children, keyset lists by depth (traversal and copy), subtree counts, stats, batch lookups, `ListDstBatchWithSrcChildren`. All use the main DB connection.
- **Constants** (`constants.go`): Traversal and copy status values, node types.
- **Types** (`nodestate.go`): `NodeState`, `NodeMeta`, `InsertOperation`, `FetchResult`, `DeterministicNodeID`.
- **Logs** (`logs.go`): **LogBuffer** batches log entries and flushes to the `logs` table via `Writer` (time-based, batch-size, and manual `Flush`/`Stop`). Unchanged from pre-seal-buffer design.
- **Seeding** (`seeding.go`): `InsertRootNode`, `BootstrapRootStats`, `BatchInsertNodes` for initial setup.
- **Stats** (`stats.go`): Stats key helpers, `GetStatsCount`, `GetStatsCountAtDepth`, `GetCopyCountAtDepth`, `GetMaxDepth`, `GetPendingTraversalCountAtDepthFromLive`, `GetStatsBreakdown`, queue stats getters.
- **Indexes** (`indexes.go`): `EnsureNodeTableIndexes` for path, parent_path, traversal_status, copy_status on node tables.

---

## Tables

| Table          | Purpose |
|----------------|---------|
| `src_nodes`    | Live source tree (path, depth, traversal_status, copy_status). Written at seal via bulk append. |
| `dst_nodes`    | Live destination tree. Same write path as SRC. |
| `src_stats`    | Per-depth counts (traversal and copy). Written at seal from in-memory snapshot. |
| `dst_stats`    | Per-depth traversal status counts. |
| `stats`        | Global key/count (e.g. completed counts). |
| `logs`         | Log entries (from `LogBuffer`). |
| `queue_stats`  | Queue metrics JSON per queue key. |
| `task_errors`  | Task error records (phase, node_id, message, etc.). |

---

## Write Paths

1. **Cache + seal (only path)**  
   **NodeCache is required.** Queue holds per-level caches (`NodeCache` / `LevelCache`). Task completion updates cache only. At **seal** (round advance), the queue calls `SealLevel(...)`. When a **SealBuffer** is configured (`Options.SealBuffer != nil`), the payload is enqueued and written asynchronously (flush on interval, row/job threshold, or `DB.Close`); otherwise the write is synchronous in one transaction.
2. **Transactional updates**  
   `RunWrite(ctx, fn)` runs `fn(WriteSession)` while holding `writeMu`. Use `s.Conn()` for raw connection (e.g. DuckDB appender in seal buffer) or `s.WithTx(fn(Writer))` for a transaction. Used for seal flush, deletes (`AddNodeDeletions`), logs, queue_stats, and test setup.

---

## Read Paths

- **Pulls**: `ListNodesByDepthKeyset`, `ListNodesCopyKeyset`, `ListDstBatchWithSrcChildren`, `GetNodeByID`, `GetNodeByPath`, etc. use `GetDB()` / `GetDBForPulls(queueType)` (single connection).
- **Stats**: `GetStatsCount`, `GetStatsCountAtDepth`, `GetCopyCountAtDepth`, `GetMaxDepth`, `GetStatsBreakdown`, etc. read from `src_stats` / `dst_stats` and live tables as needed.
- **Other**: `CountSubtree`, `GetAllLevels`, `BatchGetNodeMeta`, `BatchGetNodesByID`, and similar helpers all query the same DuckDB connection.

---

## Concurrency and Checkpoint

- **Single connection**: `conn.SetMaxOpenConns(1)` so all operations share one connection; no cross-connection CHECKPOINT issues.
- **Writes**: Guarded by `writeMu`; `RunWrite` serializes all writes (conn and WithTx). Seal buffer flushes and log buffer flushes use the same mutex. `Checkpoint` is separately guarded by `checkpointMu` (call at root seeding and round advancement only).

---

## File Layout

```
pkg/db/
├── db.go        # DB open/close, schema init, SealLevel, AddNodeDeletions, checkpoint, RunWrite (WriteSession), optional SealBuffer
├── seal_buffer.go # SealBuffer: async seal jobs, flush on interval/row/job threshold and Stop
├── writer.go    # Writer: AppenderInsert, WriteLevelStatsSnapshot, stats, deletes, logs, task_errors, queue_stats
├── schema.go    # DDL for node, stats, logs, queue_stats, task_errors, migrations
├── queries.go   # All read queries (nodes, keysets, counts, batch lookups)
├── constants.go # Status and node-type constants
├── nodestate.go # NodeState, InsertOperation, FetchResult, DeterministicNodeID
├── logs.go      # LogBuffer → logs table (time + batch + manual flush)
├── seeding.go   # InsertRootNode, BootstrapRootStats, BatchInsertNodes
├── stats.go     # Stats key helpers and stats/queue_stats read APIs
└── indexes.go   # EnsureNodeTableIndexes (path, parent_path, traversal_status, copy_status)
```

---

## Integration

- **Queue layer** (`pkg/queue`): Uses this package for reads (keysets, stats), seal (`SealLevel`), retry DST cleanup (`AddNodeDeletions`), and resume rehydration (`RehydrateLevelFromDB`). NodeCache is required; hot-path writes go to cache only.
- **Migration** (`pkg/migration`): Opens the DB via `db.Open` or receives an existing `*db.DB`; inspects status and runs verification against the same DB. Log service and config (`pkg/configs`) can supply log address and optional buffer settings.

---

## Summary

- **Single connection**: One `*sql.DB`; all reads and writes go through it.
- **Cache + seal only**: Writes to the DB are bulk append and stats at seal (`SealLevel`), plus `AddNodeDeletions` for retry and transactional updates for logs, queue_stats, and setup. The former **buffers.go** (removed) was for the old staging/node merge path; **SealBuffer** and **LogBuffer** provide the appender-buffer behavior for seal and logs only (no staging): time-based, count-based, and manual flush.
- **Transactional updates**: Seal (sync or via buffer flush), stats, deletes, and logging go through `RunWrite` (with `WriteSession.WithTx(Writer)` or `Conn()` for appender).
