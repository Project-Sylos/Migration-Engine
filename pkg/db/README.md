# db Package

The **db** package is the persistence layer for the Migration Engine. It owns the database file, schema, and all read/write operations. The queue uses a **memory-first** flow: hot-path updates go to in-memory level caches; the DB is written only at **seal** (round advance) via bulk append and stats snapshot. A legacy staging path remains for compatibility when caches are not used.

---

## Overview

- **DB** (`db.go`): Opens DuckDB (file or `:memory:`), creates schema, holds one `*sql.DB`, buffers (when using staging path), and transaction runners. Exposes **seal** API `SealLevel(table, depth, nodes, pending, successful, failed, completed, copyP, copyS, copyF)` for memory-first persistence; also `AddToStaging`, `AddCopyToStaging`, `AddNode`/`AddNodes`, `FlushTablesForQueue`, `Checkpoint` for the legacy staging path.
- **Writer** (`writer.go`): Used inside `RunUpdateWriterTx`. `AppenderInsert` bulk-inserts nodes into live tables; `WriteLevelStatsSnapshot` writes per-depth stats (traversal and, for SRC, copy). For staging path: `ApplyStatusStagingAndDrop` merges staging into live.
- **Schema** (`schema.go`): DDL for live tables (`src_nodes`, `dst_nodes`, `src_stats`, `dst_stats`, `stats`, `logs`, `queue_stats`, `task_errors`) and optional staging tables (`src_staging`, `dst_staging`).
- **Queries** (`queries.go`): Read-only helpers: node by id/path, root, children, keyset lists by depth (traversal and copy), subtree counts, stats, batch lookups, `ListDstBatchWithSrcChildren`. All use the main DB connection.
- **Constants** (`constants.go`): Traversal and copy status values, node types.
- **Types** (`nodestate.go`): `NodeState`, `NodeMeta`, `InsertOperation`, `FetchResult`, `DeterministicNodeID`.
- **Logs** (`logs.go`): `LogBuffer` batches log entries and flushes to the `logs` table via `Writer`.
- **Seeding** (`seeding.go`): `InsertRootNode`, `BootstrapRootStats`, `BatchInsertNodes` for initial setup.
- **Stats** (`stats.go`): Stats key helpers, `GetStatsCount`, `GetStatsCountAtDepth`, `GetCopyCountAtDepth`, `GetMaxDepth`, `GetPendingTraversalCountAtDepthFromLive`, `GetStatsBreakdown`, queue stats getters.
- **Indexes** (`indexes.go`): `EnsureNodeTableIndexes` for path, parent_path, traversal_status, copy_status on node tables.

---

## Tables

| Table          | Purpose |
|----------------|---------|
| `src_nodes`    | Live source tree (path, depth, traversal_status, copy_status). Written at seal via bulk append (memory-first) or via appender + staging merge (legacy). |
| `dst_nodes`    | Live destination tree. Same write paths as SRC. |
| `src_staging`  | (Legacy) Pending traversal/copy status updates; merged into `src_nodes` at seal when not using memory-first. |
| `dst_staging`  | (Legacy) Pending traversal status updates for DST. |
| `src_stats`    | Per-depth counts (traversal and copy). Written at seal from in-memory snapshot or recomputed from staging. |
| `dst_stats`    | Per-depth traversal status counts. |
| `stats`        | Global key/count (e.g. completed counts). |
| `logs`         | Log entries (from `LogBuffer`). |
| `queue_stats`  | Queue metrics JSON per queue key. |
| `task_errors`  | Task error records (phase, node_id, message, etc.). |

---

## Write Paths

1. **Memory-first (primary)**  
   Queue holds per-level caches (`NodeCache` / `LevelCache`). Task completion updates cache only. At **seal** (round advance), the queue calls `SealLevel(table, depth, nodes, ...)`: one transaction bulk-appends the sealed level’s nodes into `src_nodes`/`dst_nodes` and writes `WriteLevelStatsSnapshot` for that depth. No staging tables on the hot path.

2. **Staging (legacy)**  
   When caches are not set, queue calls `AddToStaging`, `AddCopyToStaging`, `AddNode`/`AddNodes`. Buffers flush to staging/node tables; at seal, `ApplyStatusStagingAndDrop` merges staging into live and recomputes stats.

3. **Transactional updates**  
   `RunUpdateWriterTx(fn)` runs `fn(Writer)` in a single transaction. Serialized with other writes via `writeMu`.

---

## Read Paths

- **Pulls**: `ListNodesByDepthKeyset`, `ListNodesCopyKeyset`, `ListDstBatchWithSrcChildren`, `GetNodeByID`, `GetNodeByPath`, etc. use `GetDB()` / `GetDBForPulls(queueType)` (single connection).
- **Stats**: `GetStatsCount`, `GetStatsCountAtDepth`, `GetCopyCountAtDepth`, `GetMaxDepth`, `GetStatsBreakdown`, etc. read from `src_stats` / `dst_stats` and live tables as needed.
- **Other**: `CountSubtree`, `GetAllLevels`, `BatchGetNodeMeta`, `BatchGetNodesByID`, and similar helpers all query the same DuckDB connection.

---

## Concurrency and Checkpoint

- **Single connection**: `conn.SetMaxOpenConns(1)` so all operations share one connection; no cross-connection CHECKPOINT issues.
- **Writes**: Guarded by `writeMu`; buffer flushes and `RunUpdateWriterTx` / `RunAppenderWriterTx` are serialized. `Checkpoint` is separately guarded by `checkpointMu` (call at root seeding and round advancement only).

---

## File Layout

```
pkg/db/
├── db.go        # DB open/close, schema init, buffers, staging/node add, flush, checkpoint, transaction runners
├── buffers.go   # writeBuffer (staging + nodes), batch/interval, back-pressure, flush loop
├── appender.go  # queueAppenderWriter, DuckDB Appender for staging and node tables
├── writer.go    # Writer: merge staging, stats, deletes, logs, task_errors, queue_stats
├── schema.go    # DDL for all tables
├── queries.go   # All read queries (nodes, keysets, counts, batch lookups)
├── constants.go # Status and node-type constants
├── nodestate.go # NodeState, InsertOperation, FetchResult, DeterministicNodeID
├── logs.go      # LogBuffer → logs table
├── seeding.go   # InsertRootNode, BootstrapRootStats, BatchInsertNodes
├── stats.go     # Stats key helpers and stats/queue_stats read APIs
└── indexes.go   # EnsureNodeTableIndexes (path, parent_path, traversal_status, copy_status)
```

---

## Integration

- **Queue layer** (`pkg/queue`): Uses this package for all traversal and copy reads/writes: add to staging, add nodes, flush before pull/round advance, run merge at seal, read keysets and stats.
- **Migration** (`pkg/migration`): Opens the DB via `db.Open` or receives an existing `*db.DB`; inspects status and runs verification against the same DB. Log service and config (`pkg/configs`) can supply log address and optional buffer settings.

---

## Summary

- **Single connection**: One `*sql.DB`; all reads and writes go through it so appender-written data is visible to pulls and stats immediately.
- **Appender + staging**: High-throughput staging and node inserts via DuckDB Appender; status applied to live tables at seal via `Writer`.
- **Transactional updates**: Merge, stats, deletes, and logging go through `RunUpdateWriterTx(Writer)`.
