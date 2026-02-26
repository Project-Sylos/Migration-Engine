# db Package

The **db** package is the persistence layer for the Migration Engine. It owns the database file, schema, buffered writes (staging and node inserts via DuckDB Appender), transactional updates (merge, stats, deletes, logs), and all read queries used by the queue and migration layers.

---

## Overview

- **DB** (`db.go`): Opens DuckDB (file or `:memory:`), creates schema, holds one `*sql.DB` and four **write buffers** (src/dst staging, src/dst nodes). Exposes `AddToStaging`, `AddCopyToStaging`, `AddNode`/`AddNodes`, `FlushTablesForQueue`, `MaybeMergeStagingEarly`, `Checkpoint`, and transaction runners.
- **Buffers** (`buffers.go`): Table-scoped buffers batch staging rows (coalesced by node_id) and node rows, then flush via **Appender** in `appender.go`. Batch size and flush interval are fixed (overridable via config in the future). Back-pressure at 2× batch size; optional `onFlush` callback for leased-key removal in the queue.
- **Appender** (`appender.go`): `queueAppenderWriter` creates DuckDB Appenders for `src_staging`, `dst_staging`, `src_nodes`, `dst_nodes` per queue (DST also writes copy updates to `src_staging`). Used by buffer flush paths only.
- **Writer** (`writer.go`): Used inside `RunUpdateWriterTx` / `RunAppenderWriterTx`. Applies staging → live (`ApplyStatusStagingAndDrop`), recomputes stats, deletes nodes/subtrees, writes logs and task_errors, queue_stats. All in one transaction.
- **Schema** (`schema.go`): DDL for live tables (`src_nodes`, `dst_nodes`, `src_stats`, `dst_stats`, `stats`, `logs`, `queue_stats`, `task_errors`) and staging tables (`src_staging`, `dst_staging`).
- **Queries** (`queries.go`): Read-only helpers: node by id/path, root, children, keyset lists by depth (traversal and copy), subtree counts, stats, batch lookups, `ListDstBatchWithSrcChildren` (DST batch + SRC children in one query). All use the main DB connection.
- **Constants** (`constants.go`): Traversal and copy status values, node types.
- **Types** (`nodestate.go`): `NodeState`, `NodeMeta`, `InsertOperation`, `FetchResult`, `DeterministicNodeID`; legacy `WriteOperation` implementations for staging and batch insert.
- **Logs** (`logs.go`): `LogBuffer` batches log entries and flushes to the `logs` table via `Writer`.
- **Seeding** (`seeding.go`): `InsertRootNode`, `BootstrapRootStats`, `BatchInsertNodes` for initial setup.
- **Stats** (`stats.go`): Stats key helpers, `GetStatsCount`, `GetStatsCountAtDepth`, `GetCopyCountAtDepth`, `GetMaxDepth`, `GetPendingTraversalCountAtDepthFromLive`, `GetStatsBreakdown`, queue stats getters.
- **Indexes** (`indexes.go`): `EnsureNodeTableIndexes` for path, parent_path, traversal_status, copy_status on node tables (call after traversal/copy for that table is complete).

---

## Tables

| Table          | Purpose |
|----------------|---------|
| `src_nodes`    | Live source tree (path, depth, traversal_status, copy_status). Written via appender + staging merge. |
| `dst_nodes`    | Live destination tree. Written via appender + staging merge. |
| `src_staging`  | Pending traversal/copy status updates for SRC; merged into `src_nodes` at seal. |
| `dst_staging`  | Pending traversal status updates for DST; merged into `dst_nodes` at seal. |
| `src_stats`    | Per-depth counts (traversal and copy status). Recomputed at seal. |
| `dst_stats`    | Per-depth traversal status counts. Recomputed at seal. |
| `stats`        | Global key/count (e.g. completed counts). |
| `logs`         | Log entries (from `LogBuffer`). |
| `queue_stats`  | Queue metrics JSON per queue key. |
| `task_errors`  | Task error records (phase, node_id, message, etc.). |

---

## Write Paths

1. **Staging (traversal/copy status)**  
   Queue calls `AddToStaging(table, nodeID, newTraversal)` or `AddCopyToStaging(nodeID, newCopyStatus)`. Buffers coalesce by node_id; when batch is full or `FlushTablesForQueue` runs, rows are written via **Appender** to `src_staging` / `dst_staging`. At **seal**, `ApplyStatusStagingAndDrop` merges staging into live nodes and clears staging.

2. **Node inserts**  
   Queue calls `AddNode` / `AddNodes`. Buffers accumulate nodes; flush uses **Appender** to write into `src_nodes` / `dst_nodes`. Same connection is used for subsequent pulls so new rows are visible without ETL.

3. **Transactional updates**  
   `RunUpdateWriterTx(fn)` runs `fn(Writer)` in a single transaction: merge staging, recompute stats, delete nodes/subtrees, insert logs, record task errors, write queue stats. Serialized with other writes via `writeMu`.

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
