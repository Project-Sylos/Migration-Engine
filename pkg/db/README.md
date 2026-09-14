# db Package

The **db** package is the persistence layer for the Migration Engine. The per-migration store is **Badger-only** (`{migration-id}.ops/`, package **`pkg/opsdb`**). Sylos-API uses a separate Badger store (`sylos.api/`) for users/auth/install config.

Seal writes go to Badger via **`badger_seal.go`**. Node catalog metadata, frontier, status, secondary indexes, GPL issues, filter provenance, and migration/oauth/FS-binding rows all live in Badger. Path-review mutations (exclude, delete skip/unskip, mark-for-retry) write sealed `st:*` + `pend:*` via `idx:path` prefix scans; there is no overlay staging table. Review search uses a small cardinality-based planner over `idx:*` keys plus Go post-filters (`pkg/db/review/planner.go`, `filter_eval.go`).

---

## Badger key layout (`pkg/opsdb`)

| Prefix | Content |
|--------|---------|
| `node:src\|dst:{id}` | Catalog metadata (path, name, size, mtime, …) |
| `child:src\|dst:{parent}:{id}` | Tree edges |
| `kids:src\|dst:{parent}` | Packed child snapshots |
| `map:src:{src_id}`, `map:dst:{dst_id}` | SRC↔DST pairing |
| `st:trav\|copy\|del:src\|dst:{id}` | Phase-split status overlay (hot path writes; legacy `st:{side}:{id}` read fallback) |
| `st:src\|dst:{id}` | Legacy combined status (read fallback for old stores) |
| `pend:src\|dst:{trav\|copy\|del}:{depth}:{type}:{id}` | Round frontier |
| `schedcnt:…` | Pending counts per depth/type |
| `idx:path:{side}:{path}/` | Path index (exact + subtree prefix) |
| `idx:name:{side}:{lower}\x00{id}` | Name index |
| `idx:size:{side}:{u64be}\x00{id}` | Size index |
| `idx:mtime:{side}:{u64be}\x00{id}` | MTime index |
| `idx:seg:{side}:{token}\x00{id}` | Path/name segment postings (whole token) |
| `idx:tri:{side}:{gram}\x00{id}` | Trigram postings for name contains (per segment + name; written on SRC/DST insert). Path search uses `idx:seg` instead. See [docs/search_indexes.md](../../docs/search_indexes.md). |
| `meta:tri_index_v1` | Flag that trigram indexes exist (backfill on open if missing) |
| `gpl:{src_id}`, `gplst:{status}:{src_id}` | Sparse GPL issues |
| `filtapp:{id}` | Filter application provenance |
| `mig:{id}` | Migration lifecycle metadata |
| `oauth:{connection_id}` | OAuth credential payload |
| `fsbind:{role}` | FS credential binding (`source` / `destination`) |
| `stat:review:*`, `stat:idx:*` | Review and index cardinality counters |
| `log:*`, `op:*`, `qstat:*`, `taskerr:*` | Telemetry |

---

## Open / phases

- **`Open`** opens Badger at `OpsDir` (default `MigrationOpsPath(Path)`), then ensures `meta:tri_index_v1` (one-shot rebuild of secondary indexes for older stores).
- **`BeginTraversalPhase` / `EndTraversalPhase`** flush the seal buffer only. Secondary indexes are written on catalog node insert (SRC traversal and DST copy), not at round/phase end.

### Seal flush (trusted batch)

`badger_seal.go` buffers worker results and calls **`opsdb.WriteSealBatchTrusted`**. The flush path does not probe Badger for pending-key existence or prior node/status rows. Discovery node writes set **`InsertOnly`**; status events carry **`PrevStatus`** and **`PendWasSet`** on frontier deltas. SRC folder traversal completes enqueue **`SealKidsReplace`** for the full discovered-child snapshot. Subtree bulk updates (`subtree_mutations.go`) batch status writes with **`PrevStatus`** from scan chunks.

**Review search is Badger-only:** `ListMergedReviewDiffsPage`, status overlay search, folder children, and stats route through `pkg/db/review/*_ops.go` and `planner.go` (`WalkStatus`, `idx:*` keys, Go matchers in `filter_eval.go` / `status_search.go`).
