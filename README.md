# Migration Engine

The **Migration Engine** performs one-way migrations from a *source node tree* to a *destination node tree*.
It is **not** a two-way sync engine; implementing bidirectional sync would require a completely different algorithm.

---

## How It Works

Traversal is based on the **Breadth-First Search (BFS)** algorithm, chosen for its predictability and ability to coordinate two trees in lockstep.
Each traversal operation is isolated into discrete, non-recursive **tasks** so that workloads can be parallelized and queued efficiently.

### Task Flow

1. **List Children**
   From the current node, list immediate children only (no recursion).
   Identify each child's type—whether it can contain further nodes (*recursive nodes*) or represents a terminal node.

2. **Apply Filters**
   Run each child through the configured filter rules using its ID, path, and context.
   Nodes that fail a rule are recorded along with the failure reason but are not scheduled for further traversal.
   This keeps each task stateless and lightweight.

3. **Record Results**
   Children that pass filtering are written through the Badger ops store (`pkg/opsdb` via `pkg/db`): the queue **pulls pending work from Badger** (`pend:*` + status) in batches into an in-memory **`pendingBuff`**, workers lease from that buffer, and **seal** persists completed work in bulk. Badger is the source of truth for resume (frontier, status overlay, tree, stats, telemetry).

---

## Why BFS Instead of DFS?

Depth-First Search (DFS) is memory-efficient, but it's less suited to managing two trees in parallel.
BFS, while it requires storing all nodes at the current level, provides better control, checkpointing, and fault recovery.
The Migration Engine serializes traversal data to the ops store at each round boundary (seal: bulk status/frontier updates and stats), keeping memory use bounded while preserving full traversal context.

### Two Possible Strategies

#### 1. Coupled DFS-Style Traversal

* The source and destination trees are traversed simultaneously.
* Each source node's children are compared directly with the destination's corresponding node.
* Throughput is limited by the slower of the two systems (e.g., 1000 nodes/s source vs 10 nodes/s destination).
* Harder to resume after interruptions because state exists only in memory.
* Example of this pattern: **Rclone**.
* Efficient but fragile for long migrations.

#### 2. Dual-Tree BFS Traversal (Our Approach)

* Source and destination are traversed **in rounds**.
* **Round 0**: traverse the source root and list its children.
* **Round 1**: traverse those children; destination traversal remains coordinated behind the source.
* The destination queue is gated by the `QueueCoordinator`: it may start round *N* only after the source has completed rounds *N* and *N+1* (or source traversal is finished). The source queue has no coordinator pacing cap; frontier work is streamed to Badger in batches, so the source can advance as fast as workers allow.
* When the destination processes its corresponding level, it compares existing nodes against the expected list from the source.
* Extra items in the destination are logged but not traversed further.
* The destination runs as fast as the gate allows; the source is not artificially held back to match destination speed.
* Because each round is sealed to Badger (nodes, status, per-depth stats), the system can resume after a crash; on resume, queues restore round/cursors and **pull** pending work from the ops store again.
* This maximizes both safety and throughput.

---

## Two-Pass Design

The Node Migration Engine operates in two passes:

1. **Discovery Phase** – Traverses both node trees to identify what exists, what's missing, and what conflicts may occur.
2. **Execution Phase** – After user review and approval, performs the actual creation or transfer of missing nodes.

This design gives users complete visibility and control before any data movement occurs.

---

## Migration state and YAML

**This repository’s `migration` package does not expose YAML save/load helpers** (no `SaveMigrationConfig` / `LoadMigrationConfigFromYAML` in the current API).

- **Resume** is driven by the **Badger ops store** (nodes, status overlay, frontier, stats, meta) plus how the host calls **`LetsMigrate`**, **`MigrationManager`**, and domain methods (`StartTraversal`, retry sweep, copy, …).
- A **Sylos API** or other host may persist its own runtime fields (`startedAt`, `completedAt`, etc.); that is outside this module.

See **`pkg/migration/README.md`** for the real entry points and manager lifecycle.

---

## Database architecture

The engine uses **Badger** for per-migration operational state. On disk:

| Path | Role |
|------|------|
| `{migrationDir}/{id}.ops/` | Badger ops directory (source of truth) |
| `{migrationDir}/{id}.db` | Logical path / optional marker (historical name; not a DuckDB catalog) |

Standalone runs (`LetsMigrate`, tests) pass `DatabaseConfig.Path` as the logical `{dir}/{id}.db` path; the engine opens Badger at `MigrationOpsPath(Path)` (`{id}.ops/`). Implementation: **`pkg/db`** + **`pkg/opsdb`**. Queue and migration share the same **`*db.DB`** (Badger handle) for a given run.

### Key families (see `pkg/db/README.md`)

- **Catalog** – `node:src|dst:{id}`, tree edges (`child:`, `kids:`), SRC↔DST maps (`map:`).
- **Status overlay** – phase-split `st:trav|copy|del:src|dst:{id}` (hot path); legacy `st:{side}:{id}` read fallback.
- **Frontier** – `pend:src|dst:{trav|copy|del}:{depth}:{type}:{id}` plus schedule counts.
- **Secondary indexes** – `idx:path`, `idx:name`, `idx:size`, `idx:mtime`, `idx:seg`, `idx:tri` for review search.
- **Meta** – `meta:schema_version` (permanent layout version, currently `0.1`), `meta:tri_index_v1`, `mig:`, `oauth:`, `fsbind:`.
- **Review / GPL** – `gpl:`, `filtapp:`, review counters under `stat:`.
- **Telemetry** – `log:`, `op:`, `qstat:`, `taskerr:`.

**Security:** Treat migration ops directories and backups as sensitive. Hosts such as Sylos-API may encrypt or restrict access; optional field-level sealing of OAuth payloads uses a 32-byte token key passed into the manager when configured.

The queue **pulls** pending tasks via Badger keyset/list APIs; **seal** (`WriteSealBatchTrusted` via `SealBuffer`) bulk-writes completions. **Retry** mode uses DST cleanup paths documented in **`pkg/queue/README.md`**.

There is **no per-migration DuckDB catalog** and no per-task SQL on the hot path.

See **`pkg/db/README.md`** and **`docs/search_indexes.md`** for layout and APIs.

---

## Lifecycle and database ownership

- **`LetsMigrate`** builds a **`MigrationManager`**, **`CreateMigration`**, optionally seeds roots, runs **`StartTraversal`**, then verification. It opens/closes ops stores through the manager for that call path.
- **`SetupDatabase`**, **`GetMigration`**, and domain **`Migration`** methods are used when the host keeps long-lived migrations (e.g. API with per-migration folders).
- **`MigrationController`** (`StartMigration`) only provides **`Shutdown`**, **`Done`**, and **`Wait`**—there is no **`GetDB()`** on the controller in this package.
- **Checkpointing / seal flush** is handled inside **`pkg/db`** / **`pkg/opsdb`**.

See **`pkg/migration/README.md`** for **`PreparePhase`** (`PreparePhaseRetrySweep` / `PreparePhaseCopyRetry` / `PreparePhaseDeleteRetry`) when enqueueing background retry work so phase flips **before** HTTP **202**.

---

## Package overview

| Package       | Role |
|---------------|------|
| **pkg/db**    | Persistence facade: open/close Badger ops (`OpsDir`), seal buffer, review/search, subtree mutations. |
| **pkg/opsdb** | Badger key layout, pull/list/seal primitives, schema version, indexes. |
| **pkg/queue** | Queue layer: BFS rounds, **Badger-backed pull** into `pendingBuff`, seal via seal buffer, coordinator, observer. |
| **pkg/scaling** | Autoscaler: AIMD actuation from observer metrics; `QueueActuator` implemented by `*queue.Queue` (queue does not import scaling). |
| **pkg/migration** | Orchestration: **`MigrationManager`**, domain **`Migration`**, root seeding, traversal/copy/retry APIs, verification. No YAML helpers in-tree. |
| **pkg/configs**   | JSON config loaders: buffer config, log service (UDP), Spectra. |
| **pkg/logservice** | Dual-channel logging: UDP (level-filtered) and persistence to Badger `log:*` via `db.LogBuffer`. |

---

## Documentation

### Design docs (`docs/`)

- **[docs/ops_store_overview.md](./docs/ops_store_overview.md)** – Intro: how Badger is used in write-heavy vs review/search/filter phases.
- **[docs/algorithms.md](./docs/algorithms.md)** – BFS traversal/copy/delete algorithms, streaming copy contract.
- **[docs/autoscaler.md](./docs/autoscaler.md)** – Observer → AIMD loop, profiles, knobs, stall vs throttle.
- **[docs/item_statuses.md](./docs/item_statuses.md)** – Traversal / copy / delete status semantics.
- **[docs/fs_error_classification.md](./docs/fs_error_classification.md)** – Retry vs throttle axes, ambiguous errors.
- **[docs/search_indexes.md](./docs/search_indexes.md)** – Path-segment and trigram review indexes.
- **[docs/filter_rules.md](./docs/filter_rules.md)** – Ruleset exclude predicates over discovered SRC inventory.

### Package READMEs

- **[pkg/migration/README.md](./pkg/migration/README.md)** – Entry points, manager lifecycle, retry sweep.
- **[pkg/queue/README.md](./pkg/queue/README.md)** – Pull / lease / seal, coordinator, watchdogs, checkpoints.
- **[pkg/db/README.md](./pkg/db/README.md)** – Badger key layout, SealBuffer, open/schema version.
- **[pkg/scaling/README.md](./pkg/scaling/README.md)** – Autoscaler, AIMD, operation profiles, QueueActuator.
- **[pkg/configs/README.md](./pkg/configs/README.md)** – JSON loaders (buffer, log service, Spectra).
- **[pkg/logservice/README.md](./pkg/logservice/README.md)** – UDP and DB logging.
- **[pkg/tests/README.md](./pkg/tests/README.md)** – Integration runners layout.

> **Note:** Filenames like `ENGINE_ARCHITECTURE_OVERVIEW.md` or `EPHEMERAL_MODE_GUIDE.md` are **not** in this repository; they may exist in another Sylos repo (e.g. API or docs site). Use **`docs/`** and the package READMEs above as the source of truth for this module.

### Testing

**Package tests** (when present):

```bash
go test ./pkg/db ./pkg/opsdb ./pkg/migration ./pkg/queue
```

**Heavy E2E / integration tests** (run only when doing full validation; they are resource-heavy and can stress the system):

- **Traversal Tests**: `pkg/tests/traversal/`
  - `normal/` - Standard persistent mode tests
  - `ephemeral/` - Ephemeral mode tests (stateless, large-scale)
  - `resumption/` - Resume interrupted migrations
  - `retry_sweep/` - Retry failed tasks with permission changes

- **Copy Tests**: `pkg/tests/copy/` - File content migration tests

Run these manually using the provided scripts (e.g. `pkg/tests/traversal/local/run.sh` or the repo `run.sh` / `run.ps1`). Do **not** run them via `go test ./...`; reserve them for deliberate end-to-end runs.
