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
   Children that pass filtering are written through the database layer: the queue **pulls pending work from DuckDB** in batches (keyset by depth/status), workers lease from an in-memory buffer, and **seal** persists completed levels in bulk. The database (including append-only **status events**) is the source of truth for resume.

---

## Why BFS Instead of DFS?

Depth-First Search (DFS) is memory-efficient, but it's less suited to managing two trees in parallel.
BFS, while it requires storing all nodes at the current level, provides better control, checkpointing, and fault recovery.
The Migration Engine serializes traversal data to the database at each round boundary (seal: bulk append of the completed level and stats snapshot), keeping memory use bounded while preserving full traversal context.

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
* The destination queue is gated by the `QueueCoordinator`: it may start round *N* only after the source has completed rounds *N* and *N+1* (or source traversal is finished). The source queue has no coordinator pacing cap; frontier work is streamed to the database in batches, so the source can advance as fast as workers allow.
* When the destination processes its corresponding level, it compares existing nodes against the expected list from the source.
* Extra items in the destination are logged but not traversed further.
* The destination runs as fast as the gate allows; the source is not artificially held back to match destination speed.
* Because each round is sealed to the database (nodes and per-depth stats), the system can resume after a crash; on resume, queues restore round/cursors and **pull** pending work from the DB again.
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

- **Resume** is driven by **DuckDB** contents (nodes, status events, stats) plus how the host calls **`LetsMigrate`**, **`MigrationManager`**, and domain methods (`StartTraversal`, retry sweep, copy, …).  
- A **Sylos API** or other host may persist its own runtime fields (`startedAt`, `completedAt`, etc.); that is outside this module.

See **`pkg/migration/README.md`** for the real entry points and manager lifecycle.

---

## Database architecture

The engine uses **DuckDB**. Two deployment shapes:

1. **Legacy single file** – `DatabaseConfig.Path` is set; one `.db` file (and optional multiple rows in the `migrations` table).  
2. **Per-migration files** – `Path` empty on the manager; each migration has `{migrationDir}/{id}.db` (typical for HTTP APIs).

Implementation: **`pkg/db`**. Queue and migration share the same **`*db.DB`** for a given run.

### Tables and roles

- **Node tables** (`src_nodes`, `dst_nodes`) – Metadata (path, depth, type, size, …). **Current traversal/copy status** comes from append-only **`src_status_events`** / **`dst_status_events`** (latest event per node), not from long-lived columns on the node row.
- **Stats** – `src_stats` / `dst_stats` per depth; global **`stats`** table for canonical review counters and similar key/value aggregates.
- **`migrations`** – Lifecycle row (id, name, **phase**, JSON metadata) when using **`MigrationManager`**.
- **`migration_envelope`** – Single row (`singleton = 1`) with a 32-byte **envelope master key** for Sylos-FS credential encryption (HKDF per connection). Written by the host (e.g. Sylos-API) on first use.
- **`fs_credential_binding`** – Up to two rows (`source` / `destination`): stable **connection id**, optional **relative path** to a creds/config file under the migration directory (e.g. `spectra-config.json`), **service id**, and **serialized root folder** JSON so adapters can be rebuilt after restart.
- **Other** – `logs`, `queue_stats`, `task_errors`.

**Security:** The per-migration DuckDB file contains the envelope key in plaintext. Anyone with the file can derive per-connection keys and read encrypted credential material. Restrict filesystem permissions and treat backups as sensitive; a future layer can wrap the envelope key with a server secret without changing the table shape much.

The queue **pulls** pending tasks via SQL (keyset pagination); **seal** (`SealLevel`, optionally via **SealBuffer**) bulk-writes completed levels. **Retry** mode uses DST cleanup paths documented in **`pkg/queue/README.md`** (and `AddNodeDeletions` where applicable).

All reads and writes use a **single SQL connection** per open `*db.DB`.

See **`pkg/db/README.md`** for schema and APIs.

---

## Lifecycle and database ownership

- **`LetsMigrate`** builds a **`MigrationManager`**, **`CreateMigration`**, optionally seeds roots, runs **`StartTraversal`**, then verification. It opens/closes DBs through the manager for that call path.
- **`SetupDatabase`**, **`GetMigration`**, and domain **`Migration`** methods are used when the host keeps long-lived migrations (e.g. API with per-migration folders).
- **`MigrationController`** (`StartMigration`) only provides **`Shutdown`**, **`Done`**, and **`Wait`**—there is no **`GetDB()`** on the controller in this package.
- **Checkpointing** is handled inside **`pkg/db`** (serialized with the single connection).

See **`pkg/migration/README.md`** for **`PrepareRetrySweep`** / **`PrepareCopyRetry`** when enqueueing background retry work so phase flips **before** HTTP **202**.

---

## Package overview

| Package       | Role |
|---------------|------|
| **pkg/db**    | Database layer: open/close, schema (node/stats/logs tables), seal (bulk append + stats), read queries. Single DuckDB file and connection. |
| **pkg/queue** | Queue layer: BFS rounds, **DB-backed pull** into `pendingBuff`, seal via `SealLevel`, coordinator, observer. Uses `*db.DB` for pulls, seal, and resume. |
| **pkg/migration** | Orchestration: **`MigrationManager`**, domain **`Migration`**, root seeding, traversal/copy/retry APIs, verification. No YAML helpers in-tree. |
| **pkg/configs**   | JSON config loaders: buffer config, log service (UDP), Spectra. |
| **pkg/logservice** | Dual-channel logging: UDP (level-filtered) and persistence to the main DB’s `logs` table via `db.LogBuffer`. |

---

## Documentation

### Docs in this repo

- **[docs/algorithms.md](./docs/algorithms.md)** – Algorithm notes.
- **[docs/autoscaler.md](./docs/autoscaler.md)** – Autoscaler design: pipeline, knobs, profiles, telemetry (living doc).
- **[docs/item_statuses.md](./docs/item_statuses.md)** – Status semantics.
- **[pkg/db/README.md](./pkg/db/README.md)** – Schema, seal, Writer, queries.
- **[pkg/queue/README.md](./pkg/queue/README.md)** – Pull/seal flow, modes, coordinator.
- **[pkg/migration/README.md](./pkg/migration/README.md)** – Manager, domain lifecycle, **`PrepareRetrySweep`** / async retry.
- **[pkg/configs/README.md](./pkg/configs/README.md)** – JSON loaders (buffer, log service, Spectra).
- **[pkg/logservice/README.md](./pkg/logservice/README.md)** – UDP and DB logging.
- **[pkg/tests/README.md](./pkg/tests/README.md)** – Integration runners layout.

> **Note:** Filenames like `ENGINE_ARCHITECTURE_OVERVIEW.md` or `EPHEMERAL_MODE_GUIDE.md` are **not** in this repository; they may exist in another Sylos repo (e.g. API or docs site). Use the package READMEs above as the source of truth for this module.

### Testing

**Package tests** (when present):

```bash
go test ./pkg/db ./pkg/migration ./pkg/queue
```

These packages may compile with **no `_test.go` files** in some checkouts; the command still verifies the modules build. Add focused tests here when you extend the engine.

**Heavy E2E / integration tests** (run only when doing full validation; they are resource-heavy and can stress the system):

- **Traversal Tests**: `pkg/tests/traversal/`
  - `normal/` - Standard persistent mode tests
  - `ephemeral/` - Ephemeral mode tests (stateless, large-scale)
  - `resumption/` - Resume interrupted migrations
  - `retry_sweep/` - Retry failed tasks with permission changes

- **Copy Tests**: `pkg/tests/copy/` - File content migration tests

Run these manually using the provided scripts (e.g. `pkg/tests/traversal/local/run.sh` or the repo `run.sh` / `run.ps1`). Do **not** run them via `go test ./...`; reserve them for deliberate end-to-end runs.
