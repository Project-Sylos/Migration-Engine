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
   Children that pass filtering are recorded in per-level in-memory caches (memory-first path) or the database’s buffered APIs. At round advance (seal), the current level is persisted to the database in bulk. The queue pulls by depth and status from cache or keyset queries; the database is the source of truth for sealed state and resume.

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
* The destination queue is gated by the `QueueCoordinator`; the source may run at most a few rounds ahead of the destination (configurable, default 3).
* When the destination processes its corresponding level, it compares existing nodes against the expected list from the source.
* Extra items in the destination are logged but not traversed further.
* The destination can run as fast as possible while staying coordinated with the source.
* Because each round is sealed to the database (nodes and per-depth stats), the system can resume exactly where it left off after a crash; on resume, level caches are rehydrated from the DB.
* This maximizes both safety and throughput.

---

## Two-Pass Design

The Node Migration Engine operates in two passes:

1. **Discovery Phase** – Traverses both node trees to identify what exists, what's missing, and what conflicts may occur.
2. **Execution Phase** – After user review and approval, performs the actual creation or transfer of missing nodes.

This design gives users complete visibility and control before any data movement occurs.

---

## Migration State Persistence

The Migration Engine includes a comprehensive YAML-based configuration system that automatically saves migration state at critical milestones, enabling pause and resume functionality.

### Automatic State Tracking

Migration state is automatically persisted to a YAML config file. The path defaults to `{database_path}.yaml` (e.g. `migration.duckdb.yaml`) and can be overridden via `DatabaseConfig.ConfigPath`. State is saved at:

- **Root selection** – When source and destination roots are set
- **Roots seeded** – After root tasks are seeded into the database
- **Traversal started** – When queues are initialized and ready
- **Round advancement** – When source or destination rounds advance during traversal
- **Traversal complete** – When migration finishes

### Serialization and Deserialization

You can save and load migration sessions from YAML files:

**Save (Serialization):**
```go
yamlCfg, err := migration.NewMigrationConfigYAML(cfg, status)
err = migration.SaveMigrationConfig("migration.yaml", yamlCfg)
```

**Load (Deserialization):**
```go
// Load YAML config for inspection
yamlCfg, err := migration.LoadMigrationConfig("migration.yaml")

// Or reconstruct full config for resuming
cfg, err := migration.LoadMigrationConfigFromYAML("migration.yaml", adapterFactory)
result, err := migration.LetsMigrate(cfg) // Resume migration
```

The YAML config stores:
- Migration metadata (ID, timestamps)
- Current state (status, last rounds/levels)
- Service configurations (source, destination, embedded service configs)
- Migration options (workers, retries, etc.)
- Logging and database settings

This enables:
- **Pause and Resume** - Stop a migration and resume later from the exact checkpoint
- **State Inspection** - Review migration progress without running
- **Configuration Portability** - Move migration configs between environments
- **Audit Trail** - Track when migrations were created, modified, and completed

See `pkg/migration/README.md` for detailed documentation on the configuration system.

---

## Database architecture

The Migration Engine uses a **single database file** (DuckDB) for all operational state: traversal and copy node data, per-depth stats, logs, and queue metrics. The implementation lives in `pkg/db`; the queue and migration packages use the same `*db.DB` instance.

### Tables and roles

- **Node tables** (`src_nodes`, `dst_nodes`) – One row per node (path, depth, `traversal_status`, `copy_status`, etc.). **NodeCache is required.** Workers pull tasks from the level cache; at **seal** (end of each round) the queue bulk-appends the completed level to these tables.
- **Stats tables** (`src_stats`, `dst_stats`) – Per-depth counts by status (e.g. traversal/pending, traversal/successful). Written at seal from in-memory snapshot; used for completion detection and progress.
- **Other** – `stats` (global key/count), `logs` (log buffer from `pkg/logservice`), `queue_stats` (observer metrics), `task_errors`.

Traversal and copy status are columns on the node rows; there are no separate “status buckets.” SRC/DST correlation is by path (and join by path/parent_path in queries like `ListDstBatchWithSrcChildren`). Node IDs are deterministic (e.g. from `db.DeterministicNodeID`) for stable keys and deduplication.

### Write path

1. **During a round**: Task completion updates the level cache and in-memory stats only; no DB write until seal.
2. **At seal**: The queue calls `database.SealLevel(table, depth, nodes, ...)` to bulk-append the level's nodes and write per-depth stats in one transaction. Round then advances; next pull uses the cache (rehydrated from DB on resume).
3. **Retry**: `AddNodeDeletions` for DST cleanup when SRC folder completes in retry mode.

All reads and writes use a **single connection** to the database.

See **`pkg/db/README.md`** for schema and transaction APIs.

---

## Lifecycle and database ownership

- **Who opens the database**: You can pass an already-open `*db.DB` in `migration.Config.DatabaseInstance`, or leave it nil and set `migration.Config.Database.Path` so the engine calls `db.Open` when it runs.
- **Who closes it**: If the engine opened the DB (instance was nil), it will close it on exit only when `Config.CloseWhenDone` is true (e.g. standalone/CLI). When you pass `DatabaseInstance` and use the engine as a library, typically `CloseWhenDone` is false so the API keeps the connection after the migration returns. Use `MigrationController.GetDB()` to keep using the same DB after `Wait()`.
- **Checkpointing**: The engine checkpoints the database at root seeding and when advancing rounds (and on shutdown when using the controller). Checkpoint is serialized inside `pkg/db` so only one connection is used.
- **Config YAML**: State is saved to a YAML file (default `{Database.Path}.yaml`) at the milestones listed above. Resume by loading that YAML and running again with the same (or a new) database path or instance; the engine inspects the DB and YAML to decide whether to start fresh or resume.

See **`pkg/migration/README.md`** for `LetsMigrate`, `StartMigration`, `SetupDatabase`, and config loading.

---

## Package overview

| Package       | Role |
|---------------|------|
| **pkg/db**    | Database layer: open/close, schema (node/stats/logs tables), seal (bulk append + stats), read queries. Single DuckDB file and connection. |
| **pkg/queue** | Queue layer: BFS rounds, task pull from cache, completion updates to cache, seal via `SealLevel`, coordinator, observer. NodeCache required; uses `*db.DB` for seal and resume. |
| **pkg/migration** | Orchestration: open or accept DB, YAML config, root seeding, run traversal/copy via queue, verification. Owns lifecycle (who opens/closes) when used as entrypoint. |
| **pkg/configs**   | JSON config loaders: buffer config, log service (UDP), Spectra. |
| **pkg/logservice** | Dual-channel logging: UDP (level-filtered) and persistence to the main DB’s `logs` table via `db.LogBuffer`. |

---

## Documentation

### Package READMEs

- **[docs/ENGINE_ARCHITECTURE_OVERVIEW.md](./docs/ENGINE_ARCHITECTURE_OVERVIEW.md)** - Engine-centric lifecycle architecture and ownership model.
- **[docs/API_USAGE_GUIDE.md](./docs/API_USAGE_GUIDE.md)** - API integration flow: create migration, add roots, traversal, review, copy, logs.
- **[docs/ENGINE_API_CUTOVER.md](./docs/ENGINE_API_CUTOVER.md)** - API cutover contract from legacy corebridge paths.
- **[pkg/db/README.md](./pkg/db/README.md)** – Database layer: schema, buffers, appender, Writer, queries.
- **[pkg/queue/README.md](./pkg/queue/README.md)** – Queue layer: pull/write flow, modes, coordinator, observer.
- **[pkg/migration/README.md](./pkg/migration/README.md)** – Migration orchestration, config YAML, lifecycle, verification.
- **[pkg/configs/README.md](./pkg/configs/README.md)** – JSON config loaders (buffer, log service, Spectra).
- **[pkg/logservice/README.md](./pkg/logservice/README.md)** – UDP and DB logging.

### Testing guides

- **[Ephemeral Mode Guide](./docs/EPHEMERAL_MODE_GUIDE.md)** - Complete integration guide for using Spectra's ephemeral mode for testing migrations without database persistence. Includes configuration examples, performance expectations, and troubleshooting.

- **[Ephemeral Mode Quick Reference](./docs/EPHEMERAL_MODE_QUICK_REF.md)** - Quick reference for ephemeral mode configuration templates and common patterns.

### Testing

The Migration Engine includes test suites:

- **Traversal Tests**: `pkg/tests/traversal/`
  - `normal/` - Standard persistent mode tests
  - `ephemeral/` - Ephemeral mode tests (stateless, large-scale)
  - `resumption/` - Resume interrupted migrations
  - `retry_sweep/` - Retry failed tasks with permission changes

- **Copy Tests**: `pkg/tests/copy/` - File content migration tests

Run tests using the provided shell scripts:
```bash
# Linux/Mac
./run.sh

# Windows
.\run.ps1
```
