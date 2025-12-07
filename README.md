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
   Children that pass filtering are batched through per-queue buffers and flushed to BoltDB before the next leasing pass.
   This guarantees deterministic visibility without keeping additional in-memory round queues.

---

## Why BFS Instead of DFS?

Depth-First Search (DFS) is memory-efficient, but it's less suited to managing two trees in parallel.
BFS, while it requires storing all nodes at the current level, provides better control, checkpointing, and fault recovery.
The Migration Engine (ME) serializes traversal data to BoltDB after each round, keeping memory use bounded while preserving full traversal context.

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
* The destination queue is coordinated to stay at least 3 rounds behind the source via the `QueueCoordinator`.
* When the destination processes its corresponding level, it compares existing nodes against the expected list from the source.
* Extra items in the destination are logged but not traversed further.
* The destination can run as fast as possible while staying coordinated with the source.
* Because each round is batched and stored in BoltDB, the system can resume exactly where it left off after a crash.
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

Migration state is automatically persisted to a YAML config file (default: `{database_path}.yaml`) at key points:

- **Root Selection** - When source and destination roots are set
- **Roots Seeded** - After root tasks are seeded into BoltDB
- **Traversal Started** - When queues are initialized and ready
- **Round Advancement** - When source or destination rounds advance during traversal
- **Traversal Complete** - When migration finishes

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

## Storage Architecture: BoltDB

The Migration Engine uses **BoltDB** as its operational database for storing traversal state, node metadata, and logs.

### Why BoltDB?

BoltDB was chosen for its simplicity, reliability, and perfect fit with the BFS traversal model:

1. **Single-Writer Model** - Guarantees consistent reads immediately after writes; eliminates race conditions
2. **Bucket Hierarchies** - Natural support for organizing data by structure (SRC/DST, levels, status)
3. **Atomic Consistency** - Single B-tree ensures all-or-nothing transaction semantics
4. **Simple Transactions** - Clean, predictable transaction model
5. **Embedded** - No external dependencies or services required
6. **Crash Resilience** - ACID transactions with automatic recovery

### Bucket Structure

The database is partitioned into two main areas:

**Traversal-Data** (all traversal-related data):
```
/Traversal-Data
  /SRC
    /nodes                  → pathHash: NodeState JSON (canonical data)
    /children               → parentHash: []childHash JSON (tree relationships)
    /levels
      /00000000
        /pending            → pathHash: empty (membership set)
        /successful         → pathHash: empty
        /failed             → pathHash: empty
        /status-lookup      → pathHash: status string (reverse index)
      /00000001/...
  /DST
    /nodes
    /children
    /levels
      /00000000
        /pending
        /successful
        /failed
        /not_on_src         → pathHash: empty (DST-specific status)
        /status-lookup      → pathHash: status string (reverse index)
      /00000001/...
  /STATS
    /totals                → bucketPath: int64 (bucket count statistics)
    /queue-stats           → queueKey: QueueObserverMetrics JSON (queue metrics)
```

**LOGS** (separate island, not under Traversal-Data):
```
/LOGS
  /trace                 → uuid: LogEntry JSON
  /debug                 → uuid: LogEntry JSON
  /info                  → uuid: LogEntry JSON
  /warning               → uuid: LogEntry JSON
  /error                 → uuid: LogEntry JSON
  /critical              → uuid: LogEntry JSON
```

This partitioning separates traversal operations (discovery/scanning phase) from future copy operations, allowing the copy phase to have its own data structure under a separate root bucket.

### Key Design Principles

1. **Separation of Concerns**
   - Node data lives in `/nodes` (single source of truth)
   - Status membership tracked in `/levels/{level}/{status}` buckets
   - Status-lookup index in `/levels/{level}/status-lookup` provides reverse lookup (pathHash → status)
   - Tree relationships in `/children` buckets

2. **Status Transitions are Atomic**
   Status transitions update multiple buckets atomically:
   ```go
   // 1. Update NodeState in /nodes bucket
   // 2. Remove from old status bucket
   // 3. Add to new status bucket
   // 4. Update status-lookup index
   ```

3. **Status-Lookup Index**
   - Each level has a `status-lookup` bucket that maps `pathHash → status string`
   - Provides O(1) lookup to find which status bucket a node belongs to
   - Automatically maintained on every insert and status update
   - Enables efficient queries without scanning all status buckets

4. **No Key Encoding**
   - Structure is explicit in buckets, not encoded in key names
   - Path hashes are simple lookup keys
   - Hierarchy is natural and navigable

See `pkg/db/README.md` for detailed documentation on the database layer.
