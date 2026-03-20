# Migration Package

Orchestrates migrations: **`MigrationManager`**, domain type **`Migration`**, DuckDB (**`pkg/db`**), and queues (**`pkg/queue`**).

Higher-level **API / HTTP integration** (routes, background tasks, runtime cache) lives in the Sylos application that imports this module; this package is the **engine** surface.

---

## Documentation in this repository

- **`docs/algorithms.md`**, **`docs/item_statuses.md`** – Supplemental reference.
- **`pkg/db/README.md`**, **`pkg/queue/README.md`** – Persistence and queue behavior.

> Note: Some older docs referred to `docs/ENGINE_ARCHITECTURE_OVERVIEW.md` and similar files. Those paths are **not** present in this repo; they may live in another Sylos repository or be retired—prefer the package READMEs above.

---

## Config and entry points

### `Config` (`migration.go`)

Used by **`LetsMigrate`** / **`StartMigration`**:

- **`Database`** – **`DatabaseConfig`** (`Path`, `RemoveExisting`, `RequireOpen`).  
  - **Non-empty `Path`:** legacy **single** DuckDB file (all migrations in that DB’s `migrations` table, if used).  
  - **Empty `Path`:** **per-migration** DB files; the host passes **`MigrationDir`** / **`migrationDir`** into **`CreateMigration`** / **`GetMigration`** (see **`manager.go`**).
- **`Source`**, **`Destination`** – **`Service`** with **`types.FSAdapter`** and **`types.Folder`**. **Adapters must be non-nil**; the engine does not construct or close them.
- **`SeedRoots`**, **`WorkerCount`**, **`MaxRetries`**, **`CoordinatorLead`**, **`LogAddress`**, **`LogLevel`**, **`SkipListener`**, **`StartupDelay`**, **`ProgressTick`**, **`Verification`**, **`ShutdownContext`**.

### `LetsMigrate(cfg)`

1. Builds **`MigrationManager`** from **`cfg.Database`**.
2. **`CreateMigration`** with metadata derived from config.
3. If **`SeedRoots`**, seeds root tasks into the migration DB.
4. **`StartTraversal(cfg)`** – runs traversal (and does not run copy in this path; see domain methods for copy).
5. Runs **`VerifyMigration`** unless suspended by shutdown.

Blocks until completion, error, or shutdown. Handles SIGINT/SIGTERM when **`ShutdownContext`** is not preset.

### `StartMigration(cfg)`

Runs **`LetsMigrate`** in a goroutine. **`MigrationController`** exposes only:

- **`Shutdown()`** – cancel shutdown context  
- **`Done() <-chan struct{}`**  
- **`Wait() (Result, error)`**

There is **no** `GetDB()` / `Result()` / `Error()` on the controller in the current API.

---

## `MigrationManager` (`manager.go`)

- **`NewMigrationManager(DatabaseConfig)`** – Opens legacy DB when `Path` is set; otherwise returns a manager that opens **`{dir}/{id}.db`** per migration.
- **`CreateMigration`**, **`GetMigration`**, **`ListMigrations`**, **`GetMigrationDetails`**, **`DeleteMigration`**, **`Close`**.

**`GetMigrationDetails` / `ListMigrations`** merge **`Live`** and **`Phase`** from an in-memory **`Migration`** when this process has loaded it (so `live` matches `running`); otherwise phase comes from the DB row.

---

## Domain `Migration` (`domain.go`, `phase.go`, …)

### Phases (string constants)

Examples: **`roots-set`**, **`filters-set`**, **`traversal-in-progress`**, **`awaiting-traversal-review`**, **`copy-in-progress`**, **`awaiting-copy-review`** (`phase.go`).

### Common methods

- **`AddRoots`**, **`StartTraversal`**, **`StartCopy`**
- **`RunRetrySweep`**, **`PrepareRetrySweep()`** – For **async** HTTP: call **`PrepareRetrySweep()` synchronously** before returning **202**, then run **`RunRetrySweep`** in a background task so polls see **`traversal-in-progress`** immediately.
- **`RunCopyRetry`**, **`PrepareCopyRetry()`** – Same pattern for copy retry when exposed asynchronously.
- Review helpers: query nodes, path review, exclude, mark retry, etc.
- **`Stop()`** – stop result with current phase / runtime snapshot.

**`RunMigration`** / **`RunCopyPhase`** configuration type **`MigrationConfig`** (`run.go`, `copy.go`) includes **`MaxSrcAhead`** (queue default **2** when unset).

---

## Status inspection

**`InspectMigrationStatus(database *db.DB)`** (`status.go`):

- Totals from node tables.
- **Pending / failed** from **status events** (`GetTraversalStatusCountsFromEvents`), not from stats tables alone—so counts stay correct if per-depth stats lag.

---

## Retry sweep (summary)

Engine retry sweep re-processes pending/failed traversal work (**`pkg/queue`** retry mode). DST cleanup on SRC folder completion is described in **`pkg/queue/README.md`**.

**Automated scenario:** **`pkg/tests/traversal/retry_sweep/`** (see **`pkg/tests/README.md`**).

---

## `SetupDatabase` (`database.go`)

```go
database, wasFresh, err := migration.SetupDatabase(migration.DatabaseConfig{
    Path:           "migration.duckdb",
    RemoveExisting: false,
})
```

Opens DuckDB with default seal-buffer options and ensures node + status-event indexes. Caller closes the DB when appropriate.

---

## Verification

**`VerifyMigration(database, VerifyOptions)`** – reads node/event-derived state and produces **`VerificationReport`**.

---

## Best practices

1. **Own adapters** in the host process; keep them open for the duration of a run.
2. **Async retry sweep / copy retry:** **`Prepare*`** then background **`Run*`** (see above).
3. **Per-migration DBs:** pass consistent **`migrationDir`** when loading details/listing so the manager can open the right file.

---

## Examples

- **`main.go`** (repo root) – CLI-style **`LetsMigrate`**.
- **`pkg/tests/traversal/*`**, **`pkg/tests/copy/*`** – Spectra-backed runners.
