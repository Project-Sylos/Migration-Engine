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
  - **`Path`:** DuckDB file for this migration (`{dir}/{id}.db`). Standalone runs set this directly; the HTTP API passes **`MigrationDir`** / **`migrationDir`** into **`CreateMigration`** / **`GetMigration`** instead.
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

- **`NewMigrationManager()`** – Returns a manager that opens **`{migrationDir}/{id}.db`** per migration.
- **`CreateMigration`**, **`GetMigration`**, **`ListMigrations`**, **`GetMigrationDetails`**, **`DeleteMigration`**, **`Close`**.

**`GetMigrationDetails` / `ListMigrations`** merge **`Live`** and **`Phase`** from an in-memory **`Migration`** when this process has loaded it (so `live` matches `running`); otherwise phase comes from the DB row.

---

## Domain `Migration` (`domain.go`, `phase.go`, …)

### Phases (string constants)

Examples: **`roots-set`**, **`filters-set`**, **`traversal-in-progress`**, **`traversal-suspended`**, **`awaiting-traversal-review`**, **`copy-in-progress`**, **`copy-suspended`**, **`awaiting-copy-review`** (`phase.go`).

**Soft suspend:** While phase is **`traversal-in-progress`** or **`copy-in-progress`**, **`Stop()`** requests a **coordinated suspend** (pause queues, drop non-leased pending work, drain in-flight tasks, flush seal/appender buffers, checkpoint, persist **`runtime_state_json.suspend_v1`**). The phase becomes **`traversal-suspended`** or **`copy-suspended`**. **Hard cancel** still uses **`ShutdownContext`** cancellation (abbreviated shutdown, not the full soft path).

**Resume:** **`StartTraversal(cfg)`** from **`traversal-suspended`** reloads **`suspend_v1`** and restarts with **retry-style** queues (round 0, persisted max depth and batch sizing) so the frontier is rebuilt from DuckDB—not from restored in-memory buffers. **`StartCopy(cfg)`** from **`copy-suspended`** restores tuning from **`suspend_v1`**; pending depths and copy passes still come from **DB scans** in **`RunCopyPhase`**. If phase is still **`*-in-progress`** but the run is not live, **`NormalizeDeadInProgressToSuspended()`** moves to the matching suspended phase so API resume can restart. Duplicate resume while **`IsLive()`** is a no-op at the API.

### Common methods

- **`AddRoots`**, **`StartTraversal(cfg)`**, **`StartCopy(cfg)`** – require live **FS adapters** in **`cfg`**; **`UpdateConfig`** persists **`root_config_json`**. **`StartTraversal`** accepts **`filters-set`** or **`traversal-suspended`**. **`StartCopy`** accepts **`awaiting-traversal-review`** or **`copy-suspended`** (and may be called again while phase is **`copy-in-progress`**); **`RunCopyPhase`** rescans pending depths and may enable a one-round dst existence precheck when events show both successful and pending copy work (`copy.go`).
- **`RunRetrySweep(cfg, opts)`**, **`PrepareRetrySweep()`** – For **async** HTTP: call **`PrepareRetrySweep()` synchronously** before returning **202**, then run **`RunRetrySweep`** with the same **`cfg`** shape as traversal (adapters + roots) in a background task. Both accept phase **`awaiting-traversal-review`** or **`traversal-suspended`** (after soft suspend). **`PrepareCopyRetry`** / **`RunCopyRetry`** similarly accept **`awaiting-copy-review`** or **`copy-suspended`**.
- **`RunCopyRetry(cfg, opts)`**, **`PrepareCopyRetry()`** – Same pattern for copy retry when exposed asynchronously.
- **`UpdateConfig(cfg)`** – persists **`root_config_json`** only (serializable fields); callers still pass **`cfg`** with adapters for each run.
- Review helpers: query nodes, path review, exclude, mark retry, etc.
- **`Stop()`** – for live traversal/copy, sets **soft suspend** (see above) and returns **`StopResult.SoftSuspendRequested`**. For other live phases, cancels the run context. **`runtime_state_json`** is updated when the suspend drain finishes (asynchronous relative to **`Stop()`** returning).

---

## Status inspection

**`InspectMigrationStatus(database *db.DB)`** (`status.go`):

- Totals from node tables.
- **Pending / failed** from **status events** (`GetTraversalStatusCountsFromEvents`), not from stats tables alone—so counts stay correct if per-depth stats lag.

---

## Retry sweep (summary)

Engine retry sweep re-processes pending/failed traversal work (**`pkg/queue`** retry mode). DST cleanup on SRC folder completion is described in **`pkg/queue/README.md`**.

**Automated scenario:** **`pkg/tests/traversal/retry_sweep/`** (see **`pkg/tests/README.md`**). **Soft suspend** is covered by unit tests on **`suspend_v1`** merge/parse in **`suspend_state_test.go`**; full stack interrupt tests can extend the same runners with **`Stop()`** during traversal/copy.

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
