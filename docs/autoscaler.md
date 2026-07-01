# Autoscaler Design

This document describes how the Migration Engine can be tuned, observed, and (eventually) auto-scaled **inside the engine**. The Sylos API reports logs and metrics and supports pause/stop lifecycle; it does not control scaling decisions. It is a living design note: some sections describe **current behavior**, others describe **planned** work we have not implemented yet.

Related reading:

- [algorithms.md](./algorithms.md) — BFS traversal, copy passes, completion rules
- [item_statuses.md](./item_statuses.md) — status event semantics
- `pkg/queue/README.md` — pull / lease / seal flow
- `pkg/db/README.md` — SealBuffer, schema, writes

---

## Goals

1. **Maximize throughput** within external limits (FS rate limits, memory, DuckDB write capacity).
2. **Detect bottlenecks** in the sequential pipeline and apply the *right* relief (not all knobs move the same direction).
3. **Guarantee safety** via per-knob lower and upper bounds — mins for liveness, maxes for stability.
4. **Provider-aware defaults** via FS performance profiles defined in this repo (not in Sylos-FS backends).
5. **Respect host memory headroom** — gate memory-increasing decisions on system/process availability, not only in-engine buffer metrics.

Coordinator gating (DST trailing SRC by two rounds) is **out of scope** for scaling decisions. SRC scales independently; DST scales on its own FS pressure. Coordinator metrics are diagnostic only.

---

## Scope and ownership

**The autoscaler is an engine concern.** It runs inside the Migration Engine process, reads telemetry from queues / backend groups / DuckDB, and adjusts throughput knobs within profile bounds. It does not expose a control API for scaling decisions.

**Sylos API (host) role:**

- **Observability** — logs, `queue_stats`, migration status, metrics the API surfaces to operators.
- **Lifecycle** — pause, stop, soft suspend, resume, phase transitions the product already supports.
- **Not in scope** — telling the autoscaler what worker count, batch size, or profile to use; the API does not drive those decisions.

The engine manages its own scaling loop. The API may *report* what the engine did (e.g. scaling events in logs or metrics), but configuration and actuation stay in-repo (`pkg/migration`, `pkg/queue`, planned `pkg/scaling` or similar).

**Out of autoscaler scope:**

- **`MaxRetries`** — fixed run/migration config; the queue’s retry policy handles per-task failures. Retries are not tuned up/down for throughput (and increasing retries under throttle would be counterproductive).
- **Coordinator lead** — fixed DST gating; not a throughput knob.

---

## Sequential pipeline

Work flows through two coupled stages. Think of them as sequential pub/sub, though stage A is pull-based rather than push-based.

### Stage A — Frontier consumption (DB → workers)

```
DuckDB  ──pull──►  pendingBuff  ──lease──►  worker  ──►  FS adapter (ListChildren / copy I/O)
                      ▲
                      └── refill when pending ≤ PullLowWM (25% of lease batch)
```

- The queue **pulls** pending tasks from DuckDB in batches (keyset pagination) into `pendingBuff`.
- Workers **lease** one task at a time and call the Sylos-FS adapter.
- Only one pull runs at a time per queue (`getPulling` / `setPulling`).
- **SRC** is the primary throughput driver; refill and worker count on SRC matter most for discovery rate.

**Bottleneck signals (stage A)**

| Signal | Likely cause |
|--------|----------------|
| `pendingBuff` often empty, workers idle | Under-feed: pull too slow, refill batch too small, or workers too few |
| High `InProgress`, low completion rate | FS latency, FS throttle, or stage B back-pressure (workers blocked on seal) |
| Pull loop slow, workers starved | DB read pressure (`pullKeysetWindowSize`, query cost) |

### Stage B — Completion fan-out (workers → DB buffer)

On traversal success (`CompleteTraversalTask`):

1. Parent **status event** (pending → successful / failed) → seal buffer
2. **Discovered children** (node rows + initial pending events) → seal buffer
3. DST may emit additional SRC **copy-status** events per matched child

Copy tasks follow a similar shape (status events; fewer discovery writes). See [algorithms.md](./algorithms.md).

```
worker completes task  ──►  AppendStatusEvent / AppendDiscoveredNodes  ──►  SealBuffer  ──flush──►  DuckDB
```

**Existing back-pressure (stage B)**

- `SealBuffer.HardCap` (default 40k rows): producers block when the buffer is full (`waitBelowHardCapLocked`).
- Workers treat seal I/O as non-stall via `sealIOWaitActive()` in progress watchdogs.
- Round advance may call `FlushSealBuffer` / `WaitUntilSealFlushedThrough` before dropping a level.

**Bottleneck signals (stage B)**

| Signal | Likely cause |
|--------|----------------|
| `SealIOWaitActive()` true, in-progress high | DB write / seal flush pressure |
| Rising task completion time with flat FS metrics | Seal or checkpoint contention |
| Memory growth with large pendingBuff + fat DST batches | Refill batch too large (DST loads expected-child maps per folder) |

### Stage B coupling (workers × buffer × memory)

Total work discovered over a migration is fixed, but **worker count changes how fast completions arrive** at the seal buffer. More workers finishing traversal at once → more concurrent `AppendDiscoveredNodes` / status events → **higher peak buffer depth** before flush drains it.

Rough mental model:

```
peak buffer pressure ≈ f(worker_count, children_per_folder, flush_latency, row_threshold)
```

The seal **hard cap** is a fail-safe (`waitBelowHardCapLocked`); the autoscaler should avoid operating routinely against it. Worker count, batch sizes, and seal flush settings are **coupled** for memory — not independent knobs.

**Migration-wide memory coupling:** Even when SRC and DST use different backends, raising SRC `RefillBatchSize` or worker count can increase **process RSS** (pendingBuff, in-flight task payloads, seal buffer) and affect DST. Worker splits can be mostly per backend group; **buffer and seal knobs** need migration-wide awareness (see [Global system memory watchdog](#global-system-memory-watchdog-planned)).

### Why classification matters

Different pressure classes need **opposite** knob moves. Within a class, actuators have **priority order** (try the targeted lever before the blunt one):

| Pressure | 1st lever | 2nd lever | 3rd lever |
|----------|-----------|-----------|-----------|
| FS rate limit | ↓ workers | ↑ list page size (if profile allows) | ↓ burst batches |
| Memory | ↓ seal `RowThreshold` / flush more aggressively | ↓ refill / lease batch | ↓ workers |
| DB write (seal) | ↑ flush frequency | ↓ batches | ↓ workers |
| Under-feed | ↑ refill batch | ↑ workers (if FS + memory headroom) | — |

Direction reference (same classes):

| Pressure | Workers | List page size | DB refill / lease batch | Seal buffer |
|----------|---------|----------------|-------------------------|-------------|
| FS rate limit | ↓ | ↑ (if provider supports large pages) | neutral or ↓ burst | neutral |
| Memory | ↓ (3rd) | neutral | ↓ | ↓ threshold / ↑ flush (1st) |
| DB write (seal) | ↓ (3rd) | neutral | ↓ | ↑ flush frequency (1st) |
| Under-feed | ↑ cautiously | neutral | ↑ | neutral |

The autoscaler must classify first, then act in priority order. **Before any actuation that increases memory** (workers, batches, seal caps), consult the [global system memory watchdog](#global-system-memory-watchdog-planned).

---

## Tunable knobs

Each knob should have **Min**, **Default**, **Max**, and **Current** (runtime). Min ensures liveness; Max ensures stability. Defaults come from the generic FS profile; provider profiles may tighten Max.

### Per-queue (src, dst, copy — independent)

| Knob | Code today | Default | Suggested min | Suggested max | Notes |
|------|------------|---------|-------------|---------------|-------|
| `WorkerCount` | `migration.Config`, `NewQueue` | 10 (tests) | 1 | profile `MaxWorkers` per **backend group** (or per queue if groups differ) | Primary FS concurrency lever; shared backend → split one cap |
| `LeaseBatchSize` | `queue.QueueSizing` | 1,000 | 100 | 10,000 (code cap) | Sizes `pendingBuff`; drives `PullLowWM` |
| `RefillBatchSize` | `queue.QueueSizing` | 10,000 | 500 | 10,000 | Traversal DB pulls only; retry/copy use lease batch |
| `ListPageSize` | hardcoded `100` in workers | 100 | 50 | profile max | FS API pages per `ListChildren`; **not yet configurable** |

**Exposure today**

- `WorkerCount`: top-level `migration.Config`, persisted in `root_config_json`, restored from `suspend_v1`; **autoscaler may adjust** within bounds.
- `LeaseBatchSize`, `RefillBatchSize`: `queue.QueueSizing`, `SweepConfig`, `suspend_v1` — **not** on normal traversal `Config` yet; **autoscaler may adjust**.
- `MaxRetries`: `migration.Config` / `suspend_v1` — **not an autoscaler knob**; set at run start, unchanged by the scaling loop.
- `CoordinatorLead` in config is **not wired** to `QueueCoordinator` (DST gate is hardcoded `targetRound + 2`).

### Global / DB (shared)

| Knob | Code today | Default | Suggested min | Suggested max | Notes |
|------|------------|---------|-------------|---------------|-------|
| `SealBuffer.HardCap` | `SealBufferOptions` | 40,000 rows | 5,000 | 40,000+ | Producer back-pressure tripwire |
| `SealBuffer.RowThreshold` | `SealBufferOptions` | 20,000 | 1,000 | 50,000 | Discovery flush burst |
| `SealBuffer.FlushInterval` | `SealBufferOptions` | 10s | 1s | 30s | Background flush cadence |
| `SealBuffer.CheckpointEveryRows` | `SealBufferOptions` | 100,000 | — | — | Periodic checkpoint |
| DuckDB `memory_limit` | `db.Open` PRAGMA | 4GB | — | host-dependent | Hardcoded today |
| DuckDB `threads` | `db.Open` PRAGMA | 4 | 1 | 8 | Hardcoded today |
| `ObserverPollInterval` | `MigrationConfig` | 200ms | 100ms | 2s | Control-loop freshness vs DB load |
| `ProgressTick` | `migration.Config` | 1s | — | — | Console progress only |

### Internal (not autoscaler actuators today)

| Constant | Value | Package | Role |
|----------|-------|---------|------|
| `pullKeysetWindowSize` | 50,000 | `pkg/db` | SQL scan window per pull round-trip |
| `defaultCopyBufferSize` | 64 KiB | `pkg/queue` | Per-worker file copy stream buffer |
| Log buffer batch | 50,000 / 3s | `pkg/logservice` | Secondary DB write load |

---

## FS performance profiles

Profiles live **in the Migration Engine repo** as a provider dictionary — not inside Sylos-FS backends. Sylos-FS implements adapters; the engine applies policy.

Lookup order (planned):

1. Explicit `ProviderID` on the migration service (host-set)
2. Service name / adapter type
3. `generic` fallback

### Planned shape

```go
type FSPerformanceProfile struct {
    ProviderID string // "generic", "spectra", "s3", "local", ...

    // Concurrency
    MinWorkers, DefaultWorkers, MaxWorkers int

    // Relative throughput vs other backends (used when SRC/DST are different BackendGroupIDs).
    // Ratio for initial worker split — see "SpeedScore calibration". May converge to MaxSafeWorkers.
    SpeedScore int

    // Empirical ceiling: max parallel workers before throttle or instability (benchmark-derived).
    MaxSafeWorkers int

    // List / traversal
    DefaultListPageSize, MaxListPageSize int
    PreferLargePages bool // if false, autoscaler won't grow page size under throttle

    // Rate-limit response (works with FS instance retry-after)
    WorkerStepDownOnThrottle int
    BackoffOnThrottle        time.Duration

    // Queue defaults (starting point before autoscaler adjusts)
    DefaultLeaseBatch, MaxLeaseBatch   int
    DefaultRefillBatch, MaxRefillBatch int

    // Copy
    DefaultCopyStreamBuffer int
    MaxCopyWorkers          int
}
```

Profiles set **starting defaults and bounds**. The autoscaler adjusts `Current` within those bounds at runtime.

When queues share a backend (see below), profile `MaxWorkers` applies to the **group total**, not per queue.

`SpeedScore` is used only when SRC and DST are **different** backends — see [Worker budget split](#worker-budget-split).

### SpeedScore calibration (planned)

**Today:** `SpeedScore` is a **placeholder ratio** for SRC/DST worker split — not yet tied to benchmarks.

**Target:** populate profiles empirically (Spectra first, then real providers). Options:

| Approach | Use when |
|----------|----------|
| **`MaxSafeWorkers` only** | Split weight = each side’s measured safe concurrency; simplest and honest |
| **Compound score** | e.g. `MaxSafeWorkers × (SustainedListRate / BaselineRate)` with a fixed baseline constant |
| **`SpeedScore` as stored ratio** | Hand-tuned or derived from the above; must document scale (e.g. “3 vs 1 means 3× relative throughput”) |

Benchmark inputs per provider (via Spectra chaos or real runs): sustained list throughput, max concurrency before retry-after, observed retry rate at various worker counts.

### Provider scope

Real-world targets include **local** (HDD/SSD/NAS), **SFTP**, **S3/blob**, and major **cloud drives** (SharePoint, Google Drive, Dropbox, Box, etc.). Behavior differs widely:

- **Local / NAS** — often high concurrency; limits are drive/OS-bound.
- **Cloud drives** — aggressive API rate limits; inconsistent list APIs.
- **Object storage** — different list/pagination semantics.

**Spectra** is the test harness (not a production target): simulate rate limits, latency, and failures to validate the profile schema and autoscaler before real-provider runs. If profiles are expressive enough for Spectra, they should cover production adapters.

---

## Backend grouping (planned)

Rate limits and FS concurrency apply to a **backend** (API account, Spectra instance, connection), not to “SRC” or “DST” as abstract roles. Migrations often use **two different backends** (source S3, destination GCS) — but sometimes both sides hit the **same** backend. The autoscaler must handle both without hard-coding “always combine SRC/DST.”

### Model

Introduce a **`BackendGroupID`** — a stable key that identifies one rate-limit / concurrency pool. Each queue **registers** against a group at run start:

```
BackendGroup "s3-prod"              BackendGroup "gcs-dest"
  ├─ FS instance (rate limit)         ├─ FS instance
  ├─ combined worker budget           ├─ own worker budget
  ├─ combined throttle signals        └─ dst queue only
  ├─ src queue (N workers)
  └─ dst queue (M workers)
```

**Same `BackendGroupID`** → one shared budget, shared FS signals, one throttle narrative.  
**Different IDs** → independent budgets and autoscaler lanes for FS knobs.

Seal / DuckDB pressure remains **migration-wide** (one DB per run), regardless of backend grouping.

### Resolving the group ID

Two sides share a budget **iff** they resolve to the same `BackendGroupID`. Suggested sources (first match wins):

| Source | Use |
|--------|-----|
| Explicit `BackendGroupID` on `migration.Service` (host-set) | Preferred — unambiguous |
| `ConnectionID` from `fs_credential_binding` | Same stored connection → same group |
| Provider hints (e.g. Spectra `shared_instance: true`) | Auto-assign one group for both sides |
| *(fallback)* | Each queue gets its own implicit group (per-adapter / per-side) |

Do not rely on pointer equality alone (breaks across restarts). Default when unset: **separate groups** — matches the common two-backend case with no extra config.

### What lives on `BackendGroup` (runtime)

Per group, not per queue:

- FS instance rate-limit state (retry-after, recent hit count)
- Optional API call counters
- **Combined** FS pressure signals for the classifier
- Profile **`MaxWorkers`** as a **total cap** for this backend
- **Current allocation**: e.g. `{ src: 12, dst: 4 }` where `src + dst ≤ MaxWorkers`

Queues still **own** their worker goroutines; the group owns the **budget and throttle telemetry**.

### Worker budget split

How worker counts are divided depends on whether SRC and DST share one backend.

#### Same FS instance (`BackendGroupID` shared)

When both queues hit the **exact same** backend instance, use a fixed **50 / 50** split of the group’s combined `MaxWorkers` cap:

```
src_workers = round(total * 0.5)
dst_workers = total - src_workers   // handles odd totals
```

Clamp each side to profile `MinWorkers` (at least 1 per active queue). No `SpeedScore` weighting in this case — both sides compete for the same rate-limit pool equally.

The autoscaler may still **rebalance ±1 worker** on imbalance signals (e.g. DST `TimeWaitingOnQueue`), but the **baseline** split is 50/50.

#### Different backends (separate `BackendGroupID`)

Each side has its own profile and **`MaxWorkers`** cap. There is **no shared worker pool** across groups — SRC and DST scale independently within their own limits.

**Initial worker counts** use **`SpeedScore`** or **`MaxSafeWorkers`** as the split weight (same formula):

```
src_workers = round(default_total_src * src_weight / (src_weight + dst_weight))   // conceptual; each side clamped to its own MaxWorkers
```

In practice: set **starting** workers from each profile’s `DefaultWorkers`, capped by `MaxWorkers`. Use scores only to set the **relative ratio** between sides at migration start (e.g. `src_score=3`, `dst_score=1` → start at 75%/25% of each side’s `DefaultWorkers`, not a single migration-wide `total`).

Example: SRC profile `DefaultWorkers=16`, `SpeedScore=3`; DST `DefaultWorkers=8`, `SpeedScore=1` → initial targets ~12 SRC, ~2 DST (then clamp to mins/maxes per profile).

The autoscaler adjusts each queue within its own profile bounds afterward.

### Worker allocation (runtime)

Workers are **not moved** between queues. Scaling changes **counts** on each side:

- **Scale down SRC**: workers finish their task and exit; cap 12 → 10.
- **Scale up DST**: start new worker goroutines; cap 4 → 6.

**On group throttle** (shared backend): step down **total** workers first, then re-apply 50/50 (or current split).

**On imbalance** (either topology): shift 1–2 workers toward the queue that is waiting, within mins/maxes.

**Note:** Workers are created at queue init today (`InitializeWithContext`). Dynamic rebalance requires **scale-up/down hooks** on the queue (start/stop goroutines safely) — separate from grouping but required for live allocation.

### Signals: combined vs separate

| Signal | Same `BackendGroupID` | Different groups |
|--------|----------------------|------------------|
| Rate-limit / retry-after | One stream on the group | Per adapter / per group |
| `TimeRateLimited` (observer) | Attribute to group (and per-queue for display) | Per queue |
| Autoscaler FS step-down | One decision → re-split workers | Independent per queue |
| Seal / memory / DB | Migration-wide | Migration-wide |

### Registry (planned shape)

Resolved once at run start; keeps both cases uniform:

```go
type BackendRegistry struct {
    groups map[string]*BackendGroup // BackendGroupID → shared state
}

// Each queue registers: name, BackendGroupID, FSPerformanceProfile
// Autoscaler: registry.ForQueue("src") or registry.Budget("s3-prod")
```

| Topology | Behavior |
|----------|----------|
| 1 group, 1 queue | Same as today (e.g. copy-only) |
| 1 group, 2 queues | Combined cap + split allocation |
| 2 groups, 2 queues | Independent FS autoscaler lanes |

---

## FS instance rate limiting (planned)

All workers funnel API calls through their queue’s **FS adapter instance**. Rate limiting is enforced on that instance (backed by the **backend group** when IDs match). Limits are not per-worker.

### Planned behavior

- FS instance exposes something like: **“we are getting rate limited”** → returns **retry-after duration** (`time.Duration` or seconds as `int`).
- The instance may also track API call volume for proactive throttling.
- **Back-pressure from the FS layer** is primarily **retry-after waits** — workers block or sleep until the window passes.
- This is a **fail-safe**, not the primary scaler. Hitting it repeatedly means we are too aggressive.

### Observer / autoscaler signal

When the FS rate-limit fail-safe fires:

1. **Notify** the backend group (and queue observer / scaling event channel).
2. Attribute time to `InternalQueueMetrics.TimeRateLimited` (field exists today but is **not populated**).
3. Autoscaler **steps down** total workers for that group (then re-split), and optionally list page size / batches per pressure table.
4. **Do not** rely on hammering retry-after in a loop — repeated hits are a dial-back signal.

See [Backend grouping](#backend-grouping-planned) for combined vs per-backend budgets.

---

## Observer and telemetry

`QueueObserver` (`pkg/queue/observer.go`) polls queues on an interval (default 200ms) and writes metrics to `queue_stats`.

### Published (external) metrics

- Discovery rate (EMA, α = 0.2), copy `items/sec`, `bytes/sec`
- Pending / failed totals, round, in-progress
- Per-queue `QueueStats`

### Internal metrics (in-memory, for scaling)

| Field | Populated today | Meaning |
|-------|-----------------|---------|
| `TimeProcessing` | yes | Workers actively executing |
| `TimeWaitingOnQueue` | yes | Pending work but nothing in-flight |
| `TimePausedRoundBoundary` | yes | Coordinator wait, manual pause |
| `TimeIdleNoWork` | yes | No pending, no in-progress |
| `TimeWaitingOnFS` | **no** | Reserved: blocked on FS I/O |
| `TimeRateLimited` | **no** | Reserved: FS retry-after fail-safe |

Also use:

- `database.SealIOWaitActive()` — seal flush / hard-cap back-pressure
- Task error rates from `task_errors` (and future error classification: throttle, timeout, permission)
- [Global system memory watchdog](#global-system-memory-watchdog-planned) — host/process memory vs available

### Instrumentation model (planned)

Pure polling of gauges is not enough — a spike can occur and resolve between ticks. Use **two signal types**:

| Type | Example | Use |
|------|---------|-----|
| **Gauge** (steady-state) | current seal rows, pending count, in-progress | Trend, under/over-feed |
| **Since-last-tick** (counter / HWM) | hard-cap hits, rate-limit events, buffer HWM | Event detection; **reset after observer read** |

Pattern: producers set flags or increment counters when something happens; the control loop **reads then resets** each tick so readings are fresh and non-stale (structured “did anything bad happen since I last checked?”).

**Planned SealBuffer telemetry** (internal fields exist today but are **not exported** to the observer):

```go
type SealBufferTelemetry struct {
    CurrentRows              int64
    HWMSinceLastPoll         int64 // high-water mark between flushes
    HardCapHitsSinceLastPoll int64 // times producers blocked on hard cap
    FlushCountSinceLastPoll  int64
}
```

Same pattern for FS rate-limit events, seal I/O wait episodes, and system memory threshold crossings.

### Knob ↔ signal matrix (planned)

Each autoscaler knob needs at least one gauge and one event signal where applicable:

| Knob | Gauge signals | Event signals |
|------|---------------|---------------|
| `WorkerCount` | in-progress, completion rate, FS latency | rate-limit hits; **system memory headroom low** |
| `RefillBatchSize` / `LeaseBatchSize` | `pendingBuff` depth, DST expected-child map size | seal HWM; **system memory pressure** |
| `ListPageSize` | list call count / task | rate-limit hits |
| `SealBuffer.RowThreshold` | `rowsSinceFlush`, flush latency | hard-cap hits, HWM |
| `SealBuffer.HardCap` | same | hard-cap hits (should be rare) |

### Pressure classification (planned control loop)

Slow loop (e.g. every 5–30s). **Hysteresis and cooldown** to avoid oscillation:

```
if sealIOWaitActive && inProgress high     → DB_WRITE_PRESSURE
if rateLimitEvent or classified 429        → FS_THROTTLE
if waitingOnQueue && low inProgress        → UNDERFEED
if memory high (seal HWM, RSS, or system)  → MEMORY_PRESSURE
```

**Control-loop rules (planned):**

- **Minimum dwell time** per pressure class before switching to another (e.g. remain in `FS_THROTTLE` for N ticks after last rate-limit event).
- **Cooldown** after actuation before stepping the opposite direction (do not step down on throttle and step up on under-feed in the same window).
- **Priority when multiple classes fire:** safety first — `MEMORY_PRESSURE` and `DB_WRITE_PRESSURE` before `UNDERFEED`.
- **Max one knob change** per lane per tick (optional, reduces fighting).
- **Gate scale-up:** any increase to workers, batches, or seal caps requires [system memory headroom](#global-system-memory-watchdog-planned) OK.

One actuator step per class per interval; clamp all changes to knob bounds.

---

## Autoscaler architecture (planned)

```
┌──────────────────┐     ┌──────────────┐     ┌─────────────────┐
│ QueueObserver    │────►│ Classifier   │────►│ Actuator        │
│ BackendGroup     │     │ (pressure    │     │ (clamp to       │
│ SealBuffer telem │     │  class)      │     │  profile bounds)│
│ FS rate-limit    │     └──────▲───────┘     └────────┬────────┘
│ System mem watch │            │ gate scale-up          │
└──────────────────┘            └────────────────────────┘
                                            Per-group worker split,
                                            per-queue batches, seal opts
```

The classifier reads **backend group** state for FS throttle (combined when SRC/DST share an ID) and **migration-wide** state for seal/memory. The **system memory watchdog** gates any actuation that increases memory use.

### Actuator targets

**Per backend group (FS concurrency)**

- Total `MaxWorkers` cap; allocation `{ src, dst, copy }` sums ≤ cap when queues share the group
- On throttle: step down total, then adjust split

**Per queue lane (buffers & pagination)**

- **SRC traversal / retry** — `RefillBatchSize`, `LeaseBatchSize`, `ListPageSize`, worker slice of group budget
- **DST traversal / retry** — same; watch memory on large DST pulls + expected-child maps
- **Copy** — worker slice, copy stream buffer; two passes (folders, files) stay sequential at phase level

### Apply strategy

| Approach | When |
|----------|------|
| **Live adjust** | Requires new setters on queue / DB / workers; best UX |
| **Apply on soft suspend / phase boundary** | Matches today’s `suspend_v1` persistence; easier v1 |

For v1, log scaling decisions continuously; apply on resume or phase boundary unless live hooks exist.

---

## Global system memory watchdog (planned)

Before any autoscaler decision that **increases memory use** — raising `WorkerCount`, `RefillBatchSize`, `LeaseBatchSize`, seal `HardCap` / `RowThreshold`, or similar — the engine must check **host/process memory headroom**, not only in-engine buffer metrics.

**Rationale:** DuckDB `memory_limit`, seal buffer caps, and `pendingBuff` bound *engine* structures, but the migration process shares the machine with the OS page cache, other services, and FS client buffers. Increasing throughput knobs without checking **available system memory** risks OOM or swap thrashing even when internal caps look fine.

**Planned behavior:**

- Sample **process RSS** (and optionally **system available memory**) on the same tick as the autoscaler / observer.
- Define thresholds, e.g.:
  - **Green** — headroom OK; scale-up allowed.
  - **Yellow** — cautious; no scale-up; optional small scale-down of batches.
  - **Red** — memory pressure; prefer seal flush tuning, then batch reduction, then workers (per priority table).
- **Block scale-up** when below green threshold, even if `UNDERFEED` or FS profile would allow more workers.
- Emit telemetry / logs when scale-up is vetoed due to memory (operator-visible).

This is a **global gate** on top of per-component signals (seal HWM, hard-cap hits). It does not replace seal buffer back-pressure; it prevents aggressive scale-up when the machine is already tight.

Implementation options: read `/proc/self/status` or equivalent on Linux; portable abstraction for other OSes; optional cgroup limit awareness when running containerized.

---

## Migration-wide memory budget (deferred)

Worker budgets can be **per backend group**, but **buffer memory** is coupled across SRC/DST/copy in one process:

- `pendingBuff` × refill/lease batch size (DST pulls carry large expected-child maps)
- Seal buffer peak between flushes
- Per-worker copy stream buffers

**Deferred v1:** explicit `GlobalMemoryBudget` split (e.g. 60% SRC refill cap / 40% DST) — add if Spectra stress tests show cross-queue memory contention at target scale.

**v1 safety valve:** system memory watchdog (above) + seal HWM / hard-cap hit counters + step-down on `MEMORY_PRESSURE` without a full memory allocator.

Typical enterprise folder depths may not require this split; validate under Spectra at high fan-out before building a full budget splitter.

---

## Validation path (planned)

1. **Spectra** — rate limits, latency injection, profile schema, autoscaler control loop, instrumentation counters.
2. **Synthetic load** — wide folders, fixed children-per-folder, measure seal spike amplitude vs worker count and flush thresholds.
3. **Real providers** — local/NAS, then top cloud targets (SharePoint, Google Drive, Dropbox, Box, S3), after Spectra passes.

Spectra lays groundwork for FS struct / performance profile shape; real backends are “Spectra with less predictable chaos.”

---

## Implementation status

| Item | Status |
|------|--------|
| DB pull → `pendingBuff` → worker lease | **Implemented** |
| Worker → SealBuffer → DuckDB | **Implemented** |
| Seal hard-cap back-pressure | **Implemented** |
| `QueueSizing` / `suspend_v1` batch persistence | **Implemented** |
| `InternalQueueMetrics` time buckets | **Partial** (`TimeRateLimited`, `TimeWaitingOnFS` unused) |
| SealBuffer telemetry export (HWM, hard-cap hits) | **Planned** — required for memory spike detection |
| Global system memory watchdog | **Planned** — gates memory-increasing scale-up |
| FS instance retry-after rate limit API | **Planned** — **`FS_THROTTLE` classification blocked until this exists** |
| `BackendGroupID` + `BackendRegistry` | **Planned** |
| Combined worker budget + queue split | **Planned** |
| Dynamic queue worker scale-up/down | **Planned** (required for live rebalance) |
| FS performance profile map | **Planned** |
| Knob bounds types + clamping | **Planned** |
| Autoscaler control loop | **Planned** |
| `ListPageSize` configurable | **Planned** |
| `SealBufferOptions` (engine-internal tuning) | **Planned** |
| `CoordinatorLead` configurable | **Not planned** for autoscaler (gate stays as-is) |
| Live hot-reload of all knobs mid-run | **Planned** (later) |

---

## Open questions

- **Error taxonomy**: which adapter errors count as throttle vs transient vs fatal?
- **DST memory**: separate lower `MaxRefillBatch` for DST than SRC by default?
- **SpeedScore vs `MaxSafeWorkers`**: consolidate to one split field after Spectra benchmarks?
- **System memory thresholds**: fixed percentages vs configurable per deployment (containers vs bare metal)?

---

## Changelog

| Date | Notes |
|------|-------|
| 2026-06-08 | Initial design doc from pipeline analysis and scaling discussion |
| 2026-06-08 | Backend grouping: `BackendGroupID`, combined budget when shared, per-backend when not |
| 2026-06-08 | Scope: engine-owned autoscaler; API observability/lifecycle only; `MaxRetries` excluded |
| 2026-06-08 | Worker split: 50/50 same instance; score-based ratio when backends differ |
| 2026-06-08 | Stage B coupling, instrumentation, actuator priority, hysteresis, SpeedScore calibration, validation path, global system memory watchdog |
