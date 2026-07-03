# Autoscaler Design

This document describes how the Migration Engine tunes, observes, and auto-scales **inside the engine**. The Sylos API reports logs and metrics and supports pause/stop lifecycle; it does not control scaling decisions.

**Status:** Autoscaler **v1 is implemented** for traversal (`src` / `dst` queues). Sections marked **(planned)** describe future work not yet wired into the control loop.

Related reading:

- [algorithms.md](./algorithms.md) — BFS traversal, copy passes, completion rules
- [item_statuses.md](./item_statuses.md) — status event semantics
- [fs_error_classification.md](./fs_error_classification.md) — retry vs throttle axes, ambiguous local/FUSE errors (planned)
- `pkg/queue/README.md` — pull / lease / seal flow
- `pkg/db/README.md` — SealBuffer, schema, writes

---

## Goals

1. **Maximize throughput** within external limits (FS rate limits, memory / instability).
2. **Detect bottlenecks** in the sequential pipeline and apply the *right* relief (not all knobs move the same direction).
3. **Guarantee safety** via per-knob lower and upper bounds — mins for liveness, maxes for stability.
4. **Provider-aware defaults** via FS performance profiles in ME, with **authoritative list pagination bounds** read from each connected Sylos-FS adapter at run startup.
5. **Respect host memory headroom** — gate memory-increasing decisions on system/process availability, not only in-engine buffer metrics.

Coordinator gating (DST trailing SRC by two rounds) is **out of scope** for scaling decisions. SRC scales independently; DST scales on its own FS pressure. Coordinator metrics are diagnostic only.

---

## Scope and ownership

**The autoscaler is an engine concern.** It runs inside the Migration Engine process, reads telemetry from queues / backend groups / DuckDB, and adjusts throughput knobs within profile bounds. It does not expose a control API for scaling decisions.

**Sylos API (host) role:**

- **Observability** — logs, `queue_stats`, migration status, metrics the API surfaces to operators.
- **Lifecycle** — pause, stop, soft suspend, resume, phase transitions the product already supports.
- **Not in scope** — telling the autoscaler what worker count, batch size, or profile to use; the API does not drive those decisions.

The engine manages its own scaling loop. Configuration and actuation live in `pkg/migration`, `pkg/scaling`, and `pkg/queue`.

**Out of autoscaler scope:**

- **`MaxRetries`** — fixed run/migration config; the queue’s retry policy handles per-task failures. Retries are not tuned up/down for throughput (and increasing retries under throttle would be counterproductive).
- **Coordinator lead** — fixed DST gating; not a throughput knob.

---

## Current implementation (v1)

This section describes what the autoscaler **does today**. Code lives in `pkg/scaling/` and is wired from `pkg/migration/autoscaler.go` during `RunMigration`.

### Enabling

```go
migration.Config{
    WorkerCount: 10, // initial workers per queue (before autoscaler adjusts)
    Autoscaler: migration.AutoscalerConfig{
        Enabled:  true,
        Interval: 10 * time.Second, // default if zero
        OnEvent:  func(ev scaling.ScalingEvent) { /* optional hook */ },
    },
    // Service.ProviderID selects FS profile ("spectra", "local", "generic")
    // Service.BackendGroupID optional; same Spectra SDK instance auto-merges groups
}
```

When `Enabled` is false (default), no control loop runs. Integration test: `pkg/tests/traversal/autoscaler_throttle/` (`Interval: 3s`, 20 workers, Spectra chaos).

### Control loop (each tick)

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────────────┐
│ Collect signals │ ──► │ Classify         │ ──► │ Actuate (workers/delay) │
│ observer, seal, │     │ one pressure     │     │ AIMD step up/down       │
│ meminfo         │     │ class per tick   │     │ shared or independent   │
└─────────────────┘     └──────────────────┘     └─────────────────────────┘
```

On each tick (`scaling.Autoscaler.tick`):

1. **Snapshot** per-queue `InProgress`, `Pending`, observer internal metrics, seal telemetry (read-and-reset), and memory sample (`MemAvailable` + process RSS from `/proc`, autoscaler tick only).
2. **Classify** → exactly one `PressureClass` (priority order below).
3. **Actuate:**
   - `FS_THROTTLE` or `MEMORY_PRESSURE` → step **down** (workers, `ListPageSize` ↑ on FS throttle, inter-op delay at floor)
   - `UNDERFEED` → step **up** if memory is green (delay recovery first, then workers, `ListPageSize` ↓ toward default)
   - `NONE` → no change

**Actuated knobs:** `WorkerCount`, `InterOpDelayMs`, `ListPageSize` (FS throttle only, when `PreferLargePages`), `LeaseBatchSize`, `RefillBatchSize`, seal buffer `RowThreshold` / `FlushInterval` (memory pressure only).

**Not actuated on FS throttle alone:** batch and seal knobs — FS throttle already reduces workers and inter-op delay.

### Shared vs independent FS backends

When **src and dst wrap the same FS instance** (e.g. same Spectra SDK — detected in `migration.sameBackend` or explicit shared `BackendGroupID`), both queues register in one **backend group**. The autoscaler then:

- Treats **`MaxWorkers` as a combined cap** across src+dst (not per queue).
- **AIMD on total workers**, then **even split** (`SplitWorkersTotal`) — no ±1 rebalance toward underfed queue (intentional v1).
- Applies **inter-op delay uniformly** to all queues in the group at the worker floor.
- Shares one **`FSDegradationState`** so throttle signals from either queue affect the group.

When backends are **different instances** (default `queue:src` / `queue:dst` groups), each queue scales **independently** with its own profile `MaxWorkers` and degradation telemetry.

### Why you may only see scale-down (no additive probe)

Scale-up runs on **`NONE`** (TCP-style calm probe after cooldown) and **`UNDERFEED`** (starvation). For a calm probe on `NONE`, workers must be below the group/profile cap and probe cooldown must have elapsed since the last decrease.

**UNDERFEED** additionally requires all of the following for a queue:

- `TimeWaitingOnQueue > 500ms`
- `InProgress == 0` (no worker mid-task)
- `Pending > 0` (work waiting in the local buffer)

During a throttle-heavy run, ticks often stay on `FS_THROTTLE` — calm probes do not run until rate-limit hits stop for a tick. **Probe cooldown** blocks scale-up for `2 × tick interval` (6s in the integration test) after each decrease.

On eligible calm ticks the additive / slow-start climb:

1. Recover inter-op delay toward 0 (if at worker floor)
2. **Slow start:** double total workers per tick while below `ssthresh`
3. **Congestion avoidance:** `+1` worker per tick at/above `ssthresh`

The sawtooth (up → hit limit → down → up) needs throttle to ease long enough for a `NONE` tick after cooldown. Saturated runs where every tick still reports rate-limit hits will show step-down only until chaos limits relax.

### Pressure classification

Priority (first match wins):

| Class | Trigger (current thresholds) | Autoscaler response |
|-------|------------------------------|---------------------|
| `MEMORY_PRESSURE` | Host RAM use ≥ **90%** (`MemTotal` − `MemAvailable`), **or** seal hard-cap hits since last tick | AIMD worker ↓; batch/seal knobs ↓ |
| `FS_THROTTLE` | Any `RateLimitHitsSinceLastPoll > 0` on src/dst (from FS degradation telemetry) | AIMD worker ↓ (or inter-op delay ↑ at floor); **at most one step-down per retry-after window** |
| `UNDERFEED` | `TimeWaitingOnQueue > 500ms` **and** `InProgress == 0` **and** `Pending > 0` for a queue | AIMD delay ↓ then worker ↑ (memory must be green) |
| `NONE` | Otherwise | **Calm AIMD probe:** worker ↑ (or inter-op delay ↓ at floor) when probe cooldown elapsed and host memory green |

**Explicitly not a trigger:** `SealIOWaitActive()` (slow DuckDB flush). SealBuffer already blocks producers on hard cap; we only scale when that shows up as **memory** signals (HWM / hard-cap hits / host MemAvailable red).

**Scale-up gate:** `ScaleUpAllowed()` requires host RAM use **below 80%** (yellow zone blocks worker/batch scale-up). Process RSS alone does not trigger memory pressure when the host still has headroom.

**Batch/seal AIMD (calm ticks):** On `NONE` pressure, when host use is below 90% and the estimated RAM cost of the next lease/refill/seal increment would stay below 90%, the autoscaler **increases** those knobs toward profile max (inverse of memory-pressure halving).

### FS throttle signal path

1. Sylos-FS adapter hits rate limit → `FSDegradationState.RecordSignal` (Spectra: chaos 429 + retry hooks in `listChildrenWithRetry`).
2. Shared degradation state per backend when src/dst use the same Spectra SDK instance (`migration.sharedDegradationState`).
3. `QueueObserver.RegisterRateLimitTelemetry` → `RateLimitHitsSinceLastPoll` via `TakeRecentHits()` (read-and-reset each tick); `RateLimitedUntil` from shared `FSDegradationState`.

**FS backoff (retry-after sync):** After an FS throttle step-down, further worker/inter-op decreases are blocked until `max(RateLimitedUntil, probe cooldown)` elapses. This avoids stacking multiple halving steps from in-flight requests that hit the same shared adapter rate limit before workers finish sleeping. Calm scale-up probes are also blocked until that window ends.

Other FS adapters need `FSDegradationReporter` wired similarly for autoscaler FS signals to work.

### TCP-style AIMD (`pkg/scaling/aimd.go`)

**Workers** (primary lever):

| Phase | When | Behavior |
|-------|------|----------|
| Multiplicative decrease | `FS_THROTTLE` or `MEMORY_PRESSURE` | `target = floor(cur × 0.5)`, min `MinWorkers` (1); records `ssthresh` |
| Probe cooldown | After any worker decrease | No worker scale-up for `2 × tick interval` |
| Slow start | `NONE` or `UNDERFEED`, `cur < ssthresh` | Double workers per tick toward `ssthresh` / `MaxWorkers` |
| Congestion avoidance | `NONE` or `UNDERFEED`, `cur ≥ ssthresh` | `+1` worker per tick |

**Inter-op delay at worker floor:** Uses the same FS backoff window as worker step-down. First throttle episode seeds delay from throughput (`≈ 2 / opsPerSec`); sustained pressure doubles delay (1.2ms → 2.4ms → 4.8ms). Recovery halves on calm ticks after cooldown; at/below 1ms delay clears to 0 and **the same tick** attempts worker scale-up (1→2 workers).

When `WorkerCount` is already at `MinWorkers` and pressure continues, the same AIMD *shape* applies to **`InterOpDelayMs`** instead:

| Phase | When | Behavior |
|-------|------|----------|
| Increase delay | Pressure at worker floor | Seed from observed **task completion rate** (FS ops/sec, not items discovered): `delay ≈ 2 / opsPerSec`, using peak per-worker rate seen before delay; then double on continued pressure (cap `MaxInterOpDelay`, default 5s) |
| Probe cooldown | After delay increase | No delay decrease for `2 × tick interval` |
| Decrease delay | Calm probe (`NONE` / `UNDERFEED`) while delay &gt; 0 | Halve toward 0; clears when at/below seed |
| Worker scale-up | Only when delay == 0 | Normal AIMD worker increase |

Traversal/copy workers call `Queue.WaitInterOp(ctx)` before FS work. Delay clears automatically when workers scale above the floor.

Tunables via `scaling.Config.AIMD`: `DecreaseFactor` (0.5), `AdditiveStep` (1), `ProbeCooldown`, `InitialInterOpDelay` (1ms fallback when throughput unknown), `MinInterOpDelayStep` (100µs).

### Degraded upscaling detection (planned)

**Problem (v1 gap):** Scale-up today keys only on **`UNDERFEED`** + AIMD cooldown. It does not ask whether adding workers actually improved throughput (observer discovery/copy EMA vs worker count). That can produce a wasteful sawtooth: probe up → no real gain → throttle or flat efficiency → step down → probe up again.

**Wrong fix — permanent freeze:** “Stop scale-up when efficiency is flat” implemented as a one-way latch gets stuck in a **local maximum** after temporary congestion clears (e.g. another tenant released shared quota). Nothing would re-probe; the run would quietly underperform with no throttle events to alarm on. TCP does not “give up and hold” either — it switches from aggressive to cautious probing, but **never stops probing entirely** for the life of the connection.

**Intended fix — second-order AIMD on probe cadence:** Same AIMD *shape*, one level up: tune **probe frequency vs wasted probes**, not worker count vs throttle alone.

| Event | Response (not a freeze) |
|-------|-------------------------|
| **Failed efficiency probe** — workers increased over a window but discovery/copy EMA did not improve meaningfully | Step back toward pre-probe count (or slightly below). **Lower `ssthresh`** so future climbs are cautious sooner. **Increase probe cooldown** (e.g. 2× current interval). |
| **Another failed probe** | Cooldown ratchets again (multiplicative backoff of probe interval). **No infinite ceiling** — even a 10-minute probe interval still eventually retries. |
| **Successful probe** after a long quiet stretch | **Snap probe cooldown back to normal immediately** — recovery must not be slow just because detection was cautious. |
| **Long stability** — no throttle, no failed probes, flat worker count | Optionally **decay `ssthresh` upward slowly** so stale measurements expire; time itself signals the environment may have changed (e.g. shared quota freed). |

**Signals:** `DiscoveryRateItemsPerSec` / copy items·sec⁻¹ / bytes·sec⁻¹ EMA from `QueueObserver` (already computed; not yet used for scale-up gating). Compare rate delta vs worker delta over a probe window before treating the next AIMD step-up as “paid off.”

**Design constraint:** Degraded detection must **reduce probe aggressiveness and stretch probe intervals** — never replace AIMD with a permanent “hold workers forever” state. The alternative failure mode (silent indefinite underperformance) is worse than oscillation.

**Status:** Implemented. Shared groups evaluate probes on **total worker count** and the **active queue's throughput** (idle dst excluded). Flat ceiling-bound throughput fails the probe (no rate gain). FS throttle step-down **aborts in-flight probes** and ratchets cooldown (`failed_probes` increments). Success snaps cooldown; long stability slowly raises `ssthresh`.

### List page size (secondary lever)

On **`FS_THROTTLE`** worker step-down, if the profile has `PreferLargePages` **and** the p95 of recent `ListChildren` result sizes exceeds the current page size, the autoscaler **increases `ListPageSize`** (double toward max, min step 20). Small folders (p95 ≤ page size) skip this knob — raising page size would not reduce list work.

On **`UNDERFEED`** worker step-up, page size **halves toward** `DefaultListPageSize`.

**Bounds source:** Each Sylos-FS adapter that implements `FSListChildrenPagination` exposes `MinPageSize`, `MaxPageSize`, `DefaultPageSize`, and `PreferLargePagesUnderThrottle`. At queue init and autoscaler startup, ME merges these into the profile via `scaling.ApplyAdapterListPagination` (profile map values are fallbacks only when the adapter does not implement the interface).

| Adapter | Min | Default | Max | Prefer large pages |
|---------|-----|---------|-----|--------------------|
| `SpectraFS` | 20 | 100 | 10,000 | yes |
| `LocalFS` | 20 | 100 | 1,000 | no |
| `generic` profile fallback | 20 | 100 | 10,000 | yes |

### FS performance profiles (`pkg/scaling/profile.go`)

Lookup: `Service.ProviderID` → `Service.Name` → `generic`.

| Profile | DefaultWorkers | MaxWorkers (group total if shared) | MaxInterOpDelay | List pages |
|---------|----------------|--------------------------------------|-----------------|------------|
| `generic` | 10 | 32 | 5s | 20–10,000 |
| `spectra` | 10 | 32 | 5s | 20–10,000 |
| `local` | 8 | 64 | 2s | 20–1,000 |

### Backend grouping

`BackendRegistry` resolves queue → group at run start. **Same FS instance → shared worker budget + shared inter-op delay.** Different instances → independent AIMD state per queue.

### Logging and events

Each knob change emits:

- `scaling.ScalingEvent` → optional `AutoscalerConfig.OnEvent`
- Log line: `autoscaler queue=src knob=WorkerCount 20->10 pressure=FS_THROTTLE`

Knobs: `WorkerCount`, `InterOpDelayMs` (milliseconds), `ListPageSize`.

### Dynamic workers

`Queue.SetTargetWorkerCount` spawns workers with per-worker cancel contexts; scale-down cancels excess workers (they finish the current task then exit). See `pkg/queue/queue_scaling.go`.

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

| Signal | Likely cause | Autoscaler acts? |
|--------|----------------|------------------|
| `SealIOWaitActive()` true, in-progress high | DB write / seal flush lag | **No** — SealBuffer blocks producers; only act if memory signals fire |
| Rising task completion time with flat FS metrics | Seal or checkpoint contention | Diagnostic only |
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
| Under-feed | ↑ refill batch | ↑ workers (if FS + memory headroom) | — |

Direction reference (same classes):

| Pressure | Workers | List page size | DB refill / lease batch | Seal buffer |
|----------|---------|----------------|-------------------------|-------------|
| FS rate limit | ↓ | ↑ (if provider supports large pages) | neutral or ↓ burst | neutral |
| Memory | ↓ (3rd) | neutral | ↓ | ↓ threshold / ↑ flush (1st) |
| Under-feed | ↑ cautiously | neutral | ↑ | neutral |

The autoscaler must classify first, then act in priority order. **v1 only actuates workers and inter-op delay** — the lever priority tables below are the **target** for future multi-knob control.

**Before any actuation that increases memory** (workers, batches, seal caps), the loop checks `ScaleUpAllowed()` (host MemAvailable green on Linux).

---

## Tunable knobs

Each knob should have **Min**, **Default**, **Max**, and **Current** (runtime). Min ensures liveness; Max ensures stability. Defaults come from the generic FS profile; provider profiles may tighten Max.

### Per-queue (src, dst, copy — independent)

| Knob | Code today | Default | Suggested min | Suggested max | Notes |
|------|------------|---------|-------------|---------------|-------|
| `WorkerCount` | `migration.Config`, `NewQueue` | 10 (tests) | 1 | profile `MaxWorkers` per **backend group** (or per queue if groups differ) | Primary FS concurrency lever; shared backend → split one cap |
| `LeaseBatchSize` | `queue.QueueSizing` | 1,000 | 100 | 10,000 (code cap) | Sizes `pendingBuff`; drives `PullLowWM` |
| `RefillBatchSize` | `queue.QueueSizing` | 10,000 | 500 | 10,000 | Traversal DB pulls only; retry/copy use lease batch |
| `ListPageSize` | `Queue.SetListPageSize` | 100 | 20 | 10,000 (cloud) | **Autoscaler tunes on FS throttle** (when `PreferLargePages`) |

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

Profiles live in the Migration Engine repo (`pkg/scaling/profile.go`). **List pagination min/max/default** are defined on each Sylos-FS adapter (`pkg/types/listpagination.go`, implemented on `SpectraFS`, `LocalFS`, and future cloud adapters). ME merges adapter limits at FS connect / run startup; profile map list-page fields are fallbacks when an adapter does not implement `FSListChildrenPagination`.

Lookup order:

1. `Service.ProviderID` on the migration service
2. `Service.Name`
3. `generic` fallback

### Implemented shape

```go
type FSPerformanceProfile struct {
    ProviderID string
    MinWorkers, DefaultWorkers, MaxWorkers int
    MinListPageSize, DefaultListPageSize, MaxListPageSize int
    ListPageStep int
    PreferLargePages bool
    DefaultLeaseBatch, MaxLeaseBatch     int
    DefaultRefillBatch, MaxRefillBatch   int
    MaxInterOpDelay time.Duration
}
```

Built-in profiles: `generic`, `spectra`, `local`. When src/dst share one backend, **`MaxWorkers` is the combined cap** for the group. Independent backends: each queue scales within its own profile bounds.

### Provider scope

Real-world targets include **local** (HDD/SSD/NAS), **SFTP**, **S3/blob**, and major **cloud drives** (SharePoint, Google Drive, Dropbox, Box, etc.). Behavior differs widely:

- **Local / NAS** — often high concurrency; limits are drive/OS-bound.
- **Cloud drives** — aggressive API rate limits; inconsistent list APIs.
- **Object storage** — different list/pagination semantics.

**Spectra** is the test harness (not a production target): simulate rate limits, latency, and failures to validate the profile schema and autoscaler before real-provider runs. If profiles are expressive enough for Spectra, they should cover production adapters.

### Spectra `chaos.rate_limits` config (implemented)

Spectra SDK/API middleware uses **fixed poll windows** (default 1s) to count usage, similar to Dropbox-style granular limits:

```json
"chaos": {
  "enabled": true,
  "rate_limits": {
    "poll_interval_ms": 1000,
    "global": { "calls_per_second": 0, "burst": 0 },
    "operations": {
      "list_children": { "calls_per_second": 50000, "burst": 50000 },
      "create_folder": { "calls_per_second": 1000, "burst": 1000 },
      "upload_file": { "calls_per_second": 1000, "burst": 1000 },
      "get_file_data": { "calls_per_second": 1000, "burst": 1000 },
      "get_node": { "calls_per_second": 1000, "burst": 1000 }
    },
    "bandwidth": {
      "bytes_per_second": 1073741824,
      "burst_bytes": 1073741824
    }
  },
  "backoff": {
    "base_retry_after_ms": 100,
    "exponential_factor": 2
  }
}
```

- **global** — optional cap on all API calls per window (`0` disables; per-operation limits still apply)
- **operations** — per-endpoint call caps (`list_children`, `create_folder`, `upload_file`, `get_file_data`, `get_node`); keep **list** high for traversal, **copy/read/write** ops tighter (~1000 calls/sec)
- **bandwidth** — bytes read/written per window (~1 GiB/s default in test configs; writes charged at request time, reads after `GetFileData`)
- **backoff.exponential_factor** — retry-after = `base * factor^attempt` on consecutive rejections (default `2` → 1×, 2×, 4×…; use e.g. `1.5` for gentler penalties or `3`+ for harsher chaos)

Legacy flat `chaos.rate_limit.requests_per_second` still maps to `global` for backward compatibility.

Reference chaos limits (high list, tighter copy/read/write): `pkg/tests/traversal/shared/spectra_ephemeral_throttle.json`.

Autoscaler integration test uses `spectra_ephemeral_autoscaler_throttle.json` — same copy/bandwidth caps, but `list_children` at **3200/sec** (vs 50K in the reference config) so 20 parallel workers emit `FS_THROTTLE`, AIMD reaches the worker floor (1), and inter-op delay fallback can engage if pressure continues.

---

## Backend grouping

**Implemented:** shared budget when src/dst use the same FS instance; independent scaling otherwise. See [Current implementation (v1)](#shared-vs-independent-fs-backends).

Design notes for explicit `BackendGroupID` / connection-based grouping:

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

### Worker budget split (shared backend)

When both queues hit the **same FS instance**, use a **50 / 50** split of the group’s combined `MaxWorkers` cap after each AIMD step on **total** workers:

```
total = src_workers + dst_workers
total_after_aimd = DecreaseTarget(total, ...)  // or IncreaseTarget on UNDERFEED
src, dst = SplitWorkersTotal(total_after_aimd, ["src","dst"], MinWorkers)
```

Clamp each side to at least `MinWorkers` (1). Different backends: each queue has its own cap — no shared total.

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

## FS instance rate limiting (implemented for Spectra)

All workers funnel API calls through their queue’s **FS adapter instance**. Rate-limit signals for the autoscaler come from **Sylos-FS degradation telemetry**, not from parsing queue task errors.

### Current behavior

- Spectra adapter: chaos 429 / `ErrRateLimited` → `FSDegradationState` via retry hooks (`listChildrenWithRetry`, `OnRateLimitWait`).
- Shared state when src/dst adapters wrap the same Spectra SDK instance.
- Observer polls `TakeRecentHits()` each autoscaler tick → `FS_THROTTLE` if any hits since last poll.
- Workers still block on retry-after in Sylos-FS (`DoWithAuthRetry`) — that is reactive; autoscaler step-down is proactive concurrency reduction.

**Gap:** non-Spectra adapters need `FSDegradationReporter` wired per provider (Dropbox, S3, etc.) for `FS_THROTTLE` to fire. Local-path adapters that sit on sync/FUSE layers also need the [ambiguous error classification model](./fs_error_classification.md) — explicit HTTP signals are often unavailable at the syscall boundary.

See [Backend grouping](#backend-grouping-partial) for future combined vs per-backend budgets.

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
| `TimeWaitingOnFS` | partial | Reserved; not primary classifier input |
| `TimeRateLimited` | yes | Populated from FS degradation `RateLimitedUntil` + hit estimates in observer |

Also use:

- `database.SealIOWaitActive()` — seal flush / hard-cap back-pressure (watchdogs + metrics; **not** an autoscaler trigger)
- Task error rates from `task_errors` (and future error classification per [fs_error_classification.md](./fs_error_classification.md): throttle, timeout, permission, ambiguous local-mount)
- [Global system memory watchdog](#global-system-memory-watchdog-planned) — host/process memory vs available

### Instrumentation model (planned)

Pure polling of gauges is not enough — a spike can occur and resolve between ticks. Use **two signal types**:

| Type | Example | Use |
|------|---------|-----|
| **Gauge** (steady-state) | current seal rows, pending count, in-progress | Trend, under/over-feed |
| **Since-last-tick** (counter / HWM) | hard-cap hits, rate-limit events, buffer HWM | Event detection; **reset after observer read** |

Pattern: producers set flags or increment counters when something happens; the control loop **reads then resets** each tick so readings are fresh and non-stale (structured “did anything bad happen since I last checked?”).

**SealBuffer telemetry** (read-and-reset each autoscaler tick):

```go
type SealBufferTelemetry struct {
    CurrentRows              int64
    HWMSinceLastPoll         int64 // triggers MEMORY_PRESSURE if > 30000
    HardCapHitsSinceLastPoll int64 // triggers MEMORY_PRESSURE if > 0
    FlushCountSinceLastPoll  int64
}
```

Same pattern for FS rate-limit events, seal I/O wait episodes, and system memory threshold crossings.

### Knob ↔ signal matrix (target)

v1 actuates **WorkerCount** and **InterOpDelayMs** only. Future knobs:

| Knob | Gauge signals | Event signals |
|------|---------------|---------------|
| `WorkerCount` | in-progress, completion rate, FS latency | rate-limit hits; **system memory headroom low** |
| `RefillBatchSize` / `LeaseBatchSize` | `pendingBuff` depth, DST expected-child map size | seal HWM; **system memory pressure** |
| `ListPageSize` | list call count / task | rate-limit hits |
| `SealBuffer.RowThreshold` | `rowsSinceFlush`, flush latency | hard-cap hits, HWM |
| `SealBuffer.HardCap` | same | hard-cap hits (should be rare) |

### Pressure classification

See [Current implementation (v1)](#current-implementation-v1) — classifier rules, AIMD, and inter-op delay are documented there.

---

## Autoscaler architecture

**v1 (implemented):** observer + seal telemetry + meminfo → classifier → AIMD actuators on src/dst queues.

**Target (not yet wired):**

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

## Global system memory watchdog (partial)

**Implemented:** `SampleMemoryLevel()` reads Linux `MemAvailable` from `/proc/meminfo`:

| Level | Threshold | Effect |
|-------|-----------|--------|
| Green | ≥ 2 GiB | Scale-up allowed |
| Yellow | 512 MiB – 2 GiB | Scale-up blocked |
| Red | &lt; 512 MiB | `MEMORY_PRESSURE` → step down |

Non-Linux hosts: returns green (no gate). Process RSS sampling not implemented.

**Not implemented:** yellow-tier batch reduction, configurable thresholds, cgroup/container awareness, operator logs when scale-up is vetoed.

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
| Autoscaler control loop (traversal src/dst) | **Done** |
| Classifier: FS throttle, memory, underfeed | **Done** |
| TCP AIMD workers + inter-op delay at floor | **Done** |
| Shared backend worker budget (same FS instance) | **Done** |
| List page size on FS throttle | **Done** |
| Adapter `FSListChildrenPagination` bounds at connect | **Done** |
| Error classification (local + Spectra classified retry) | **Partial** |
| Degraded upscaling detection (efficiency-aware probe cadence) | **Done** |
| Copy / retry sweep autoscaler | **Done** |
| BackendRegistry + group split | **Done** |
| Dynamic `SetTargetWorkerCount` | **Done** |
| FS degradation → observer (Spectra) | **Done** |
| Spectra chaos harness | **Done** |
| SealBuffer telemetry → memory pressure | **Done** |
| Memory gate on scale-up (Linux meminfo + RSS) | **Done** |
| FS profiles (`generic`, `spectra`, `local`) | **Done** |
| `BackendRegistry` registration | **Done** |
| Integration test (Spectra throttle) | **Done** |
| Setters: batches, seal opts | **Done** (memory pressure actuation) |
| Local FS classification + inject tests | **Done** |
| Local autoscaler smoke test | **Done** (`pkg/tests/traversal/local_classify/`) |

---

## Remaining work

What v1 does **not** do yet — likely next steps ordered by impact:

### High priority (real-world correctness)

1. **FS degradation on production cloud adapters** — wire Dropbox, SharePoint, S3, etc. using the checklist in [fs_error_classification.md](./fs_error_classification.md) and Sylos-FS `pkg/types/cloud_adapter_contract.go`.

2. **Real FUSE / sync-folder errno tuning** — empirical histogram collection under load; unit injection covers the engine path only.

### Backlog (defer until needed)

3. **Efficiency benchmarking** — Spectra oracle comparison over ~10 min runs.

4. **Pluggable backoff strategies per profile** — jitter for quota-heavy APIs; co-locate backoff state on `BackendGroup`. See [fs_error_classification.md](./fs_error_classification.md).

5. **Configurable thresholds (#5)** — expose AIMD factors, memory cutoffs, underfeed dwell via `migration.AutoscalerConfig`. **Defer unless local/Spectra tuning pain.**

6. **Pressure dwell / hysteresis (#6)** — require throttle/memory pressure to persist N ticks before actuating. **Defer unless classifier flicker observed in tests**; AIMD probe cooldown already limits scale-up churn.

7. **Non-Linux memory sampling (#12)**.

8. **API observability (#13)** — scaling events in Sylos API metrics.

9. **Background quota consumer** (Spectra chaos) — simulate shared-tenant API budget.

### Completed in this phase

- LocalFS classified retry on all I/O paths + injected EIO/EAGAIN tests (Sylos-FS)
- Spectra retry audit (`CreateFolder`, `GetNode`, `UploadFile`)
- Cloud adapter contract documentation
- Memory sampler (MemAvailable + RSS, injectable)
- Batch / seal actuators on `MEMORY_PRESSURE` only
- Local autoscaler smoke test asserting `FS_THROTTLE`
- Shared-backend symmetry documented (#10 — even split, no worker shifting)

### Explicitly out of scope

- `MaxRetries`, coordinator lead, DB-write-only pressure as a scaler trigger
- Migration-wide memory budget splitter (deferred until stress tests justify it)

---

## Open questions

- **DST memory**: separate lower `MaxRefillBatch` for DST than SRC by default?
- **System memory thresholds**: fixed GiB cutoffs vs configurable per deployment?
- **Imbalance rebalance**: shift ±1 worker toward underfed queue within a shared backend group? **Deferred** — v1 uses even split only (see [Shared vs independent FS backends](#shared-vs-independent-fs-backends)).


