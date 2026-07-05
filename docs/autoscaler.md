# Autoscaler Design

This document explains how the Migration Engine observes load, classifies pressure, and adjusts throughput **inside the engine**. The Sylos API reports logs and metrics and supports pause/stop lifecycle; it does not drive scaling decisions.

The autoscaler runs during traversal (`src` / `dst`), copy, and retry sweeps unless opted out via `DisableAutoscaler`. Code lives in `pkg/scaling/` and is wired from `pkg/migration/autoscaler.go`.

Related reading:

- [algorithms.md](./algorithms.md) — BFS traversal, copy passes, completion rules
- [item_statuses.md](./item_statuses.md) — status event semantics
- [fs_error_classification.md](./fs_error_classification.md) — retry vs throttle axes, ambiguous local/FUSE errors
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

## How it works

This section walks through the control loop as it exists today.

### Enabling

```go
migration.Config{
    WorkerCount: 10, // initial workers per queue (before autoscaler adjusts)
    ObserverPollInterval: 200 * time.Millisecond, // default if zero; metrics sampling
    Autoscaler: migration.AutoscalerConfig{
        Interval: 3 * time.Second, // default if zero (migration.AutoscalerConfig.Resolve)
        OnEvent:  func(ev scaling.ScalingEvent) { /* optional hook */ },
        DebugAIMD: true, // or ME_AUTOSCALER_DEBUG_AIMD=1 for probe diagnostics
        // DisableAutoscaler: true, // opt out
    },
    SrcService: Service{ProviderID: "local"}, // selects operation profiles
    DstService: Service{ProviderID: "spectra", BackendGroupID: "..."}, // optional shared group
}
```

Autoscaler runs by default on traversal, copy, and retry runs. Set **`DisableAutoscaler: true`** to turn off the control loop. Default **`Interval` is 3s** (`migration.defaultAutoscalerInterval`; `AutoscalerConfig.Resolve()` applies when zero). Integration test: `pkg/tests/traversal/autoscaler_throttle/` (`Interval: 3s`, 20 workers, Spectra chaos).

Copy phase uses `startCopyAutoscaler` (single `copy` queue, operation profile resolved from copy pass + src/dst providers).

### Measurement vs decision (two loops)

The control loop separates **fast measurement** from **slow actuation**:

| Loop | Default cadence | Package | Role |
|------|-----------------|---------|------|
| **QueueObserver** | 200ms (`ObserverPollInterval`) | `pkg/queue/observer.go` | Poll queues; accumulate internal time buckets; update EMA rates (discovery, copy items, bytes, task completions) |
| **Autoscaler** | 3s (`AutoscalerConfig.Interval`) | `pkg/scaling/autoscaler.go` | Read latest observer/seal/memory snapshots; classify once; apply at most one AIMD step per tick |

The observer runs continuously in its own goroutine. The autoscaler **reads** smoothed EMA values and internal metrics on each tick — it does not re-poll queues itself. FS rate-limit hits are **read-and-reset** on autoscaler tick (`TakeRecentHits()`), not on every observer poll.

**Implication:** AIMD `AdditiveStep` is **+N workers per autoscaler tick**, not per second. Halving `Interval` from 6s→3s doubles the effective ramp rate even with the same `AdditiveStep`. Probe cooldown defaults to `2 × Interval`. Tuning `ObserverPollInterval` without changing `Interval` improves measurement freshness without increasing control churn.

EMA smoothing uses `α = 0.2` on each observer poll (~5-sample effective window at 200ms).

### Control loop (each tick)

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────────────┐
│ Collect signals │ ──► │ Classify         │ ──► │ Actuate (workers/delay) │
│ observer, seal, │     │ one pressure     │     │ AIMD step up/down       │
│ meminfo         │     │ class per tick   │     │ shared or independent   │
└─────────────────┘     └──────────────────┘     └─────────────────────────┘
```

On each tick (`scaling.Autoscaler.tick`):

1. **Resolve profiles** — read each queue's `ScalingContext()` (mode, copy pass, src/dst provider), compose the effective operation profile, merge adapter list pagination when `list_children` is active, and reconcile worker caps when the effective max drops (e.g. copy pass 1 → 2).
2. **Snapshot** per-queue `InProgress`, `Pending`, observer internal metrics, seal telemetry (read-and-reset), and memory sample (`MemAvailable` + process RSS from `/proc`, autoscaler tick only).
3. **Classify** → exactly one `PressureClass` (priority order below).
4. **Actuate** (at most one primary pressure response per tick, plus optional seal backpressure step-down):
   - `FS_THROTTLE` or `MEMORY_PRESSURE` → step **down** workers (and inter-op delay ↑ at worker floor); **list page size ↑** on FS throttle when `PreferLargePages`
   - `UNDERFEED` → step **up** workers if memory is green (delay recovery first); **batch/seal knobs ↑** when memory budget allows
   - `NONE` → **calm AIMD probe:** worker ↑ (or inter-op delay ↓ at floor) when probe cooldown elapsed and memory green
   - **Seal hard-cap hits** (since last tick) → step down batch/seal knobs independently (`PressureSeal` label); does not change pressure class

**Actuated knobs:** `WorkerCount`, `InterOpDelayMs`, `ListPageSize` (FS throttle, when `PreferLargePages`), `LeaseBatchSize`, `RefillBatchSize`, seal buffer `RowThreshold` / `FlushInterval` (memory pressure step-down; underfeed step-up; seal hard-cap step-down).

**Not actuated on FS throttle alone:** batch and seal knobs — FS throttle already reduces workers and inter-op delay first.

### Shared vs independent FS backends

When **src and dst wrap the same FS instance** (e.g. same Spectra SDK — detected in `migration.sameBackend` or explicit shared `BackendGroupID`), both queues register in one **backend group**. The autoscaler then:

- Treats **`MaxWorkers` as a combined cap** across src+dst (not per queue).
- **AIMD on total workers**, then **even split** (`SplitWorkersTotal`) — no ±1 rebalance toward underfed queue (by design).
- Applies **inter-op delay uniformly** to all queues in the group at the worker floor.
- Shares one **`FSDegradationState`** so throttle signals from either queue affect the group.

When backends are **different instances** (default `queue:src` / `queue:dst` groups), each queue scales **independently** with its own profile `MaxWorkers` and degradation telemetry.

### Why you may only see scale-down (no additive probe)

Scale-up runs on **`NONE`** (TCP-style calm probe after cooldown) and **`UNDERFEED`** (starvation). For a calm probe on `NONE`, workers must be below the group/profile cap and probe cooldown must have elapsed since the last decrease.

**UNDERFEED** additionally requires all of the following for a queue:

- `TimeWaitingOnQueue > 500ms`
- `Pending > 0` (work waiting in the local buffer)

Workers may be busy (`InProgress > 0`); underfeed still fires when the pending buffer is starved while workers wait on the queue.

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
| `MEMORY_PRESSURE` | Host RAM use ≥ **90%** (`MemTotal` − `MemAvailable`) / `MemTotal`, or MemAvailable &lt; 512 MiB when `MemTotal` unknown | AIMD worker ↓; batch/seal knobs ↓ |
| `FS_THROTTLE` | Any `RateLimitHitsSinceLastPoll > 0` on src/dst/copy (from FS degradation telemetry) | AIMD worker ↓ (or inter-op delay ↑ at floor); list page ↑ when eligible; **at most one step-down per retry-after window** |
| `UNDERFEED` | `TimeWaitingOnQueue > 500ms` **and** `Pending > 0` for a queue | AIMD delay ↓ then worker ↑; batch/seal knobs ↑ if memory green + budget allows |
| `NONE` | Otherwise | **Calm AIMD probe:** worker ↑ (or inter-op delay ↓ at floor) when probe cooldown elapsed and host memory green |

**Separate from classifier:** `HardCapHitsSinceLastPoll > 0` on seal telemetry triggers batch/seal step-down on the same tick (labeled `SEAL_BACKPRESSURE` on events). `HWMSinceLastPoll` is collected for diagnostics but does not classify pressure today.

**Explicitly not a trigger:** `SealIOWaitActive()` (slow DuckDB flush). SealBuffer already blocks producers on hard cap; we act on hard-cap **hit events**, not flush wait state alone.

**Scale-up gate:** `ScaleUpAllowed()` requires host RAM use **below 80%** (yellow zone at 80–90% blocks worker/batch scale-up). Process RSS is sampled but does not trigger memory pressure when the host still has headroom.

**Batch/seal scale-up:** On `UNDERFEED` only (not calm `NONE`), when host use stays below 90% and `MemoryBudgetAllowsIncrease` passes, the autoscaler increases lease/refill batches and seal row threshold / flush interval toward profile max (inverse of memory-pressure halving). A **3 × Interval** cooldown applies after any batch/seal step-down.

### FS throttle signal path

1. Sylos-FS adapter hits rate limit → `FSDegradationState.RecordSignal` (Spectra: chaos 429 + retry hooks in `listChildrenWithRetry`).
2. Shared degradation bridge when src/dst adapters share state (`migration.combinedRateLimitBridge`).
3. `QueueObserver.RegisterRateLimitTelemetry` → `RateLimitHitsSinceLastPoll` via `TakeRecentHits()` (read-and-reset each tick); `RateLimitedUntil` from shared `FSDegradationState`.

**FS backoff (retry-after sync):** After an FS throttle step-down, further worker/inter-op decreases are blocked until `max(RateLimitedUntil, probe cooldown)` elapses. This avoids stacking multiple halving steps from in-flight requests that hit the same shared adapter rate limit before workers finish sleeping. Calm scale-up probes are also blocked until that window ends.

Other FS adapters need degradation telemetry wired via `GetDegradationState()` (LocalFS and Spectra are wired today; cloud adapters in backlog).

### TCP-style AIMD (`pkg/scaling/aimd.go`)

**Workers** (primary lever):

| Phase | When | Behavior |
|-------|------|----------|
| Multiplicative decrease | `FS_THROTTLE` or `MEMORY_PRESSURE` | `target = floor(cur × 0.5)`, min `MinWorkers` (1); records `ssthresh` |
| Probe cooldown | After any worker decrease | No worker scale-up for `2 × Autoscaler.Interval` (default **6s** at 3s tick) |
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

### Efficiency-aware scale-up probing (second-order AIMD)

Scale-up is not blind AIMD: before each worker increase (and after a probe window), the autoscaler compares **throughput EMA** from the observer against the worker count delta.

| Event | Response |
|-------|----------|
| **Failed efficiency probe** — workers increased but discovery/copy/bytes EMA did not improve enough (`rate_gain / worker_gain < MinEfficiencyRatio`, default 0.08) | Roll back toward pre-probe count; lower `ssthresh`; ratchet probe cooldown (2×, capped at 10m) |
| **FS throttle during probe** | Abort in-flight probe; ratchet cooldown |
| **Successful probe** | Snap probe cooldown back to normal |
| **Long stability** (no throttle, no failed probes) | Slowly raise `ssthresh` toward `MaxWorkers` (default 5m interval) |

**Signals:** `QueueObserver` EMA — discovery items/sec, copy items/sec, bytes/sec (copy pass 2 prefers bytes/sec), task completions/sec (inter-op delay seeding). Shared groups evaluate **total worker count** and the **active queue's throughput** (idle peers skipped).

**Probe window:** `MinProbeWindow` defaults to one autoscaler tick. Tunables via `scaling.Config.EfficiencyProbe`.

Design rationale: reduce probe aggressiveness when scale-up does not pay off — without permanently freezing concurrency (TCP-style cautious probing, not a one-way latch).

### List page size (secondary lever)

On **`FS_THROTTLE`** worker step-down, if the profile has `PreferLargePages` **and** the p95 of recent `ListChildren` result sizes exceeds the current page size, the autoscaler **increases `ListPageSize`** (double toward max, min step 20). Small folders (p95 ≤ page size) skip this knob — raising page size would not reduce list work.

On **`UNDERFEED`** worker step-up, page size **halves toward** `DefaultListPageSize`.

**Bounds source:** Each Sylos-FS adapter that implements `FSListChildrenPagination` exposes `MinPageSize`, `MaxPageSize`, `DefaultPageSize`, and `PreferLargePagesUnderThrottle`. At queue init and autoscaler startup, ME merges these into the profile via `scaling.ApplyAdapterListPagination` (profile map values are fallbacks only when the adapter does not implement the interface).

| Adapter | Min | Default | Max | Prefer large pages |
|---------|-----|---------|-----|--------------------|
| `SpectraFS` | 20 | 100 | 10,000 | yes |
| `LocalFS` | 20 | 100 | 1,000 | no |
| `generic` profile fallback | 20 | 100 | 10,000 | yes |

### Operation-based FS profiles (`pkg/scaling/operation_profile.go`)

Scaling bounds are keyed by **provider + operation**, not by queue role or migration phase. Each queue exposes a `ScalingContext` (mode, copy pass, src/dst provider IDs, backend group IDs). The autoscaler **re-resolves** the effective profile every tick from that context.

**Operations:**

| Operation | FS adapter touchpoints |
|-----------|------------------------|
| `list_children` | Traversal src/dst `ListChildren` |
| `create_folder` | Copy pass 1 dst `CreateFolder` |
| `download` | Copy pass 2 src `OpenRead` (incl. GDrive export) |
| `upload` | Copy pass 2 dst `OpenWrite` / upload commit |

**Compose rules:**

| Context | Active ops | Effective profile |
|---------|------------|-------------------|
| Traversal `src` | `list_children` on src provider | `src.list_children` (+ adapter pagination merge) |
| Traversal `dst` | `list_children` on dst provider | `dst.list_children` |
| Copy pass 1 | `create_folder` (dst only) | `dst.create_folder` |
| Copy pass 2 | `download` + `upload` | `ComposePipelineMin(src.download, dst.upload)` — min on workers/default/max/inter-op; list knobs omitted |
| Copy retry | Same as copy by pass | Same resolver |

When src and dst share one backend (`BackendGroupID`), **`MaxWorkers` is still a combined cap** across src+dst for traversal; copy uses a single queue budget from the composed pass profile.

**Profile shape:**

```go
type OperationProfile struct {
    MinWorkers, DefaultWorkers, MaxWorkers int
    MaxInterOpDelay time.Duration
    // list_children only:
    MinListPageSize, DefaultListPageSize, MaxListPageSize int
    ListPageStep int
    PreferLargePages bool
    DefaultLeaseBatch, MaxLeaseBatch, MinLeaseBatch int
    DefaultRefillBatch, MaxRefillBatch, MinRefillBatch int
}
```

Resolved profiles map to the actuator shape `FSPerformanceProfile` via `ToActuatorProfile`. Unknown providers fall back to `generic` operation profiles.

**Built-in providers:** `generic`, `local`, `google_drive`, `dropbox`, `spectra`.

| Provider | list_children | create_folder | download | upload |
|----------|---------------|---------------|----------|--------|
| `generic` | workers 10/32, pages 100/10k | workers 8/32 | workers 8/32 | workers 8/32 |
| `local` | workers 8/64, pages 100/1k | workers 8/64 | workers 8/64 | workers 8/64 |
| `google_drive` | pages 100/500, workers 6/16 | workers 4/12 | workers 8/16 | workers 8/16 |
| `dropbox` | pages 100/500, workers 6/16 | workers 4/12 | workers 8/16 | workers 8/16 |
| `spectra` | **all max caps = 0** | **0** | **0** | **0** |

**Zero-cap semantics (`MaxWorkers == 0`, etc.):** Spectra chaos limits vary per test config, so Spectra operation profiles do not encode fixed throughput caps. **`0` = no provider-imposed ceiling** — AIMD + throttle/memory gating only:

| Field | `0` means |
|-------|-----------|
| `MaxWorkers` | Uncapped — autoscaler uses `UnboundedMaxWorkers` (32) as fallback ceiling |
| `DefaultWorkers` | Conservative startup (`DefaultWorkersForUnbounded` = 2); AIMD probes up |
| `MaxListPageSize`, batch maxes, etc. | No cap — adapter/runtime defaults or AIMD list-page logic |
| `MaxInterOpDelay` | Generic autoscaler default (5s) |

**Compose with zero caps:** `ComposePipelineMin` treats `0` as "no limit from this leg" (same as `minPositive` — if one side is 0, the other side's cap wins). GDrive→Spectra copy pass 2 uses GDrive download cap; Spectra upload leg contributes no ceiling.

**Copy pass 2 efficiency probe:** When classifying calm/underfeed probes on the copy queue in pass 2, throughput rate prefers `SnapshotBytesPerSecond("copy")` (bytes/sec EMA) over items/sec so export-bound GDrive→local runs get meaningful efficiency signals.

**FS operation name mapping:** Sylos-FS degradation signals carry operation strings (`ListChildren`, `OpenRead`, `UploadFile`, etc.). `scaling.MapFSOperation` / `ClassifyFSOperation` map these to `FSOperation` for future per-op throttle filtering. Today all operations share one degradation bridge per adapter instance.

**Profile bound reconciliation:** When the effective `MaxWorkers` **drops** (copy pass 1→2, tighter upload cap), the autoscaler clamps `SetTargetWorkerCount` immediately instead of waiting for throttle. When max **rises**, existing underfeed/calm-probe paths apply.

### Backend grouping

`BackendRegistry` resolves queue → group at run start. **Same FS instance → shared worker budget + shared inter-op delay.** Different instances → independent AIMD state per queue.

### Logging and events

Each knob change emits:

- `scaling.ScalingEvent` → optional `AutoscalerConfig.OnEvent`
- Log line: `autoscaler queue=src knob=WorkerCount 20->10 pressure=FS_THROTTLE`

Knobs: `WorkerCount`, `InterOpDelayMs` (microseconds in events), `ListPageSize`, `LeaseBatchSize`, `RefillBatchSize`, `SealRowThreshold`, `SealFlushIntervalMs`.

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

**Migration-wide memory coupling:** Even when SRC and DST use different backends, raising SRC `RefillBatchSize` or worker count can increase **process RSS** (pendingBuff, in-flight task payloads, seal buffer) and affect DST. Worker splits can be mostly per backend group; **buffer and seal knobs** need migration-wide awareness (see [Global system memory watchdog](#global-system-memory-watchdog)).

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

The autoscaler must classify first, then act in priority order. Knob actuation covers workers, inter-op delay, list page size (FS throttle), and batch/seal settings — see [How it works](#how-it-works).

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

### Where knobs live

- `WorkerCount`: top-level `migration.Config`, persisted in `root_config_json`, restored from `suspend_v1`; **autoscaler adjusts live**.
- `LeaseBatchSize`, `RefillBatchSize`: `queue.QueueSizing`, `SweepConfig`, `suspend_v1`; **autoscaler adjusts** on underfeed (up) and memory/seal pressure (down).
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
| `ObserverPollInterval` | `MigrationConfig` | 200ms | 100ms | 2s | Observer EMA / internal metrics sampling (separate from autoscaler tick) |
| `Autoscaler.Interval` | `AutoscalerConfig` | 3s | 1s | 30s | Decision + actuation tick; probe cooldown = 2× this value |
| `ProgressTick` | `migration.Config` | 1s | — | — | Console progress only |

### Internal (reference constants)

| Constant | Value | Package | Role |
|----------|-------|---------|------|
| `pullKeysetWindowSize` | 50,000 | `pkg/db` | SQL scan window per pull round-trip |
| `defaultCopyBufferSize` | 64 KiB | `pkg/queue` | Per-worker file copy stream buffer |
| Log buffer batch | 50,000 / 3s | `pkg/logservice` | Secondary DB write load |

---

## FS performance profiles

Profiles live in `pkg/scaling/operation_profile.go`. `pkg/scaling/profile.go` defines the actuator shape (`FSPerformanceProfile`); use `ToActuatorProfile(LookupOperationProfile(...))` for the provider's `list_children` profile. **List pagination min/max/default** are defined on each Sylos-FS adapter (`pkg/types/listpagination.go`, implemented on `SpectraFS`, `LocalFS`, and future cloud adapters). ME merges adapter limits when `list_children` is the active operation via `scaling.ApplyAdapterListPagination`.

Lookup order:

1. `Service.ProviderID` on the migration service
2. `Service.Name`
3. `generic` fallback

### Profile shapes

**Operation profile** (source of truth for autoscaler):

```go
type OperationProfile struct {
    MinWorkers, DefaultWorkers, MaxWorkers int
    MaxInterOpDelay time.Duration
    MinListPageSize, DefaultListPageSize, MaxListPageSize int
    ListPageStep int
    PreferLargePages bool
    DefaultLeaseBatch, MaxLeaseBatch, MinLeaseBatch int
    DefaultRefillBatch, MaxRefillBatch, MinRefillBatch int
}
```

**Actuator profile** (runtime knobs on queues):

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

Built-in providers: `generic`, `spectra`, `local`, `google_drive`, `dropbox`. When src/dst share one backend, **`MaxWorkers` is the combined cap** for the group during traversal. Copy pass profiles compose src download + dst upload legs independently of traversal list profiles.

### Provider scope

Real-world targets include **local** (HDD/SSD/NAS), **SFTP**, **S3/blob**, and major **cloud drives** (SharePoint, Google Drive, Dropbox, Box, etc.). Behavior differs widely:

- **Local / NAS** — often high concurrency; limits are drive/OS-bound.
- **Cloud drives** — aggressive API rate limits; inconsistent list APIs.
- **Object storage** — different list/pagination semantics.

**Spectra** is the test harness (not a production target): simulate rate limits, latency, and failures to validate the profile schema and autoscaler before real-provider runs. If profiles are expressive enough for Spectra, they should cover production adapters.

### Spectra `chaos.rate_limits` config

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

Reference chaos limits (high list, tighter copy/read/write): `pkg/tests/traversal/shared/spectra_ephemeral_throttle.json`.

Autoscaler integration test uses `spectra_ephemeral_autoscaler_throttle.json` — same copy/bandwidth caps, but `list_children` at **3200/sec** (vs 50K in the reference config) so 20 parallel workers emit `FS_THROTTLE`, AIMD reaches the worker floor (1), and inter-op delay fallback can engage if pressure continues.

---

## Backend grouping

`BackendRegistry` resolves queue → group at run start. Same FS instance (Spectra SDK pointer equality or explicit `BackendGroupID`) → shared worker budget, shared inter-op delay, and a combined rate-limit bridge. Different instances → independent AIMD per queue (`queue:src`, `queue:dst`, `queue:copy`).

See [Shared vs independent FS backends](#shared-vs-independent-fs-backends) for runtime behavior.

### Model (reference)

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

Workers are scaled via **`Queue.SetTargetWorkerCount`** (`pkg/queue/queue_scaling.go`):

- **Scale up:** spawn worker goroutines with per-worker cancel contexts
- **Scale down:** cancel idle workers immediately; busy workers finish their task then exit via retire flag
- FS adapter concurrency hints updated on each change

**On group throttle** (shared backend): step down **total** workers first, then re-split (`SplitWorkersTotal`).

**On imbalance:** workers split evenly (`SplitWorkersTotal`); no rebalance toward the underfed queue within a shared group.

### Signals: combined vs separate

| Signal | Same `BackendGroupID` | Different groups |
|--------|----------------------|------------------|
| Rate-limit / retry-after | One stream on the group | Per adapter / per group |
| `TimeRateLimited` (observer) | Attribute to group (and per-queue for display) | Per queue |
| Autoscaler FS step-down | One decision → re-split workers | Independent per queue |
| Seal / memory / DB | Migration-wide | Migration-wide |

### Registry

```go
type BackendRegistry struct {
    groups map[string]*BackendGroup // BackendGroupID → shared state
}
```

Resolved at run start in `migration.startAutoscalerActuators`. Autoscaler uses `registry.QueuesByGroup()` for shared AIMD and `GroupMaxWorkers(groupID)`.

---

## FS instance rate limiting

All workers funnel API calls through their queue’s **FS adapter instance**. Rate-limit signals for the autoscaler come from **Sylos-FS degradation telemetry**, not from parsing queue task errors.

- Spectra: chaos 429 / `ErrRateLimited` → `FSDegradationState` via retry hooks (`listChildrenWithRetry`, `OnRateLimitWait`).
- LocalFS: classified errno paths feed the same degradation state.
- Shared state when src/dst adapters wrap the same Spectra SDK instance (`migration.combinedRateLimitBridge`).
- Observer reads `TakeRecentHits()` each autoscaler tick → `FS_THROTTLE` if any hits since last poll.
- Workers still block on retry-after in Sylos-FS (`DoWithAuthRetry`) — reactive backoff at the adapter; autoscaler step-down is proactive concurrency reduction.

Cloud adapters still need degradation wiring — see [Notes for future work](#notes-for-future-work).

See [Backend grouping](#backend-grouping) for combined vs per-backend budgets.

---

## Observer and telemetry

`QueueObserver` (`pkg/queue/observer.go`) polls queues on `ObserverPollInterval` (default **200ms**), writes external metrics to `queue_stats`, and maintains in-memory signals for the autoscaler.

### Published (external) metrics

- Discovery rate (EMA, α = 0.2), copy `items/sec`, `bytes/sec`, task completions/sec
- Pending / failed totals, round, in-progress
- Per-queue `QueueStats`

During seal I/O wait (`SealIOWaitActive`), rate EMA updates are frozen so transient flush pauses do not skew throughput signals.

### Internal metrics (in-memory, for scaling)

| Field | Populated | Meaning |
|-------|-----------|---------|
| `TimeProcessing` | yes | Workers actively executing |
| `TimeWaitingOnQueue` | yes | Pending work but nothing in-flight (cumulative since run start; used as underfeed latch) |
| `TimePausedRoundBoundary` | yes | Coordinator wait, manual pause |
| `TimeIdleNoWork` | yes | No pending, no in-progress |
| `TimeWaitingOnFS` | partial | Reserved; not primary classifier input |
| `TimeRateLimited` | yes | From FS degradation `RateLimitedUntil` + hit estimates |
| `TasksCompletedWhileActive` | yes | Used by efficiency probe to skip idle queues |

`SnapshotInternalMetrics()` copies current counters; it does **not** reset time buckets (except FS hits via `TakeRecentHits()` on the bridge).

### Read-and-reset event counters (autoscaler tick)

| Source | Fields | Effect |
|--------|--------|--------|
| FS degradation bridge | `RateLimitHitsSinceLastPoll`, `RateLimitedUntil` | `FS_THROTTLE` |
| Seal buffer telemetry | `HardCapHitsSinceLastPoll`, `HWMSinceLastPoll`, `CurrentRows` | Hard-cap hits → batch/seal step-down; HWM diagnostic only |
| Host memory sampler | `MemTotal`, `MemAvailable`, process RSS | `MEMORY_PRESSURE` / scale-up gate |

Also used by watchdogs (not autoscaler triggers):

- `database.SealIOWaitActive()` — progress/stall detection during flush
- Task errors in `task_errors` (future classification per [fs_error_classification.md](./fs_error_classification.md))

### Knob ↔ signal matrix

| Knob | Gauge signals | Event signals |
|------|---------------|---------------|
| `WorkerCount` | in-progress, completion rate EMA, FS latency buckets | rate-limit hits; host memory headroom |
| `RefillBatchSize` / `LeaseBatchSize` | `pendingBuff` depth | seal hard-cap hits; host memory budget |
| `ListPageSize` | list p95 item count | rate-limit hits (when `PreferLargePages`) |
| `SealBuffer.RowThreshold` / `FlushInterval` | `CurrentRows`, flush latency | hard-cap hits |
| `InterOpDelayMs` | task completion rate EMA | rate-limit at worker floor |

---

## Autoscaler architecture

```
┌──────────────────┐     ┌──────────────┐     ┌─────────────────────────────┐
│ QueueObserver    │────►│ Classifier   │────►│ Actuators (live adjust)     │
│ 200ms EMA/sample │     │ one class /  │     │ workers, inter-op, list page│
│ BackendRegistry  │     │ tick         │     │ batches, seal opts          │
│ SealBuffer telem │     └──────▲───────┘     └─────────────────────────────┘
│ FS rate-limit    │            │ ScaleUpAllowed() gates increases
│ Host mem sample  │            │ Efficiency probe gates scale-up
└──────────────────┘            └──────────────────────────────────────────
         ▲ autoscaler tick (default 3s) reads snapshots; observer runs faster
```

The classifier reads **backend group** state for FS throttle (combined when src/dst share an instance) and **migration-wide** state for seal/memory. Efficiency probing uses observer EMA throughput vs worker deltas.

### Actuator targets

**Per backend group (FS concurrency)**

- Total `MaxWorkers` cap; allocation `{ src, dst, copy }` sums ≤ cap when queues share the group
- On throttle: step down total, then adjust split

**Per queue lane (buffers & pagination)**

- **SRC traversal / retry** — `RefillBatchSize`, `LeaseBatchSize`, `ListPageSize`, worker slice of group budget
- **DST traversal / retry** — same; watch memory on large DST pulls + expected-child maps
- **Copy** — worker slice, copy stream buffer; two passes (folders, files) stay sequential at phase level

### Runtime behavior

Knob changes apply **live** each autoscaler tick (`SetTargetWorkerCount`, batch/seal setters, inter-op delay). On soft suspend, worker/batch sizing is persisted in `suspend_v1` and restored on resume; the autoscaler continues tuning from there. Each change emits a `ScalingEvent` (optional `OnEvent` hook + log line).

---

## Global system memory watchdog

Host memory is sampled in `pkg/scaling/memory.go` and `memory_budget.go`. On Linux, `MemTotal` / `MemAvailable` come from `/proc/meminfo`; process RSS from `/proc/self/status`. Tests can inject a custom `MemorySampler`.

When `MemTotal` is available (typical Linux):

| Level | Threshold | Effect |
|-------|-----------|--------|
| Green | Host used &lt; **80%** | `ScaleUpAllowed()` true |
| Yellow | Host used **80–90%** | Scale-up blocked; no `MEMORY_PRESSURE` yet |
| Red | Host used ≥ **90%** | `MEMORY_PRESSURE` → worker + batch/seal step-down |

When `MemTotal` is unavailable (non-Linux / restricted `/proc`):

| Level | Threshold | Effect |
|-------|-----------|--------|
| Green | `MemAvailable` ≥ 2 GiB | Scale-up allowed |
| Yellow | 512 MiB – 2 GiB | Scale-up blocked |
| Red | &lt; 512 MiB | `MEMORY_PRESSURE` |

Process RSS is sampled and logged but **does not** trigger red when the host still has headroom.

**Batch scale-up budget:** `MemoryBudgetAllowsIncrease` projects whether the next batch/seal increment would push host use past 90%.

Thresholds are hardcoded today (not configurable per deployment).

---

## Cross-queue memory coupling

Worker budgets are **per backend group**, but buffer memory is **shared in-process** across SRC/DST/copy:

- `pendingBuff` × refill/lease batch size (DST pulls carry large expected-child maps)
- Seal buffer peak between flushes
- Per-worker copy stream buffers

Raising SRC workers or refill batch can increase process RSS and affect DST even when backends differ. Today the safety valve is the host memory watchdog plus seal hard-cap step-down — not an explicit per-queue memory budget split. A dedicated `GlobalMemoryBudget` allocator (e.g. 60% SRC / 40% DST refill caps) may be worth adding if Spectra stress tests show cross-queue contention at target scale; typical enterprise folder depths may never need it.

---

## Notes for future work

Topics worth revisiting as real providers and deployment environments come online. Nothing here blocks the current design from running; these are gaps, tuning questions, and ideas we have not prioritized yet.

### Real FS adapters and error surfaces

Spectra and LocalFS wire degradation telemetry today (`GetDegradationState` → observer → `FS_THROTTLE`). Production cloud adapters (Dropbox, SharePoint, Google Drive, S3, etc.) still need the same bridge — see [fs_error_classification.md](./fs_error_classification.md) and Sylos-FS `pkg/types/cloud_adapter_contract.go`.

Local/FUSE paths have classified retry in Sylos-FS, but errno histograms under real sync-folder load would help tune ambiguous cases (EIO vs EAGAIN vs throttle-shaped delays). Unit injection tests cover the engine path; field data does not.

### Tuning and configurability

AIMD factors, memory cutoffs, underfeed dwell, and observer/autoscaler intervals are mostly constants or struct defaults today. Exposing them via `migration.AutoscalerConfig` / `scaling.Config` would help when Spectra tuning stops matching production behavior.

**Pressure dwell / hysteresis** (require N consecutive throttle ticks before step-down) is not implemented. Probe cooldown and FS backoff already limit scale-up churn; add dwell only if classifier flicker shows up in long runs.

**Time-based AIMD increase** (`workers += rate × dt`) would decouple ramp speed from autoscaler tick interval. Today `AdditiveStep` is per tick, so halving `Interval` doubles effective acceleration.

### Observability and validation

Scaling events log via `ScalingEvent` / `OnEvent` but are not yet surfaced in Sylos API metrics. Operators may want a first-class stream of knob changes alongside queue stats.

Long-run **efficiency benchmarking** (Spectra oracle: did worker probes actually improve throughput over 10+ minutes?) would validate efficiency-probe thresholds. Integration tests cover throttle step-down; sustained calm-probe behavior is less exercised.

**Validation ladder:** Spectra chaos → synthetic wide-folder load (seal spike vs worker count) → real cloud providers. Spectra profiles are expressive; production APIs are the same shape with less predictable limits.

Spectra could also simulate a **background quota consumer** (shared-tenant API budget) to stress shared-backend grouping beyond single-migration chaos.

### Design questions

- **DST vs SRC batch defaults:** should DST get a lower `MaxRefillBatch` by default given expected-child map memory cost?
- **Shared-backend imbalance:** workers split evenly today (`SplitWorkersTotal`) with no ±1 rebalance toward the underfed queue. Worth revisiting if one side consistently starves while the other idles.
- **Seal HWM:** `HWMSinceLastPoll` is collected but not used for classification — only hard-cap hits trigger step-down. A high-water mark threshold could act earlier than hitting the cap.
- **Non-Linux / containers:** memory sampling falls back to coarse MemAvailable heuristics when `/proc/meminfo` is missing; no cgroup-aware limits.

### Out of scope (by design)

These are intentionally not autoscaler knobs: `MaxRetries`, coordinator lead (DST round gating), and seal I/O wait alone (producers already block on hard cap; we react to cap **hits**, not flush latency).

