# scaling Package

Throughput control for the Migration Engine: observes queue/FS/memory/seal telemetry and adjusts worker and batch knobs within provider bounds. Wired from **`pkg/migration/autoscaler.go`** during traversal, copy, delete, and retry runs.

For the full design (measurement vs actuation cadence, pressure classes, knob matrix, provider tables), see **[`docs/autoscaler.md`](../../docs/autoscaler.md)**.

---

## Package map

Root is a thin contract; actuation lives under `loop`. Children import root types where needed (`QueueActuator`, `PressureClass`); `loop` imports the other kids.

| Package | Role |
|---------|------|
| **`pkg/scaling`** (root) | `QueueActuator`, `ScalingEvent`, `PressureClass` / `Classify`, `FormatScalingEvent` |
| **`pkg/scaling/profile`** | `FSPerformanceProfile`, operation lookup/compose, list pagination apply, resolve from `queue.ScalingContext` |
| **`pkg/scaling/aimd`** | `State`, `AIMDPolicy`, soft-cap / bounce / efficiency probe / FS backoff |
| **`pkg/scaling/memory`** | Host/process sampling, budget estimates, green/yellow/red levels |
| **`pkg/scaling/backend`** | `BackendRegistry`, group worker split, `RateLimitBridge` |
| **`pkg/scaling/loop`** | `Autoscaler`, `Config`, `NewAutoscaler`, tick / actuate / memory knobs |

`pkg/queue` must **not** import `pkg/scaling` (or any scaling child). `*queue.Queue` satisfies `scaling.QueueActuator` from migration.

---

## Control loop

Two loops, same process:

1. **Observe (fast)** – **`pkg/queue/observe`** polls registered queues (~200ms), accumulates internal time buckets, updates EMA rates, persists **`queue_stats`**, and exposes **`InternalMetricsSnapshot`**.
2. **Actuate (slow)** – **`loop.Autoscaler`** ticks on **`Config.Interval`** (default 3s from migration config). Each tick: classify pressure once (root **`Classify`**), then apply at most one AIMD step per queue (`aimd` + `loop` actuate).

AIMD shape: multiplicative decrease on **`FS_THROTTLE`** / memory / seal backpressure; slow-start then additive increase when calm; inter-op delay as a fallback lever at worker floor.

---

## Integration

| Consumer | Role |
|----------|------|
| **`pkg/migration`** | Starts observer + `loop.NewAutoscaler`, registers queues as actuators, resolves **`profile`** / **`backend`** at run start. |
| **`pkg/queue`** | Exposes metrics and knob setters; no import of this package. |
| **`pkg/db`** | **`SealBufferTelemetry`**, review/work stats keys read on observer/autoscaler ticks. |

See also **`pkg/queue/README.md`** (pull / lease / seal) and **`pkg/db/README.md`** (schema, seal, stats).
