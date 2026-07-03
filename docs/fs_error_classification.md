# FS Error Classification & Backoff

**Status:** Explicit throttle via `FSDegradationState` works for Spectra and LocalFS. Local adapter uses the 3-bucket model + `DoWithClassifiedRetry` + behavioral ambiguous promotion. Spectra FSAdapter paths are wrapped with classified retry. Pluggable backoff calculators per profile remain planned.

Related reading:

- [autoscaler.md](./autoscaler.md) — control loop, `FS_THROTTLE`, backend groups
- Sylos-FS `pkg/types/degradation.go` — degradation telemetry shape
- Sylos-FS `DoWithAuthRetry` — per-call retry with auth refresh and rate-limit sleep

---

## Core principle: two independent axes

Retry policy and throttle detection must not be conflated into one bucket. Treat them as **separate booleans**:

1. **Is this error retryable at all?** (fatal vs. retryable)
2. **Should this error inflate the rate-limit / throttle backoff timer?** (throttle-like vs. ordinary retryable)

A given error can be true or false on either axis independently. Treating every “please retry” signal as “we got rate limited” causes the backoff timer to inflate on transient, non-throttle errors — slowing the run when load is not the problem.

This separation applies in Sylos-FS (adapter retry hooks), queue task retry (`MaxRetries`), and autoscaler inputs (`FSDegradationState` / `FS_THROTTLE`).

---

## Recommended 3-bucket model

| Bucket | Retry? | Backoff timer | Examples |
|--------|--------|---------------|----------|
| **Definitely fatal** | No | Not touched | `EACCES` / `EPERM`, stable not-found, auth failure |
| **Definitely throttle** | Yes | Inflates (exponential, honor explicit retry-after) | HTTP 429, explicit `Retry-After` header or provider retry-after value |
| **Ambiguous** | Yes, with **bounded** extra allowance | Inflates **only** if behaviorally flagged as suspected throttle | Generic `EIO`, “folder not available,” opaque FUSE / reparse-point errors with no real status code |

**Key rule:** ambiguous errors must **not** default to throttle. Default posture is neutral or fatal-leaning with a small bounded retry allowance. Promote to throttle-like backoff only when behavioral evidence supports it (below).

---

## Classifying ambiguous errors behaviorally

When no reliable status code is available (common for local-path adapters reading through sync clients, FUSE, or reparse-point layers), classify using runtime telemetry keyed on **`(operation, error_code)` per backend group** — same read-and-reset counter pattern as `FSDegradationState` and `SealBufferTelemetry`.

### Signals (planned)

| Signal | Hypothesis |
|--------|------------|
| **Concurrency correlation** | Bucket error occurrences by worker count at time of error. Throttle-like behavior should scale with load; permissions or missing-folder errors should not. |
| **Burst correlation** | Do occurrences cluster in tight bursts (e.g. during an autoscaler scale-up phase) vs. spread evenly? Bursty → load-shedding-like. Steady/uniform → more likely a persistent fault. |

**Not used:** single immediate retry success. An immediate retry against a real rate limit usually fails again by construction, so “did the very next retry succeed?” is a weak signal and mostly picks up noise.

When both concurrency and burst correlation suggest load-shedding, record `FSDegradationRateLimit` (or a new `suspected_rate_limit` kind) on the backend group’s degradation state so the autoscaler can act — without treating every ambiguous errno as throttle by default.

---

## Bounded retry allowance (safety valve)

Ambiguous-bucket errors flagged as **suspected throttle** may receive extra retry headroom beyond normal `MaxRetries`, but **capped** — not unlimited.

Example shape: `MaxRetries × backoffMultiplierCap` (~6–8 total attempts, still under the existing exponential backoff ceiling) before falling through to normal fatal-abandon handling.

This bounds the downside of misclassifying a real fatal error (e.g. permissions on a broken folder) to “takes a bit longer to fail” rather than retrying indefinitely.

Queue-level `MaxRetries` remains fixed run config for throughput tuning; this allowance is an **adapter / retry-layer** extension for ambiguous cases only.

---

## Why declared FS type is not a reliable input

`ProviderID: "local"` does not reliably describe what is underneath the path. Examples that all present as local filesystem access:

- A native disk path
- A cloud sync folder (e.g. `C:\Dropbox`)
- A user-mounted FUSE or WinFsp drive pointing at remote storage

A path can be “local” in the adapter while an API-backed throttle occurs underneath. Classification must be keyed on **error code and behavioral evidence**, not on declared FS class.

---

## The opaque local-mount gap

Cloud adapters typically see real HTTP status codes and explicit retry-after values. **Local-path adapters** often do not:

- POSIX errno has no standard “rate limited” equivalent.
- FUSE and reparse-point layers translating a backend 429 frequently surface generic `EIO` (sometimes `EAGAIN` / `EBUSY` if the implementation is careful), with no guarantee across implementations.

This is a **structural translation gap** at the boundary between remote API semantics and local syscall semantics — not something a cloud-only retry layer can solve by itself.

**Implication for Sylos:** empirical testing under load against real sync-client / mounted-folder setups is the practical way to collect errno distributions for ambiguous-bucket rules. Design the telemetry hooks first; tune buckets from measured behavior per `(operation, errno)` and backend group.

---

## Backoff strategy patterns (planned)

Backoff policy should be **pluggable per FS performance profile**, not one hardcoded formula for all providers. Separate **state tracking** (current sleep, consecutive retries, last error class) from **backoff policy** so different provider classes can use different shapes, not just different constants.

| Profile posture | Behavior sketch |
|-----------------|-----------------|
| **Default / general** | Truncated exponential increase on retry, decay on success (shrink sleep when calls succeed), bounded by min/max sleep. “Recover on success” in the same primitive may complement discrete AIMD worker step-ups. |
| **Fragile / quota-heavy APIs** | Exponential backoff **with jitter** (`base × 2^n + random_ms`) to avoid synchronized retry collisions after a shared throttle event. |
| **Assume-healthy object storage** | Near-zero delay on the happy path; backoff only on actual errors — a different default posture than quota-heavy drive APIs. |

Provider profiles in `pkg/scaling/profile.go` may eventually select a backoff **shape** as well as numeric bounds.

---

## Co-locate concurrency cap with backoff state

Worker concurrency limits and throttle/backoff state should live on the same **backend group** object, not in separate subsystems that only reference each other indirectly.

Today (v1):

- `BackendRegistry` owns combined worker budget and group identity when src/dst share an FS instance.
- `FSDegradationState` holds rate-limit until / recent hits per backend.

**Planned:** extend backend group state to include backoff calculator state, ambiguous-error behavioral counters, and `WorkerCount` actuation context in one place so scale-down, inter-op delay, and retry-after inflation stay coherent.

---

## Single retry path for explicit retry-after

If Sylos maintains more than one retry mechanism (per-call adapter retry + queue-level `MaxRetries`), **both paths must honor an explicit backend-provided retry-after value**.

A common failure mode in large codebases: a low-level retry path only understands a generic “retryable error” type and silently downgrades an explicit retry-after to generic exponential math. Audit any secondary retry loops (upload paths, streaming reads, etc.) when wiring production adapters.

Sylos-FS `DoWithAuthRetry` is the primary per-call path; queue workers should not implement a parallel backoff policy that ignores adapter-reported `RetryAfter`.

---

## Retry-path audit checklist

| FSAdapter method | LocalFS | SpectraFS | Records degradation |
|------------------|---------|-----------|---------------------|
| `ListChildren` | `withClassifiedRetry` | `withClassifiedRetry` | yes |
| `OpenRead` / `GetFileData` | `withClassifiedRetry` | `withClassifiedRetry` | yes |
| `CreateFolder` | `withClassifiedRetry` | `withClassifiedRetry` | yes |
| `CreateFile` | `withClassifiedRetry` (via `GetNode` on Spectra) | `getNodeWithRetry` | yes |
| `OpenWrite` / `UploadFile` | `withClassifiedRetry` | `withClassifiedRetry` on `UploadFile` | yes |
| `GetNode` (persistent list path) | n/a | `getNodeWithRetry` | yes |

When adding a cloud adapter, every row must be **yes** before production use. See Sylos-FS `pkg/types/cloud_adapter_contract.go`.

---

## New cloud adapter checklist

1. Implement `ClassifyXxxError` mapping fatal / throttle / ambiguous buckets.
2. Wrap **all** `FSAdapter` I/O with `credentials.DoWithClassifiedRetry`.
3. Hold one `FSDegradationState` per backend instance; implement `FSDegradationReporter`.
4. When ME registers src/dst on the same backend, share degradation state (see `migration.sharedDegradationState`).
5. Register `ProviderID` in ME `pkg/scaling/profile.go`.
6. Never downgrade explicit `RetryAfter` to generic exponential sleep only.
7. Embed `types.ConcurrencyHint` (or equivalent) so ambiguous-error telemetry correlates with worker count.

---

## FUSE / sync-folder validation playbook

Local-path adapters cannot see HTTP 429 underneath a mount. Validate empirically:

1. Point ME local migration at a sync-client folder or FUSE mount (not plain ext4 when testing opaque errors).
2. Run with autoscaler enabled and elevated worker count; collect `(operation, errno)` histograms from adapter logs or injected test hooks.
3. Tune `ClassifyLocalError` ambiguous bucket from measured data — start with `EIO`, `EAGAIN`, `EBUSY`.
4. Unit tests with `LocalFS.InjectBeforeOp` cover the engine path; mount behavior remains empirical.

---

## Relationship to autoscaler v1

| Concern | v1 today | This doc |
|---------|----------|----------|
| Explicit 429 / retry-after | Spectra → `ClassifySpectraError` + `DoWithClassifiedRetry` → `FS_THROTTLE` | Wire on production cloud adapters |
| Ambiguous local / FUSE errors | **LocalFS**: classify + behavioral promotion → `suspected_rate_limit` | Tune errno buckets from empirical runs |
| Retry vs throttle axes | **`DoWithClassifiedRetry`** (LocalFS + SpectraFS) | Cloud adapters via `ClassifyError` |
| Backoff shape per profile | AIMD on workers + inter-op delay; fixed retry sleep in FS layer | Pluggable backoff calculators per profile |
| Backend group state | Workers + degradation hits + ambiguous tracker on `FSDegradationState` | Co-locate backoff calculator on `BackendGroup` |

See [autoscaler.md — Remaining work](./autoscaler.md#remaining-work) for implementation ordering.

---

## Open questions

- Should `suspected_rate_limit` be a distinct `FSDegradationKind` or folded into `FSDegradationRateLimit` with a confidence flag?
- Default `backoffMultiplierCap` for ambiguous allowance — fixed engine constant or profile field?
- Minimum sample size before behavioral promotion (avoid flicker on single spurious `EIO`)?
- Should autoscaler treat **behaviorally promoted** ambiguous throttle the same as explicit `FS_THROTTLE`, or with a softer worker step-down?
