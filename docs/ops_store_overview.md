# How Sylos uses the ops store (intro)

This is a guided tour of the per-migration **Badger ops store**: what lives there, how write-heavy phases feed it, and how review / search / filters read it. It sits between “what is a Badger key?” and the deep design docs. After reading this, dive into [pkg/db/README.md](../pkg/db/README.md), [algorithms.md](./algorithms.md), [search_indexes.md](./search_indexes.md), and [filter_rules.md](./filter_rules.md) when you need specifics.

---

## The big picture

Each migration has its own on-disk store under the migration folder:

| On disk | What it is |
|---------|------------|
| `{id}.ops/` | **Badger** directory. This is the operational source of truth. |
| `{id}.db` | Logical path / optional marker name (historical). Not a SQL catalog. |

There is **no per-migration DuckDB**. Queues, review UI, resume, and stats all go through Badger via `pkg/db` → `pkg/opsdb`.

Mentally, the store has a few jobs:

1. **Catalog** – what we discovered (path, name, size, type, …).
2. **Status overlay** – where each item is in traversal / copy / delete.
3. **Frontier** – what work is still pending at which BFS depth.
4. **Indexes** – ways to find candidates quickly during review search.
5. **Meta / credentials / telemetry** – lifecycle, OAuth bindings, logs, queue stats.

Write-heavy phases mostly **append and update** those structures through a **seal buffer**. Read-heavy review phases mostly **plan a candidate set**, then filter in Go.

```
Write path (discovery / copy / delete)
  workers → seal buffer → Badger batch flush

Read path (path review / search / filters)
  query → index / status planner → hydrate nodes → Go matchers → page of results
```

---

## Mental model of the schema (light)

You do not need every key prefix memorized. These families matter day to day:

| Family | Plain English |
|--------|----------------|
| `node:…` | Catalog row for one SRC or DST item. |
| `st:…` | Current traversal / copy / delete status for that item. |
| `pend:…` | “Still need to work this” frontier keys, ordered by depth and type. |
| `child:` / `kids:` | Tree edges and packed child snapshots for a parent. |
| `idx:…` | Secondary indexes for path, name, size, mtime, segments, trigrams. |
| `map:…` | SRC↔DST identity pairing after copy. |
| `filtapp:` / `gpl:` | Filter apply provenance and path-issue (GPL) rows. |
| `meta:schema_version` | Permanent layout version (currently `0.1`). Never rename this key. |
| `mig:` / `oauth:` / `fsbind:` | Migration lifecycle and how to rebuild FS adapters. |
| `log:` / `qstat:` / … | Telemetry. |

Status and catalog are **separate on purpose**: discovery can stamp many nodes; later phases rewrite status (and sometimes frontier) without rewriting the whole catalog row every time.

For the full key table, see [pkg/db/README.md](../pkg/db/README.md). For status value meanings, see [item_statuses.md](./item_statuses.md).

---

## The seal buffer (why writes feel “batched”)

Workers never open a Badger write transaction per tiny completion on the hot path. They hand results to a **SealBuffer**, which:

- Accumulates node inserts, status updates, frontier deltas, and kids packs.
- Flushes in **trusted batches** (`WriteSealBatchTrusted`) when row thresholds or timers fire.
- Applies **hard caps** so producers block if seal cannot keep up (back-pressure the autoscaler cares about).

That design keeps discovery/copy/delete CPU on FS I/O and queue leasing, while Badger sees fewer, larger transactions.

**Rule of thumb for new code:** on the queue hot path (pull → worker → complete), prefetch what you need in **batched** pull/list APIs, compare in memory, and only **append** to the seal buffer. Do not `GetNodeByID` inside a per-item completion loop.

---

## Write-heavy phases

### Discovery (traversal)

Goal: walk SRC (and coordinated DST) breadth-first and record the inventory.

Typical DB traffic:

1. **Pull** pending folder tasks from `pend:…` into an in-memory buffer.
2. Worker calls FS `ListChildren`.
3. **Seal** discovered children as catalog nodes, set traversal status, enqueue next-depth pending keys, update kids packs / indexes.

Indexes for later search (`idx:seg`, `idx:tri`, name/path/size/mtime) are written **when catalog nodes are inserted**, not in a giant end-of-phase rebuild (aside from a one-shot backfill flag for older stores).

Resume works because the frontier and statuses live in Badger: after a crash or soft suspend, the queue pulls `pend:…` again.

### Copy

Goal: create missing items on DST and stream file bytes SRC→DST (no full-file staging).

DB traffic shifts toward:

- Pulling copy-pending work (still frontier + status).
- Updating copy status on success / failure / exclude.
- Writing SRC↔DST maps and, for files mid-transfer, resume fields on the SRC node (`xfer_*`).
- Creating DST catalog rows as folders/files appear on the destination.

Copy is still seal-buffered. Long uploads heartbeat progress so watchdogs do not confuse “uploading” with “stalled.”

### Delete (source cleanup)

Goal: remove selected SRC items in reverse-BFS order after copy review.

DB traffic is mostly status + frontier for the delete phase: mark delete pending/skipped, pull deepest folders/files first, seal `deleted` / `failed`. Catalog rows remain useful for review; the filesystem is what goes away.

---

## Read-heavy phases

### Path review (awaiting-*-review)

Between phases, the UI browses and searches the discovered tree. Those screens are **read-mostly** against Badger:

- Folder children and merged SRC/DST review rows.
- Status overlays (“show me failed copy”, “pending delete”, …).
- Pagination with cursors (keyset-style), not giant SQL offsets.

Mutations during review (exclude, unexclude, mark retry, delete skip) still go through sealed status/frontier updates. They are not a separate “staging table.”

### Arbitrary search

Review search is planned, not “scan every node every time”:

1. Look at the query (path segments, name contains, size/mtime bounds, status filters, …).
2. Pick the cheapest **candidate source** (path index, segment postings, trigrams, size/mtime index, or a capped status walk).
3. Hydrate those ids into node + status records.
4. Apply remaining predicates in **Go**.

Path search is **ordered folder parts**, not free-form “substring anywhere in the path.” Name substring uses **trigrams**. Details: [search_indexes.md](./search_indexes.md).

Cardinality counters (`stat:idx:*`, review stats) help the planner guess which index is selective.

### Filter system (rulesets)

Filters are **exclusion predicates over the already-discovered SRC inventory**. They do not steer BFS while discovery is running.

Flow in plain terms:

1. User authors or picks a ruleset (AND/OR groups, size/mtime/name/path/… leaves).
2. Preview / evaluate compiles the ruleset and runs it in Go (same compiler as apply).
3. Apply walks matching nodes (often via search) and seals `copy_status` to excluded-explicit / excluded-inherited, with provenance under `filtapp:` and fields on the status overlay.

Path `contains` / `glob` / `regex` in a **ruleset** are boolean leaves in `pkg/filter`. That is intentionally separate from the review **search bar** path-segment model. See [filter_rules.md](./filter_rules.md).

---

## How the pieces fit across a run

```
AddRoots / seed
    → write root catalog + pending frontier

StartTraversal
    → pull / lease / seal discovery (indexes grow with inserts)

Awaiting traversal review
    → search + exclude / mark-retry (read planner + sealed mutations)

StartCopy
    → pull / lease / seal copy status + maps + xfer checkpoints

Awaiting copy review / source cleanup plan
    → search + delete pending/skipped marks

StartDelete
    → reverse-BFS pull / seal delete outcomes
```

Through all of that, **one ops directory** holds the story. Soft suspend clears in-memory leases but keeps Badger; resume restores queue cursors and pulls again.

---

## Where to go next

| If you want… | Read… |
|--------------|--------|
| Exact key prefixes and seal flush rules | [pkg/db/README.md](../pkg/db/README.md) |
| BFS / copy / delete algorithms | [algorithms.md](./algorithms.md) |
| Status value meanings | [item_statuses.md](./item_statuses.md) |
| Search indexes and planner | [search_indexes.md](./search_indexes.md) |
| Ruleset exclude semantics | [filter_rules.md](./filter_rules.md) |
| Pull / lease / seal in the queue | [pkg/queue/README.md](../pkg/queue/README.md) |
| Seal pressure and autoscaler | [autoscaler.md](./autoscaler.md) |

Welcome aboard. When in doubt: **batch on the write path, plan on the read path, and treat `{id}.ops/` as the SoT.**
