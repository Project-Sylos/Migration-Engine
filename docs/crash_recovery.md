# Soft stop, hard stop, crash, and resume

How a migration pauses mid-phase and comes back. Related: [algorithms.md](./algorithms.md), [ops_store_overview.md](./ops_store_overview.md).

A migration run belongs to the **Sylos process**, not a browser session. Signing out does not stop work. Stopping the binary does.

| Mode | Intent | Trust in “where we left off” |
|------|--------|------------------------------|
| **Soft stop** | Pause cleanly | High. Finish in-flight work, flush durable state, save cursors. Resume continues from that checkpoint. |
| **Hard stop** | Abandon now | Low. Cancel aggressively. Resume still works, but first runs a **recovery sweep** (re-check / maybe redo some work). |
| **Crash / kill** | Process died | Same resume path as hard stop: Badger frontier remains; in-memory leases are gone; recovery mode applies. |

**Resume** starts from a suspended (or healed) phase. Soft-stop resume is lightweight. Crash / hard-stop resume enters **unclean recovery** for one round, then continues normally.

Prefer soft stop when the UI can wait. Hard stop is for when soft stop is stuck.

```
Soft stop → suspend checkpoint → Resume → continue cursors (no forced hash verify)

Crash / hard stop → unclean + suspended → Resume → recovery sweep → clear unclean → normal
```

---

## Pieces that matter

- **Badger `pend:*` (+ status overlays)** is the durable work frontier. Queue leases are in memory. Kill the process and leases vanish; pending keys remain.
- **Seal buffer** batches status/frontier writes. Soft stop flushes it. Kill can lose rows that never sealed; recovery has to rediscover reality on the filesystems where needed.
- **`suspend_v1`** (runtime meta): soft-stop checkpoint (rounds, keyset cursors, sizing knobs, queue positions). Present and usable means clean resume.
- **`unclean_interrupt`** (runtime meta): do not trust soft cursors; run recovery mode on the next Resume. Set by crash heal, hard stop, or resume with missing/unusable suspend state. Cleared when the recovery round finishes. Soft stop clears it when writing a clean checkpoint.
- **Phases**: live work uses `*-in-progress` (and briefly `*-finalizing`). Paused work uses `*-suspended`. Legacy hard-stop phase `aborted` heals to a suspended phase on startup/resume.
- **API startup** finds registry rows stuck in active phases with no live run, heals them to suspended / finalize-failed, sets unclean when needed, and never auto-starts work.

If a usable soft checkpoint exists, Resume must not also force recovery HashVerify or cursor reconstruct.

---

## Soft stop

Leave a checkpoint Resume can trust.

1. Soft stop is requested while a phase is live.
2. Queues pause; workers get a short grace window to finish. After grace, leases cancel and Working-on clears immediately (copy checkpoints publish without blocking drain). Soft stop does not wait on hung FS syscalls.
3. Seal buffer flushes; durable teardown / checkpoint for that phase runs as needed.
4. Runtime writes `suspend_v1` (rounds, cursors, knobs) and related queue position state.
5. Phase becomes the matching `*-suspended`.
6. `unclean_interrupt` is cleared if it was set.
7. This migration sits idle until Resume; other migrations in the same process are unaffected.

### Resume after soft stop

1. Phase is already suspended (or a review / finalize-failed path as applicable).
2. Load `suspend_v1` and restore rounds / cursors / knobs.
3. Re-enter the phase (`*-in-progress`).
4. Traversal: re-sweep the stopped round from before any remaining `pend:*` folders at that depth (soft-stop force-abandon can leave pending behind the live keyset cursor). When that round drains, advance and continue BFS as normal. Saved cursors that would skip pending are rewritten on soft-stop save and again on resume.
5. Copy: optional one-round DST existence window only when there was real prior copy success **and** pending work remains. No forced HashVerify.
6. Delete: continue from `pending_explicit` frontier; no hash window.
7. Run continues as a normal live phase.

---

## Hard stop

Stop immediately when soft stop cannot finish. Still recoverable.

1. Hard stop is requested.
2. Run context cancels; in-flight queue work is abandoned (no long drain, no soft checkpoint).
3. Phase moves to the matching `*-suspended`.
4. `unclean_interrupt` is set for the interrupted kind (traversal / copy / delete).
5. Later Resume uses unclean recovery, not soft cursors.

Legacy `aborted` phases heal to suspended + unclean on startup/resume the same way.

---

## Crash / process kill

1. Process exits mid `*-in-progress` or `*-finalizing` (kill, OOM, host reboot, etc.).
2. In-memory leases and unflushed seal buffer are gone.
3. On next Sylos start, the API reconciles registry rows that look active but are not live:
   - `*-in-progress` → matching `*-suspended` + `unclean_interrupt`
   - `*-finalizing` → matching `*-finalize-failed` (+ unclean as applicable) so finalize can be retried
   - legacy `aborted` → suspended + unclean
4. Registry phase is updated for Resume-eligible lists. Nothing is auto-started.
5. Resume follows unclean recovery (same as hard stop).

---

## Unclean recovery

Reconcile the Badger frontier with what may already exist on SRC/DST after an unclean interrupt, then return to normal operation.

Entered when Resume sees `unclean_interrupt` (or suspended without a usable `suspend_v1`). Soft-stop resume with a good checkpoint does not use this path.

### Shared

1. Normalize any dead in-progress / aborted phase to suspended if needed; ensure unclean is set.
2. Start the phase with recovery mode enabled.
3. After the first recovery round / existence window completes, clear `unclean_interrupt`.
4. Continue as a normal live phase.

### Traversal

1. Ignore soft-stop keyset cursors for the recovery pass.
2. Reconstruct position before the first pending folder at the recovery depth/round.
3. Re-list pending folders; skip work already marked successful.
4. After that sweep, resume normal BFS discovery.
5. Optional discovery HashVerify (user setting) is separate from copy recovery hashing.

### Copy

1. Enable the one-round DST existence window when Pending > 0, even if Successful == 0 (unlike soft-stop resume).
2. Folders in the window: existence via ListChildren only (no hash).
3. Files that name-match on DST during the window:
   - Dual-read SHA-256 HashVerify (SRC vs DST).
   - Equal → treat as already copied.
   - Unequal or verify failure → leave pending / needs overwrite so copy redoes the file.
4. Prefer existing transfer checkpoints when present; hash verify applies when existence suggests a finished object without a trusted soft-stop seal.
5. When the anchored recovery pass/round leaves, clear the existence window, HashVerify flag, and unclean interrupt.

### Delete (v1)

1. Heal / unclean normalize only.
2. Resume from `pending_explicit` frontier.
3. No hash / existence recovery window.

---

## Situation → behavior

| Situation | Behavior |
|-----------|----------|
| Soft-stopped, then Resume | Continue cursors; lightweight |
| Hard-stopped, then Resume | Recovery sweep first, then normal |
| Process killed mid-run, Sylos restarted, Resume | Heal to suspended + unclean, then recovery sweep |
| Signed out while migration live | No stop; work keeps running in-process |
| Finalize interrupted mid-teardown | `*-finalize-failed`; retry finalize, not worker Resume |

---

## Related

- [algorithms.md](./algorithms.md) — discovery / copy / delete BFS rules
- [ops_store_overview.md](./ops_store_overview.md) — Badger, seal buffer, frontier
- [item_statuses.md](./item_statuses.md) — pending / successful / already_existed
- [autoscaler.md](./autoscaler.md) — knobs restored from soft-stop checkpoint
