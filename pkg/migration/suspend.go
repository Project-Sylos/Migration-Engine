// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"
)

// Sentinel errors returned when a live phase ends via soft suspend (not failure).
var (
	ErrTraversalSoftSuspended = errors.New("traversal soft suspended")
	ErrCopySoftSuspended      = errors.New("copy phase soft suspended")
	ErrDeleteSoftSuspended    = errors.New("delete phase soft suspended")
)

// suspendedError embeds the soft-suspend sentinel as its error and matches it under errors.Is.
type suspendedError[S any] struct {
	error
	stats   S
	suspend RuntimeSuspendV1
}

func (e *suspendedError[S]) Is(target error) bool { return target == e.error }

func newSuspendedError[S any](sentinel error, stats S, s RuntimeSuspendV1) error {
	return &suspendedError[S]{error: sentinel, stats: stats, suspend: s}
}

// AsSuspended unwraps stats and suspend payload from a soft suspend error. S is RuntimeStats for
// ErrTraversalSoftSuspended (RunMigration / RunRetrySweep) and queue.QueueStats for the copy and delete sentinels.
func AsSuspended[S any](err, sentinel error) (S, RuntimeSuspendV1, bool) {
	var se *suspendedError[S]
	if errors.Is(err, sentinel) && errors.As(err, &se) {
		return se.stats, se.suspend, true
	}
	var zero S
	return zero, RuntimeSuspendV1{}, false
}

// RuntimeSuspendV1 is merged into migrations.runtime_state_json under key "suspend_v1".
// It holds only what resume needs: tuning knobs and last-known position for UI / max-depth.
type RuntimeSuspendV1 struct {
	Version int `json:"version"` // 1

	SuspendedAtUnix int64  `json:"suspended_at_unix"`
	Kind            string `json:"kind"` // "traversal" | "copy" | "delete"

	WorkerCount int `json:"worker_count"`
	MaxRetries  int `json:"max_retries"`

	// Queue pull sizing (0 in JSON means "use engine default" on resume).
	LeaseBatchSize  int `json:"lease_batch_size,omitempty"`
	RefillBatchSize int `json:"refill_batch_size,omitempty"`
	ObserverPollMs  int `json:"observer_poll_ms,omitempty"`
	ProgressTickMs  int `json:"progress_tick_ms,omitempty"`

	// Traversal / coordinator
	LastRoundSrc    int    `json:"last_round_src,omitempty"`
	LastRoundDst    int    `json:"last_round_dst,omitempty"`
	MaxKnownDepth   int    `json:"max_known_depth,omitempty"`
	SrcKeysetCursor string `json:"src_keyset_cursor,omitempty"`
	DstKeysetCursor string `json:"dst_keyset_cursor,omitempty"`

	// Copy
	CopyPass           int    `json:"copy_pass,omitempty"`
	LastKnownCopyRound int    `json:"last_known_copy_round,omitempty"`
	CopyKeysetCursor   string `json:"copy_keyset_cursor,omitempty"`

	// Delete (reuses CopyPass as delete pass 1=folders / 2=files)
	LastKnownDeleteRound int `json:"last_known_delete_round,omitempty"`
}

const runtimeSuspendJSONKey = "suspend_v1"

// mergeRuntimeSuspendV1 merges s into the existing runtime_state_json map (decoded object).
func mergeRuntimeSuspendV1(existing map[string]any, s RuntimeSuspendV1) (map[string]any, error) {
	if existing == nil {
		existing = make(map[string]any)
	}
	raw, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	var asMap map[string]any
	if err := json.Unmarshal(raw, &asMap); err != nil {
		return nil, err
	}
	existing[runtimeSuspendJSONKey] = asMap
	return existing, nil
}

// parseRuntimeSuspendV1 extracts suspend_v1 from merged runtime JSON blob bytes.
func parseRuntimeSuspendV1(runtimeStateJSON string) (RuntimeSuspendV1, bool) {
	if runtimeStateJSON == "" || runtimeStateJSON == "{}" {
		return RuntimeSuspendV1{}, false
	}
	var top map[string]any
	if err := json.Unmarshal([]byte(runtimeStateJSON), &top); err != nil {
		return RuntimeSuspendV1{}, false
	}
	raw, ok := top[runtimeSuspendJSONKey]
	if !ok || raw == nil {
		return RuntimeSuspendV1{}, false
	}
	inner, err := json.Marshal(raw)
	if err != nil {
		return RuntimeSuspendV1{}, false
	}
	var s RuntimeSuspendV1
	if err := json.Unmarshal(inner, &s); err != nil {
		return RuntimeSuspendV1{}, false
	}
	if s.Version != 1 || s.Kind == "" {
		return RuntimeSuspendV1{}, false
	}
	return s, true
}

// suspendRuntimeMergePatch returns JSON for updateRuntimeState with suspend_v1 payload.
func suspendRuntimeMergePatch(s RuntimeSuspendV1) (string, error) {
	merged, err := mergeRuntimeSuspendV1(make(map[string]any), s)
	if err != nil {
		return "", err
	}
	raw, err := json.Marshal(merged)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func queueSizingFromSuspend(s *RuntimeSuspendV1) *queue.QueueSizing {
	if s == nil {
		return nil
	}
	if s.LeaseBatchSize <= 0 && s.RefillBatchSize <= 0 {
		return nil
	}
	return &queue.QueueSizing{
		LeaseBatchSize:  s.LeaseBatchSize,
		RefillBatchSize: s.RefillBatchSize,
	}
}

func effectiveInt(override, def int) int {
	if override > 0 {
		return override
	}
	return def
}

func observerPollFromConfigAndSuspend(cfg time.Duration, s *RuntimeSuspendV1) time.Duration {
	if s != nil && s.ObserverPollMs > 0 {
		return time.Duration(s.ObserverPollMs) * time.Millisecond
	}
	if cfg > 0 {
		return cfg
	}
	return 200 * time.Millisecond
}

func progressTickFromConfigAndSuspend(cfg time.Duration, s *RuntimeSuspendV1, fallback time.Duration) time.Duration {
	if s != nil && s.ProgressTickMs > 0 {
		return time.Duration(s.ProgressTickMs) * time.Millisecond
	}
	if cfg > 0 {
		return cfg
	}
	return fallback
}

func reportStopProgress(fn func(string, int), step string, inProgress int) {
	if fn != nil {
		fn(step, inProgress)
	}
}

func reportStopProgressDetail(fn func(string, int), step string, inProgress int, detail string, setDetail func(string)) {
	reportStopProgress(fn, step, inProgress)
	if setDetail != nil && detail != "" {
		setDetail(detail)
	}
}

func waitDetailSetter(observer *observe.QueueObserver) func(string) {
	return func(d string) {
		if observer != nil {
			observer.SetWaitReason(d)
		}
	}
}

// newSuspendState builds the suspend payload fields shared by every phase; callers set the position fields.
func (rt *PhaseRuntime) newSuspendState(kind string, workerCount, maxRetries int, q *queue.Queue, defaultProgressMs int) RuntimeSuspendV1 {
	obsMs, progMs := rt.suspendTuning(defaultProgressMs)
	return RuntimeSuspendV1{
		Version:         1,
		SuspendedAtUnix: time.Now().Unix(),
		Kind:            kind,
		WorkerCount:     workerCount,
		MaxRetries:      maxRetries,
		LeaseBatchSize:  q.EffectiveLeaseBatchSize(),
		RefillBatchSize: q.EffectiveRefillBatchSize(),
		ObserverPollMs:  obsMs,
		ProgressTickMs:  progMs,
	}
}

// drainQueuesInOrder returns a drain step that waits for each queue's in-flight work in turn.
func drainQueuesInOrder(shutdown context.Context, queues ...*queue.Queue) func(context.Context, func(int)) error {
	return func(mergedCtx context.Context, onDrainTick func(int)) error {
		for _, q := range queues {
			if err := q.WaitInProgressZeroFunc(mergedCtx, 50*time.Millisecond, onDrainTick); err != nil {
				if softSuspendHardKilled(shutdown) {
					abandonQueuesDBOnly(queues...)
				}
				return fmt.Errorf("%s queue drain in-flight: %w", q.Name(), err)
			}
		}
		// Hard kill may clear in-progress so Wait succeeds; do not continue soft-save.
		if err := errIfSoftSuspendHardKilled(shutdown); err != nil {
			abandonQueuesDBOnly(queues...)
			return err
		}
		return nil
	}
}

// softSuspendQueues pauses queues, drains in-flight work via drain (noun labels the stop checklist), and flushes the seal buffer.
func softSuspendQueues(
	waitCtx context.Context,
	database *db.DB,
	observer *observe.QueueObserver,
	rt *PhaseRuntime,
	noun string,
	queues []*queue.Queue,
	drain func(mergedCtx context.Context, onDrainTick func(int)) error,
) error {
	if observer != nil {
		observer.Stop()
	}
	n0 := 0
	for _, q := range queues {
		q.SetState(queue.QueueStatePaused)
		q.StopWatchdog()
		q.ClearPendingBufferForSuspend()
		n0 += q.InProgressCount()
	}
	setDetail := waitDetailSetter(observer)
	reportStopProgressDetail(rt.ReportStopProgress, StopStepDraining, n0, drainWaitDetail(n0, noun), setDetail)

	mergedCtx, cancelMerged := mergeWaitContexts(waitCtx, rt.ShutdownContext)
	defer cancelMerged()

	onDrainTick := func(n int) {
		reportStopProgressDetail(rt.ReportStopProgress, StopStepDraining, n, drainWaitDetail(n, noun), setDetail)
	}
	if err := drain(mergedCtx, onDrainTick); err != nil {
		return err
	}

	reportStopProgressDetail(rt.ReportStopProgress, StopStepSaving, 0, "Writing the latest updates…", setDetail)
	if err := awaitSoftSuspendDB(mergedCtx, rt.ShutdownContext, func() error {
		return database.Flush(context.Background())
	}); err != nil {
		return fmt.Errorf("flush seal buffer: %w", err)
	}
	return nil
}

func performTraversalSoftSuspend(
	waitCtx context.Context,
	database *db.DB,
	srcQueue, dstQueue *queue.Queue,
	observer *observe.QueueObserver,
	coordinator *queue.QueueCoordinator,
	rt *PhaseRuntime,
	start time.Time,
	workerCount, maxRetries int,
) (RuntimeStats, RuntimeSuspendV1, error) {
	queues := []*queue.Queue{srcQueue, dstQueue}
	if err := softSuspendQueues(waitCtx, database, observer, rt, "items", queues, drainQueuesInOrder(rt.ShutdownContext, queues...)); err != nil {
		return RuntimeStats{}, RuntimeSuspendV1{}, err
	}
	mergedCtx, cancelMerged := mergeWaitContexts(waitCtx, rt.ShutdownContext)
	defer cancelMerged()
	if err := awaitSoftSuspendDB(mergedCtx, rt.ShutdownContext, func() error {
		return queue.FinalizeCopyWorkOnStop(database, coordinator)
	}); err != nil {
		return RuntimeStats{}, RuntimeSuspendV1{}, fmt.Errorf("finalize copy work on stop: %w", err)
	}

	srcStats, dstStats := snapshotTraversalQueueStats(database, coordinator)
	maxD := srcQueue.GetMaxKnownDepth()
	if d := dstQueue.GetMaxKnownDepth(); d > maxD {
		maxD = d
	}
	if maxD < 0 {
		if d, err := stats.GetMaxDepth(database, "SRC"); err == nil {
			maxD = d
		}
	}

	suspend := rt.newSuspendState("traversal", workerCount, maxRetries, srcQueue, 1000)
	suspend.LastRoundSrc = srcStats.Round
	suspend.LastRoundDst = dstStats.Round
	suspend.MaxKnownDepth = maxD
	suspend.SrcKeysetCursor = srcQueue.GetKeysetCursor()
	suspend.DstKeysetCursor = dstQueue.GetKeysetCursor()
	if database.Ops() != nil {
		if err := database.SaveTraversalQueuePositions(
			srcStats.Round, suspend.SrcKeysetCursor,
			dstStats.Round, suspend.DstKeysetCursor,
		); err != nil {
			return RuntimeStats{}, RuntimeSuspendV1{}, fmt.Errorf("save traversal queue positions: %w", err)
		}
	}

	// StopStepDone is reported after seal stop + checkpoint (see finishSoftStopBulkPhase).
	waitDetailSetter(observer)("Saving buffered progress…")
	return RuntimeStats{
		Duration: time.Since(start),
		Src:      srcStats,
		Dst:      dstStats,
	}, suspend, nil
}

// drainCopyQueue returns the copy drain step: a grace window, then force-checkout of workers, then abandon.
func drainCopyQueue(copyQueue *queue.Queue, shutdown context.Context) func(context.Context, func(int)) error {
	return func(mergedCtx context.Context, onDrainTick func(int)) error {
		copyQueue.EnterStopAbandonWindow(queue.DefaultSpinDownGrace)
		graceCtx, graceCancel := context.WithTimeout(mergedCtx, queue.DefaultSpinDownGrace)
		defer graceCancel()
		if err := copyQueue.WaitInProgressZeroFunc(graceCtx, 50*time.Millisecond, onDrainTick); err != nil {
			// After grace: force-checkout smallest file workers (freeze callback also does this).
			copyQueue.RequestForceCheckoutAllWorkersForStop()
			drainCtx, drainCancel := context.WithTimeout(mergedCtx, 5*time.Second)
			defer drainCancel()
			if err2 := copyQueue.WaitInProgressZeroFunc(drainCtx, 50*time.Millisecond, onDrainTick); err2 != nil {
				copyQueue.Spin.AbandonDBOnly.Store(true)
				copyQueue.AbandonInProgressTasks()
			}
		}
		if err := errIfSoftSuspendHardKilled(shutdown); err != nil {
			abandonQueuesDBOnly(copyQueue)
			return err
		}
		copyQueue.Spin.AbandonDBOnly.Store(false)
		return nil
	}
}

// performQueueSoftSuspend is the copy/delete soft suspend; kind is "copy" or "delete" and selects the position fields saved.
func performQueueSoftSuspend(
	waitCtx context.Context,
	database *db.DB,
	q *queue.Queue,
	observer *observe.QueueObserver,
	rt *PhaseRuntime,
	noun, kind string,
	workerCount, maxRetries int,
	drain func(context.Context, func(int)) error,
) (queue.QueueStats, RuntimeSuspendV1, error) {
	if err := softSuspendQueues(waitCtx, database, observer, rt, noun, []*queue.Queue{q}, drain); err != nil {
		return queue.QueueStats{}, RuntimeSuspendV1{}, err
	}
	stats := q.Stats()
	suspend := rt.newSuspendState(kind, workerCount, maxRetries, q, 2000)
	suspend.CopyPass = q.GetCopyPass()
	suspend.MaxKnownDepth = q.GetMaxKnownDepth()
	suspend.CopyKeysetCursor = q.GetKeysetCursor()
	save := database.SaveCopyQueuePosition
	if kind == "delete" {
		suspend.LastKnownDeleteRound = stats.Round
		save = database.SaveDeleteQueuePosition
	} else {
		suspend.LastKnownCopyRound = stats.Round
	}
	if database.Ops() != nil {
		if err := save(stats.Round, suspend.CopyKeysetCursor); err != nil {
			return queue.QueueStats{}, RuntimeSuspendV1{}, fmt.Errorf("save %s queue position: %w", kind, err)
		}
	}
	waitDetailSetter(observer)("Saving buffered progress…")
	return stats, suspend, nil
}

// finishSoftStopBulkPhase flushes the seal buffer and checkpoints without rebuilding secondary indexes.
// Index rebuild belongs to end-of-mode finalize (*-finalizing), not soft Stop.
// Call after perform*SoftSuspend and before returning suspended so Stop progress
// stays on "Saving" until teardown is finished (Resume-ready).
// Honors ctx cancellation and HardAborted (force stop must not Flush/checkpoint).
func finishSoftStopBulkPhase(
	ctx context.Context,
	database *db.DB,
	reportFn func(string, int),
	setDetail func(string),
) error {
	if database == nil {
		reportStopProgress(reportFn, StopStepDone, 0)
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if database.HardAborted() {
		return fmt.Errorf("soft stop bulk phase aborted by force stop")
	}
	reportStopProgressDetail(reportFn, StopStepSaving, 0, "Saving buffered progress…", setDetail)
	database.SetActivity("Saving buffered progress…")
	if err := database.StopBulkPhaseSeal(); err != nil {
		database.SetActivity("")
		return fmt.Errorf("stop bulk phase seal: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if database.HardAborted() {
		return fmt.Errorf("soft stop bulk phase aborted by force stop")
	}
	reportStopProgressDetail(reportFn, StopStepSaving, 0, "Making sure everything is safely stored…", setDetail)
	if err := database.CheckpointWithRetry(ctx, 5); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}
	database.SetActivity("")
	if setDetail != nil {
		setDetail("")
	}
	reportStopProgress(reportFn, StopStepDone, 0)
	return nil
}

func forceStopOverridesSoftSuspend(shutdown context.Context, database *db.DB) bool {
	return softSuspendHardKilled(shutdown) || (database != nil && database.HardAborted())
}

func logTraversalResumePositionCheck(resume *RuntimeSuspendV1, srcQueue, dstQueue *queue.Queue) {
	if resume == nil || srcQueue == nil || dstQueue == nil {
		return
	}
	if srcQueue.GetMode() != queue.QueueModeTraversal || dstQueue.GetMode() != queue.QueueModeTraversal {
		logResumeCheck("warning", fmt.Sprintf(
			"traversal resume: expected traversal mode (src=%s dst=%s)",
			srcQueue.GetMode(), dstQueue.GetMode(),
		))
		return
	}
	srcRound := srcQueue.Stats().Round
	dstRound := dstQueue.Stats().Round
	logResumeCheck("info", fmt.Sprintf(
		"traversal resume: continuing src round %d dst round %d (suspended src=%d dst=%d)",
		srcRound, dstRound, resume.LastRoundSrc, resume.LastRoundDst,
	))
}

func logCopyResumePositionCheck(resume *RuntimeSuspendV1, copyQueue *queue.Queue, startRound int) {
	if resume == nil || copyQueue == nil {
		return
	}
	if copyQueue.GetMode() != queue.QueueModeCopy {
		logResumeCheck("warning", fmt.Sprintf(
			"copy resume: expected copy mode, got %s", copyQueue.GetMode(),
		))
		return
	}
	if resume.LastKnownCopyRound > 0 && startRound != resume.LastKnownCopyRound {
		logResumeCheck("info", fmt.Sprintf(
			"copy resume: derived start round %d (suspended at %d, copy pass %d)",
			startRound, resume.LastKnownCopyRound, resume.CopyPass,
		))
	}
}

func logResumeCheck(level, message string) {
	if logservice.LS == nil {
		return
	}
	_ = logservice.LS.Log(level, message, "migration", "resume-check", "")
}

// softSuspendWaitContext cancels when ShutdownContext is canceled (force abort / process shutdown).
// Soft stop itself has no wall-clock kill timer; use Abort() for hard kill.
func softSuspendWaitContext(shutdown context.Context) (context.Context, context.CancelFunc) {
	parent := shutdown
	if parent == nil {
		parent = context.Background()
	}
	return context.WithCancel(parent)
}

func softSuspendHardKilled(shutdown context.Context) bool {
	return shutdown != nil && shutdown.Err() != nil
}

func errIfSoftSuspendHardKilled(shutdown context.Context) error {
	if !softSuspendHardKilled(shutdown) {
		return nil
	}
	return fmt.Errorf("soft suspend aborted by force stop: %w", shutdown.Err())
}

// awaitSoftSuspendDB runs fn but returns immediately when hard kill cancels waitCtx/shutdown.
// An in-flight DB op may still finish in the background; hard kill must not block the run loop on it.
func awaitSoftSuspendDB(waitCtx, shutdown context.Context, fn func() error) error {
	if waitCtx == nil {
		waitCtx = context.Background()
	}
	if err := errIfSoftSuspendHardKilled(shutdown); err != nil {
		return err
	}
	if err := waitCtx.Err(); err != nil {
		if kill := errIfSoftSuspendHardKilled(shutdown); kill != nil {
			return kill
		}
		return err
	}
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		if kill := errIfSoftSuspendHardKilled(shutdown); kill != nil {
			return kill
		}
		return err
	case <-waitCtx.Done():
		if kill := errIfSoftSuspendHardKilled(shutdown); kill != nil {
			return kill
		}
		return waitCtx.Err()
	}
}

func abandonQueuesDBOnly(queues ...*queue.Queue) {
	for _, q := range queues {
		if q == nil {
			continue
		}
		q.Spin.AbandonDBOnly.Store(true)
		q.ClearPendingBufferForSuspend()
		q.RequestForceCheckoutAllWorkersForStop()
		q.CancelBusyWorkerContexts()
		q.AbandonInProgressTasks()
		q.ClearPendingBufferForSuspend()
	}
}

func (m *Migration) disarmStopGraceTimer() {
	m.stopGraceMu.Lock()
	defer m.stopGraceMu.Unlock()
	if m.stopGraceTimer != nil {
		m.stopGraceTimer.Stop()
		m.stopGraceTimer = nil
	}
}
