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

type traversalSuspendedError struct {
	stats   RuntimeStats
	suspend RuntimeSuspendV1
}

func (e *traversalSuspendedError) Error() string { return ErrTraversalSoftSuspended.Error() }

func (e *traversalSuspendedError) Unwrap() error { return ErrTraversalSoftSuspended }

func newTraversalSuspendedError(stats RuntimeStats, s RuntimeSuspendV1) error {
	return &traversalSuspendedError{stats: stats, suspend: s}
}

// AsTraversalSuspended unwraps stats and suspend payload from RunMigration / RunRetrySweep soft suspend.
func AsTraversalSuspended(err error) (RuntimeStats, RuntimeSuspendV1, bool) {
	var te *traversalSuspendedError
	if errors.As(err, &te) {
		return te.stats, te.suspend, true
	}
	return RuntimeStats{}, RuntimeSuspendV1{}, false
}

type copySuspendedError struct {
	stats   queue.QueueStats
	suspend RuntimeSuspendV1
}

func (e *copySuspendedError) Error() string { return ErrCopySoftSuspended.Error() }

func (e *copySuspendedError) Unwrap() error { return ErrCopySoftSuspended }

func newCopySuspendedError(stats queue.QueueStats, s RuntimeSuspendV1) error {
	return &copySuspendedError{stats: stats, suspend: s}
}

// AsCopySuspended unwraps stats and suspend payload from RunCopyPhase soft suspend.
func AsCopySuspended(err error) (queue.QueueStats, RuntimeSuspendV1, bool) {
	var ce *copySuspendedError
	if errors.As(err, &ce) {
		return ce.stats, ce.suspend, true
	}
	return queue.QueueStats{}, RuntimeSuspendV1{}, false
}

type deleteSuspendedError struct {
	stats   queue.QueueStats
	suspend RuntimeSuspendV1
}

func (e *deleteSuspendedError) Error() string { return ErrDeleteSoftSuspended.Error() }

func (e *deleteSuspendedError) Unwrap() error { return ErrDeleteSoftSuspended }

func newDeleteSuspendedError(stats queue.QueueStats, s RuntimeSuspendV1) error {
	return &deleteSuspendedError{stats: stats, suspend: s}
}

// AsDeleteSuspended unwraps stats and suspend payload from RunDeletePhase soft suspend.
func AsDeleteSuspended(err error) (queue.QueueStats, RuntimeSuspendV1, bool) {
	var de *deleteSuspendedError
	if errors.As(err, &de) {
		return de.stats, de.suspend, true
	}
	return queue.QueueStats{}, RuntimeSuspendV1{}, false
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

func newTraversalSuspendState(
	workerCount, maxRetries, leaseBatch, refillBatch, observerPollMs, progressTickMs int,
	lastRoundSrc, lastRoundDst, maxKnownDepth int,
	srcCursor, dstCursor string,
) RuntimeSuspendV1 {
	return RuntimeSuspendV1{
		Version:         1,
		SuspendedAtUnix: time.Now().Unix(),
		Kind:            "traversal",
		WorkerCount:     workerCount,
		MaxRetries:      maxRetries,
		LeaseBatchSize:  leaseBatch,
		RefillBatchSize: refillBatch,
		ObserverPollMs:  observerPollMs,
		ProgressTickMs:  progressTickMs,
		LastRoundSrc:    lastRoundSrc,
		LastRoundDst:    lastRoundDst,
		MaxKnownDepth:   maxKnownDepth,
		SrcKeysetCursor: srcCursor,
		DstKeysetCursor: dstCursor,
	}
}

func newCopySuspendState(
	workerCount, maxRetries, leaseBatch, refillBatch, observerPollMs, progressTickMs int,
	copyPass, lastRound, maxKnownDepth int,
	cursor string,
) RuntimeSuspendV1 {
	return RuntimeSuspendV1{
		Version:            1,
		SuspendedAtUnix:    time.Now().Unix(),
		Kind:               "copy",
		WorkerCount:        workerCount,
		MaxRetries:         maxRetries,
		LeaseBatchSize:     leaseBatch,
		RefillBatchSize:    refillBatch,
		ObserverPollMs:     observerPollMs,
		ProgressTickMs:     progressTickMs,
		CopyPass:           copyPass,
		LastKnownCopyRound: lastRound,
		MaxKnownDepth:      maxKnownDepth,
		CopyKeysetCursor:   cursor,
	}
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

func performTraversalSoftSuspend(
	waitCtx context.Context,
	database *db.DB,
	srcQueue, dstQueue *queue.Queue,
	observer *observe.QueueObserver,
	coordinator *queue.QueueCoordinator,
	cfg MigrationConfig,
	start time.Time,
	workerCount, maxRetries int,
) (RuntimeStats, RuntimeSuspendV1, error) {
	if observer != nil {
		observer.Stop()
	}
	srcQueue.SetState(queue.QueueStatePaused)
	dstQueue.SetState(queue.QueueStatePaused)
	srcQueue.StopWatchdog()
	dstQueue.StopWatchdog()
	srcQueue.ClearPendingBufferForSuspend()
	dstQueue.ClearPendingBufferForSuspend()

	setDetail := func(d string) {
		if observer != nil {
			observer.SetWaitReason(d)
		}
	}
	n0 := srcQueue.InProgressCount() + dstQueue.InProgressCount()
	reportStopProgressDetail(cfg.ReportStopProgress, StopStepDraining, n0, drainWaitDetail(n0, "items"), setDetail)

	mergedCtx, cancelMerged := mergeWaitContexts(waitCtx, cfg.ShutdownContext)
	defer cancelMerged()

	onDrainTick := func(n int) {
		reportStopProgressDetail(cfg.ReportStopProgress, StopStepDraining, n, drainWaitDetail(n, "items"), setDetail)
	}
	if err := srcQueue.WaitInProgressZeroFunc(mergedCtx, 50*time.Millisecond, onDrainTick); err != nil {
		if softSuspendHardKilled(cfg.ShutdownContext) {
			abandonQueuesDBOnly(srcQueue, dstQueue)
		}
		return RuntimeStats{}, RuntimeSuspendV1{}, fmt.Errorf("src queue drain in-flight: %w", err)
	}
	if err := dstQueue.WaitInProgressZeroFunc(mergedCtx, 50*time.Millisecond, onDrainTick); err != nil {
		if softSuspendHardKilled(cfg.ShutdownContext) {
			abandonQueuesDBOnly(srcQueue, dstQueue)
		}
		return RuntimeStats{}, RuntimeSuspendV1{}, fmt.Errorf("dst queue drain in-flight: %w", err)
	}
	// Hard kill may clear in-progress so Wait succeeds; do not continue soft-save.
	if err := errIfSoftSuspendHardKilled(cfg.ShutdownContext); err != nil {
		abandonQueuesDBOnly(srcQueue, dstQueue)
		return RuntimeStats{}, RuntimeSuspendV1{}, err
	}

	reportStopProgressDetail(cfg.ReportStopProgress, StopStepSaving, 0, "Writing the latest updates…", setDetail)

	if err := awaitSoftSuspendDB(mergedCtx, cfg.ShutdownContext, func() error {
		return database.Flush(context.Background())
	}); err != nil {
		return RuntimeStats{}, RuntimeSuspendV1{}, fmt.Errorf("flush seal buffer: %w", err)
	}
	if err := awaitSoftSuspendDB(mergedCtx, cfg.ShutdownContext, func() error {
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

	obsMs := int(cfg.ObserverPollInterval / time.Millisecond)
	if obsMs <= 0 {
		obsMs = 200
	}
	progMs := int(cfg.ProgressTick / time.Millisecond)
	if progMs <= 0 {
		progMs = 1000
	}

	suspend := newTraversalSuspendState(
		workerCount, maxRetries,
		srcQueue.EffectiveLeaseBatchSize(), srcQueue.EffectiveRefillBatchSize(),
		obsMs, progMs,
		srcStats.Round, dstStats.Round, maxD,
		srcQueue.GetKeysetCursor(), dstQueue.GetKeysetCursor(),
	)
	if database.Ops() != nil {
		_ = database.SaveTraversalQueuePositions(
			srcStats.Round, srcQueue.GetKeysetCursor(),
			dstStats.Round, dstQueue.GetKeysetCursor(),
		)
	}

	stats := RuntimeStats{
		Duration: time.Since(start),
		Src:      srcStats,
		Dst:      dstStats,
	}
	// StopStepDone is reported after seal stop + checkpoint (see finishSoftStopBulkPhase).
	if observer != nil {
		observer.SetWaitReason("Saving buffered progress…")
	}
	return stats, suspend, nil
}

func performCopySoftSuspend(
	waitCtx context.Context,
	database *db.DB,
	copyQueue *queue.Queue,
	observer *observe.QueueObserver,
	cfg CopyPhaseConfig,
	workerCount, maxRetries int,
) (queue.QueueStats, RuntimeSuspendV1, error) {
	if observer != nil {
		observer.Stop()
	}
	copyQueue.SetState(queue.QueueStatePaused)
	copyQueue.StopWatchdog()
	copyQueue.ClearPendingBufferForSuspend()
	copyQueue.EnterStopAbandonWindow(queue.DefaultSpinDownGrace)

	setDetail := func(d string) {
		if observer != nil {
			observer.SetWaitReason(d)
		}
	}
	n0 := copyQueue.InProgressCount()
	reportStopProgressDetail(cfg.ReportStopProgress, StopStepDraining, n0, drainWaitDetail(n0, "copies"), setDetail)

	mergedCtx, cancelMerged := mergeWaitContexts(waitCtx, cfg.ShutdownContext)
	defer cancelMerged()

	onDrainTick := func(n int) {
		reportStopProgressDetail(cfg.ReportStopProgress, StopStepDraining, n, drainWaitDetail(n, "copies"), setDetail)
	}
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
	if err := errIfSoftSuspendHardKilled(cfg.ShutdownContext); err != nil {
		copyQueue.Spin.AbandonDBOnly.Store(true)
		copyQueue.ClearPendingBufferForSuspend()
		copyQueue.AbandonInProgressTasks()
		return queue.QueueStats{}, RuntimeSuspendV1{}, err
	}
	copyQueue.Spin.AbandonDBOnly.Store(false)

	reportStopProgressDetail(cfg.ReportStopProgress, StopStepSaving, 0, "Writing the latest updates…", setDetail)

	if err := awaitSoftSuspendDB(mergedCtx, cfg.ShutdownContext, func() error {
		return database.Flush(context.Background())
	}); err != nil {
		return queue.QueueStats{}, RuntimeSuspendV1{}, fmt.Errorf("flush seal buffer: %w", err)
	}
	setDetail("Updating where things left off…")
	if err := awaitSoftSuspendDB(mergedCtx, cfg.ShutdownContext, func() error {
		return database.Flush(context.Background())
	}); err != nil {
		return queue.QueueStats{}, RuntimeSuspendV1{}, fmt.Errorf("flush seal buffer: %w", err)
	}

	stats := copyQueue.Stats()
	obsMs := int(cfg.ObserverPollInterval / time.Millisecond)
	if obsMs <= 0 {
		obsMs = 200
	}
	progMs := int(cfg.ProgressTick / time.Millisecond)
	if progMs <= 0 {
		progMs = 2000
	}

	suspend := newCopySuspendState(
		workerCount, maxRetries,
		copyQueue.EffectiveLeaseBatchSize(), copyQueue.EffectiveRefillBatchSize(),
		obsMs, progMs,
		copyQueue.GetCopyPass(), stats.Round, copyQueue.GetMaxKnownDepth(),
		copyQueue.GetKeysetCursor(),
	)
	if database.Ops() != nil {
		_ = database.SaveCopyQueuePosition(stats.Round, copyQueue.GetKeysetCursor())
	}

	if observer != nil {
		observer.SetWaitReason("Saving buffered progress…")
	}
	return stats, suspend, nil
}

func newDeleteSuspendState(
	workerCount, maxRetries, leaseBatch, refillBatch, observerPollMs, progressTickMs int,
	deletePass, lastRound, maxKnownDepth int,
	cursor string,
) RuntimeSuspendV1 {
	return RuntimeSuspendV1{
		Version:              1,
		SuspendedAtUnix:      time.Now().Unix(),
		Kind:                 "delete",
		WorkerCount:          workerCount,
		MaxRetries:           maxRetries,
		LeaseBatchSize:       leaseBatch,
		RefillBatchSize:      refillBatch,
		ObserverPollMs:       observerPollMs,
		ProgressTickMs:       progressTickMs,
		CopyPass:             deletePass,
		LastKnownDeleteRound: lastRound,
		MaxKnownDepth:        maxKnownDepth,
		CopyKeysetCursor:     cursor,
	}
}

func performDeleteSoftSuspend(
	waitCtx context.Context,
	database *db.DB,
	deleteQueue *queue.Queue,
	observer *observe.QueueObserver,
	cfg DeletePhaseConfig,
	workerCount, maxRetries int,
) (queue.QueueStats, RuntimeSuspendV1, error) {
	if observer != nil {
		observer.Stop()
	}
	deleteQueue.SetState(queue.QueueStatePaused)
	deleteQueue.StopWatchdog()
	deleteQueue.ClearPendingBufferForSuspend()

	setDetail := func(d string) {
		if observer != nil {
			observer.SetWaitReason(d)
		}
	}
	n0 := deleteQueue.InProgressCount()
	reportStopProgressDetail(cfg.ReportStopProgress, StopStepDraining, n0, drainWaitDetail(n0, "removals"), setDetail)

	mergedCtx, cancelMerged := mergeWaitContexts(waitCtx, cfg.ShutdownContext)
	defer cancelMerged()

	onDrainTick := func(n int) {
		reportStopProgressDetail(cfg.ReportStopProgress, StopStepDraining, n, drainWaitDetail(n, "removals"), setDetail)
	}
	if err := deleteQueue.WaitInProgressZeroFunc(mergedCtx, 50*time.Millisecond, onDrainTick); err != nil {
		if softSuspendHardKilled(cfg.ShutdownContext) {
			deleteQueue.Spin.AbandonDBOnly.Store(true)
			deleteQueue.AbandonInProgressTasks()
		}
		return queue.QueueStats{}, RuntimeSuspendV1{}, fmt.Errorf("delete queue drain in-flight: %w", err)
	}
	if err := errIfSoftSuspendHardKilled(cfg.ShutdownContext); err != nil {
		deleteQueue.Spin.AbandonDBOnly.Store(true)
		deleteQueue.ClearPendingBufferForSuspend()
		deleteQueue.AbandonInProgressTasks()
		return queue.QueueStats{}, RuntimeSuspendV1{}, err
	}

	reportStopProgressDetail(cfg.ReportStopProgress, StopStepSaving, 0, "Writing the latest updates…", setDetail)

	if err := awaitSoftSuspendDB(mergedCtx, cfg.ShutdownContext, func() error {
		return database.Flush(context.Background())
	}); err != nil {
		return queue.QueueStats{}, RuntimeSuspendV1{}, fmt.Errorf("flush seal buffer: %w", err)
	}
	setDetail("Updating where things left off…")
	if err := awaitSoftSuspendDB(mergedCtx, cfg.ShutdownContext, func() error {
		return database.Flush(context.Background())
	}); err != nil {
		return queue.QueueStats{}, RuntimeSuspendV1{}, fmt.Errorf("flush seal buffer: %w", err)
	}

	stats := deleteQueue.Stats()
	obsMs := int(cfg.ObserverPollInterval / time.Millisecond)
	if obsMs <= 0 {
		obsMs = 200
	}
	progMs := int(cfg.ProgressTick / time.Millisecond)
	if progMs <= 0 {
		progMs = 2000
	}

	suspend := newDeleteSuspendState(
		workerCount, maxRetries,
		deleteQueue.EffectiveLeaseBatchSize(), deleteQueue.EffectiveRefillBatchSize(),
		obsMs, progMs,
		deleteQueue.GetCopyPass(), stats.Round, deleteQueue.GetMaxKnownDepth(),
		deleteQueue.GetKeysetCursor(),
	)
	if database.Ops() != nil {
		_ = database.SaveDeleteQueuePosition(stats.Round, deleteQueue.GetKeysetCursor())
	}
	if observer != nil {
		observer.SetWaitReason("Saving buffered progress…")
	}
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
// An in-flight DuckDB op may still finish in the background; hard kill must not block the run loop on it.
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
