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

// RuntimeSuspendV1 is merged into migrations.runtime_state_json under key "suspend_v1".
// It holds only what resume needs: tuning knobs and last-known position for UI / max-depth.
type RuntimeSuspendV1 struct {
	Version int `json:"version"` // 1

	SuspendedAtUnix int64  `json:"suspended_at_unix"`
	Kind            string `json:"kind"` // "traversal" | "copy"

	WorkerCount int `json:"worker_count"`
	MaxRetries  int `json:"max_retries"`

	// Queue pull sizing (0 in JSON means "use engine default" on resume).
	LeaseBatchSize  int `json:"lease_batch_size,omitempty"`
	RefillBatchSize int `json:"refill_batch_size,omitempty"`
	ObserverPollMs  int `json:"observer_poll_ms,omitempty"`
	ProgressTickMs  int `json:"progress_tick_ms,omitempty"`

	// Traversal / coordinator
	LastRoundSrc  int `json:"last_round_src,omitempty"`
	LastRoundDst  int `json:"last_round_dst,omitempty"`
	MaxKnownDepth int `json:"max_known_depth,omitempty"`

	// Copy
	CopyPass           int `json:"copy_pass,omitempty"`
	LastKnownCopyRound int `json:"last_known_copy_round,omitempty"`
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
	}
}

func newCopySuspendState(
	workerCount, maxRetries, leaseBatch, refillBatch, observerPollMs, progressTickMs int,
	copyPass, lastRound, maxKnownDepth int,
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

	mergedCtx, cancelMerged := mergeWaitContexts(waitCtx, cfg.ShutdownContext)
	defer cancelMerged()

	if err := srcQueue.WaitInProgressZero(mergedCtx, 50*time.Millisecond); err != nil {
		if cfg.ShutdownContext != nil && cfg.ShutdownContext.Err() != nil {
			srcQueue.AbandonInProgressTasks()
			dstQueue.AbandonInProgressTasks()
		}
		return RuntimeStats{}, RuntimeSuspendV1{}, fmt.Errorf("src queue drain in-flight: %w", err)
	}
	if err := dstQueue.WaitInProgressZero(mergedCtx, 50*time.Millisecond); err != nil {
		if cfg.ShutdownContext != nil && cfg.ShutdownContext.Err() != nil {
			srcQueue.AbandonInProgressTasks()
			dstQueue.AbandonInProgressTasks()
		}
		return RuntimeStats{}, RuntimeSuspendV1{}, fmt.Errorf("dst queue drain in-flight: %w", err)
	}

	if err := database.Flush(); err != nil {
		return RuntimeStats{}, RuntimeSuspendV1{}, fmt.Errorf("flush seal buffer: %w", err)
	}
	if err := database.RebuildCurrentByDepth(srcQueue.GetRound()); err != nil {
		return RuntimeStats{}, RuntimeSuspendV1{}, fmt.Errorf("rebuild src current: %w", err)
	}
	if err := database.RebuildCurrentByDepth(dstQueue.GetRound()); err != nil {
		return RuntimeStats{}, RuntimeSuspendV1{}, fmt.Errorf("rebuild dst current: %w", err)
	}
	if err := queue.FinalizeCopyWorkOnStop(database, coordinator); err != nil {
		return RuntimeStats{}, RuntimeSuspendV1{}, fmt.Errorf("finalize copy work on stop: %w", err)
	}
	if err := database.CheckpointWithRetry(mergedCtx, 5); err != nil {
		return RuntimeStats{}, RuntimeSuspendV1{}, fmt.Errorf("checkpoint: %w", err)
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
	)

	stats := RuntimeStats{
		Duration: time.Since(start),
		Src:      srcStats,
		Dst:      dstStats,
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

	mergedCtx, cancelMerged := mergeWaitContexts(waitCtx, cfg.ShutdownContext)
	defer cancelMerged()

	graceCtx, graceCancel := context.WithTimeout(mergedCtx, queue.DefaultSpinDownGrace)
	defer graceCancel()
	if err := copyQueue.WaitInProgressZero(graceCtx, 50*time.Millisecond); err != nil {
		// After grace: force-checkout smallest file workers (freeze callback also does this).
		copyQueue.RequestForceCheckoutAllWorkersForStop()
		drainCtx, drainCancel := context.WithTimeout(mergedCtx, 5*time.Second)
		defer drainCancel()
		if err2 := copyQueue.WaitInProgressZero(drainCtx, 50*time.Millisecond); err2 != nil {
			copyQueue.AbandonInProgressTasks()
		}
	}
	copyQueue.Spin.AbandonDBOnly.Store(false)

	if err := database.Flush(); err != nil {
		return queue.QueueStats{}, RuntimeSuspendV1{}, fmt.Errorf("flush seal buffer: %w", err)
	}
	if err := database.RebuildCurrentByDepth(copyQueue.GetRound()); err != nil {
		return queue.QueueStats{}, RuntimeSuspendV1{}, fmt.Errorf("rebuild current: %w", err)
	}
	if err := database.CheckpointWithRetry(mergedCtx, 5); err != nil {
		return queue.QueueStats{}, RuntimeSuspendV1{}, fmt.Errorf("checkpoint: %w", err)
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
	)

	return stats, suspend, nil
}

func logTraversalResumePositionCheck(resume *RuntimeSuspendV1, srcQueue, dstQueue *queue.Queue) {
	if resume == nil || srcQueue == nil || dstQueue == nil {
		return
	}
	if srcQueue.GetMode() != queue.QueueModeRetry || dstQueue.GetMode() != queue.QueueModeRetry {
		logResumeCheck("warning", fmt.Sprintf(
			"traversal resume: expected retry mode (src=%s dst=%s)",
			srcQueue.GetMode(), dstQueue.GetMode(),
		))
		return
	}
	srcRound := srcQueue.Stats().Round
	dstRound := dstQueue.Stats().Round
	if resume.LastRoundSrc > 0 && srcRound != resume.LastRoundSrc {
		logResumeCheck("info", fmt.Sprintf(
			"traversal resume: restarting at src round %d (suspended at %d) in retry mode",
			srcRound, resume.LastRoundSrc,
		))
	}
	if resume.LastRoundDst > 0 && dstRound != resume.LastRoundDst {
		logResumeCheck("info", fmt.Sprintf(
			"traversal resume: restarting at dst round %d (suspended at %d) in retry mode",
			dstRound, resume.LastRoundDst,
		))
	}
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

// DefaultStopGracePeriod is how long a soft-suspend drain may run before the run context is canceled.
const DefaultStopGracePeriod = 30 * time.Second

const softSuspendMaxWait = 4 * time.Minute

// softSuspendWaitContext bounds soft-suspend I/O; it is canceled when ShutdownContext is canceled.
func softSuspendWaitContext(shutdown context.Context) (context.Context, context.CancelFunc) {
	parent := shutdown
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, softSuspendMaxWait)
}

func (m *Migration) armStopGraceTimer(period time.Duration) {
	if period <= 0 {
		period = DefaultStopGracePeriod
	}
	m.stopGraceMu.Lock()
	defer m.stopGraceMu.Unlock()
	if m.stopGraceTimer != nil {
		m.stopGraceTimer.Stop()
	}
	m.stopGraceTimer = time.AfterFunc(period, func() {
		_, _ = m.ForceStop()
	})
}

func (m *Migration) disarmStopGraceTimer() {
	m.stopGraceMu.Lock()
	defer m.stopGraceMu.Unlock()
	if m.stopGraceTimer != nil {
		m.stopGraceTimer.Stop()
		m.stopGraceTimer = nil
	}
}
