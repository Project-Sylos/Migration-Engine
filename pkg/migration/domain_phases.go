// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// AddRoots seeds source and destination root tasks into the migration DB.
// This is the explicit root insert step before starting traversal.
// prep may inject UI-reviewed depth-1 children so queues start at round 1.
func (m *Migration) AddRoots(srcRoot, dstRoot types.Folder, prep RootPreparation) (RootSeedSummary, error) {
	if m.Phase() != PhaseCreated {
		return RootSeedSummary{}, fmt.Errorf("add roots requires created phase")
	}
	normalizedSrc, err := normalizeRootFolder(srcRoot)
	if err != nil {
		return RootSeedSummary{}, fmt.Errorf("source root: %w", err)
	}
	normalizedDst, err := normalizeRootFolder(dstRoot)
	if err != nil {
		return RootSeedSummary{}, fmt.Errorf("destination root: %w", err)
	}
	m.mu.RLock()
	srcProv, dstProv, profile, winCompat := m.pathCheckSrcProvider, m.pathCheckDstProvider, m.pathCheckProfile, m.windowsCompatFlag
	m.mu.RUnlock()
	summary, err := SeedRootTasksWithPreparation(normalizedSrc, normalizedDst, m.DB, prep, srcProv, dstProv, profile, winCompat, m.FilterRuleset())
	if err != nil {
		return RootSeedSummary{}, fmt.Errorf("seed roots: %w", err)
	}
	err = m.store.updateUpdatedAt(m.ID)
	if err != nil {
		return RootSeedSummary{}, fmt.Errorf("update updated at: %w", err)
	}
	if err := m.transitionTo(PhaseFiltersSet); err != nil {
		return RootSeedSummary{}, err
	}
	return summary, nil
}

// phaseKind names the phase-specific constants runPhase needs: the soft-suspend sentinel,
// the suspended phase it transitions to, and the finalize phases it completes through.
type phaseKind struct {
	label          string
	sentinel       error
	suspended      string
	finalizing     string
	finalizeFailed string
	review         string
}

var (
	traversalPhaseKind = phaseKind{"traversal", ErrTraversalSoftSuspended, PhaseTraversalSuspended, PhaseTraversalFinalizing, PhaseTraversalFinalizeFailed, PhaseTraversalReview}
	copyPhaseKind      = phaseKind{"copy", ErrCopySoftSuspended, PhaseCopySuspended, PhaseCopyFinalizing, PhaseCopyFinalizeFailed, PhaseCopyReview}
	deletePhaseKind    = phaseKind{"delete", ErrDeleteSoftSuspended, PhaseDeleteSuspended, PhaseDeleteFinalizing, PhaseDeleteFinalizeFailed, PhaseDeleteReview}
)

// phaseRuntime builds the shared run knobs from cfg, applying o over it.
func (m *Migration) phaseRuntime(cfg Config, o RunOverrides, runCtx context.Context) PhaseRuntime {
	return PhaseRuntime{
		WorkerCount:          effectiveInt(o.WorkerCount, cfg.WorkerCount),
		MaxRetries:           effectiveInt(o.MaxRetries, cfg.MaxRetries),
		LogAddress:           cmp.Or(o.LogAddress, cfg.LogAddress),
		LogLevel:             cmp.Or(o.LogLevel, cfg.LogLevel),
		PersistLogLevel:      cmp.Or(o.PersistLogLevel, cfg.PersistLogLevel, "trace"),
		SkipListener:         o.SkipListener || cfg.SkipListener,
		StartupDelay:         cfg.StartupDelay,
		ProgressTick:         cfg.ProgressTick,
		ShutdownContext:      runCtx,
		SoftSuspendRequested: func() bool { return m.softSuspendRequested.Load() },
		ReportStopProgress:   m.reportStopProgress,
		OnQueueObserver:      func(obs *observe.QueueObserver) { m.activeQueueObs.Store(obs) },
		OnAutoscaler:         m.bindAutoscaler,
		Autoscaler:           m.resolveAutoscalerConfig(cfg.Autoscaler),
		SrcService:           cfg.Source,
		DstService:           cfg.Destination,
		PathCheckTarget:      cfg.PathCheckTarget,
		WindowsCompat:        cfg.WindowsCompat,
		OverwritePolicy:       NormalizeOverwritePolicy(cfg.OverwritePolicy),
		HashVerify:            cfg.HashVerify,
		StallTimeoutsSet:      cfg.StallTimeoutsSet,
		StallTimeoutTraversal: cfg.StallTimeoutTraversal,
		StallTimeoutDelete:    cfg.StallTimeoutDelete,
		StallTimeoutCopy:      cfg.StallTimeoutCopy,
	}
}

// loadResume returns the persisted suspend payload when prevPhase is kind's suspended phase and the payload matches.
func (m *Migration) loadResume(prevPhase string, kind phaseKind) *RuntimeSuspendV1 {
	if prevPhase != kind.suspended {
		return nil
	}
	if s, ok := parseRuntimeSuspendV1(m.runtimeStateJSON()); ok && s.Kind == kind.label {
		return &s
	}
	return nil
}

// handleSoftSuspend turns a soft suspend error into the suspended phase. handled is false when err is not
// a soft suspend of kind. A force abort during the transition is not an error.
func handleSoftSuspend[S any](m *Migration, err error, kind phaseKind) (stats S, handled bool, retErr error) {
	stats, sus, ok := AsSuspended[S](err, kind.sentinel)
	if !ok {
		return stats, false, nil
	}
	patch, patchErr := suspendRuntimeMergePatch(sus)
	if patchErr == nil {
		patchErr = m.store.updateRuntimeState(m.ID, patch)
	}
	if patchErr != nil {
		patchErr = fmt.Errorf("persist %s suspend state: %w", kind.label, patchErr)
	}
	transitionErr := m.transitionTo(kind.suspended)
	if transitionErr != nil && m.Phase() == PhaseAborted {
		transitionErr = nil
	}
	m.softSuspendRequested.Store(false)
	_ = m.ClearUncleanInterrupt() // soft-stop checkpoint replaces recovery mode
	m.refreshRuntimeState()
	return stats, true, errors.Join(patchErr, transitionErr)
}

// runPhase runs one phase after the caller has validated inputs and transitioned to the active phase:
// begin the run, persist cfg, execute run, then either suspend, honor an abort, or finalize durably.
func runPhase[S any](m *Migration, cfg Config, o RunOverrides, kind phaseKind, run func(PhaseRuntime) (S, error)) (S, error) {
	var zero S
	// Mark live immediately so concurrent GetMigration skips syncRecord and cannot race
	// a stale snapshot over this phase before the run starts.
	runCtx, cancelRun := m.beginRun(cfg.ShutdownContext)
	defer func() {
		cancelRun()
		m.endRun()
		if updErr := m.store.updateUpdatedAt(m.ID); updErr != nil {
			fmt.Println("error updating updated at", updErr)
		}
	}()
	if err := m.UpdateConfig(cfg); err != nil {
		return zero, fmt.Errorf("persist migration config: %w", err)
	}
	m.setLastRunConfig(cfg)

	stats, err := run(m.phaseRuntime(cfg, o, runCtx))
	if err != nil {
		if suspended, handled, suspendErr := handleSoftSuspend[S](m, err, kind); handled {
			return suspended, suspendErr
		}
		// Hard stop may land on aborted (legacy) or matching *-suspended + unclean.
		if m.Phase() == PhaseAborted || m.Phase() == kind.suspended {
			return zero, nil
		}
		return zero, err
	}
	if err := m.completeDurableFinalize(kind.finalizing, kind.finalizeFailed, kind.review, FinalizeOptions{}); err != nil {
		m.refreshRuntimeState()
		return stats, err
	}
	return stats, nil
}

// StartTraversal begins traversal lifecycle and transitions to awaiting-traversal-review on success.
// Requires filters-set, or traversal-suspended to resume after soft suspend.
func (m *Migration) StartTraversal(cfg Config) (RuntimeStats, error) {
	prevPhase := m.Phase()
	if prevPhase != PhaseFiltersSet && prevPhase != PhaseTraversalSuspended && prevPhase != PhaseCreated {
		return RuntimeStats{}, fmt.Errorf("start traversal requires filters-set or traversal-suspended phase, got %s", prevPhase)
	}
	srcRoot, err := normalizeRootFolder(cfg.Source.Root)
	if err != nil {
		return RuntimeStats{}, fmt.Errorf("source root: %w", err)
	}
	dstRoot, err := normalizeRootFolder(cfg.Destination.Root)
	if err != nil {
		return RuntimeStats{}, fmt.Errorf("destination root: %w", err)
	}
	if err := m.transitionTo(PhaseTraversing); err != nil {
		return RuntimeStats{}, err
	}

	cfgForRun := cfg
	cfgForRun.Source.Root = srcRoot
	cfgForRun.Destination.Root = dstRoot
	return runPhase(m, cfgForRun, RunOverrides{}, traversalPhaseKind, func(rt PhaseRuntime) (RuntimeStats, error) {
		resume := m.loadResume(prevPhase, traversalPhaseKind)
		unclean := m.HasUncleanInterrupt()
		if resume == nil && prevPhase == PhaseTraversalSuspended {
			resume = &RuntimeSuspendV1{Version: 1, Kind: "traversal"}
			unclean = true
			_ = m.SetUncleanInterrupt("traversal")
		}
		return RunMigration(MigrationConfig{
			PhaseRuntime:    rt,
			DB:              m.DB,
			SrcAdapter:      cfg.Source.Adapter,
			DstAdapter:      cfg.Destination.Adapter,
			SrcRoot:         srcRoot,
			DstRoot:         dstRoot,
			SrcServiceName:  cfg.Source.Name,
			ResumeTraversal: resume,
			RootPreparation: cfg.RootPreparation,
			UncleanResume:   unclean,
			OnRecoveryDone: func() {
				_ = m.ClearUncleanInterrupt()
			},
		})
	})
}

// StartCopy transitions review->copying and runs copy phase. cfg must include live source/destination adapters (same as StartTraversal).
// Requires awaiting-traversal-review or copy-suspended (resume after soft suspend).
func (m *Migration) StartCopy(cfg Config) (queue.QueueStats, error) {
	prevPhase := m.Phase()
	validCopyStartPhases := []string{PhaseTraversalReview, PhaseCopySuspended, PhaseCopyReview}
	if !slices.Contains(validCopyStartPhases, prevPhase) {
		return queue.QueueStats{}, fmt.Errorf("start copy requires awaiting-traversal-review or copy-suspended phase, got %s", prevPhase)
	}
	if prevPhase == PhaseTraversalReview {
		snap, err := stats.GetReviewStatsSnapshot(m.DB)
		if err != nil {
			return queue.QueueStats{}, fmt.Errorf("read review stats before copy: %w", err)
		}
		if snap.TraversalPendingRetry > 0 {
			return queue.QueueStats{}, fmt.Errorf("start copy requires retry of %d pending discovery items first", snap.TraversalPendingRetry)
		}
	}
	if cfg.Source.Adapter == nil || cfg.Destination.Adapter == nil {
		return queue.QueueStats{}, fmt.Errorf("copy requires source and destination adapters in config")
	}
	if err := m.transitionTo(PhaseCopying); err != nil {
		return queue.QueueStats{}, err
	}
	return runPhase(m, cfg, RunOverrides{}, copyPhaseKind, func(rt PhaseRuntime) (queue.QueueStats, error) {
		resume := m.loadResume(prevPhase, copyPhaseKind)
		unclean := m.HasUncleanInterrupt()
		if resume == nil && prevPhase == PhaseCopySuspended {
			unclean = true
			_ = m.SetUncleanInterrupt("copy")
		}
		return RunCopyPhase(CopyPhaseConfig{
			PhaseRuntime:   rt,
			DB:             m.DB,
			SrcAdapter:     cfg.Source.Adapter,
			DstAdapter:     cfg.Destination.Adapter,
			ResumeCopy:     resume,
			UncleanResume:  unclean,
			OnRecoveryDone: func() { _ = m.ClearUncleanInterrupt() },
		})
	})
}

// DeleteSummary holds counts and source info for the delete confirmation modal.
type DeleteSummary struct {
	SourceRootPath string
	Pending        int64
	Failed         int64
	Deleted        int64
}

// GetDeleteSummary returns delete status counts for confirmation UI.
// Pending/Failed/Deleted are limited to copy-complete SRC nodes (same
// population as source-cleanup Selected), not every node with a delete event.
func (m *Migration) GetDeleteSummary(sourceRootPath string) (DeleteSummary, error) {
	if m.DB == nil {
		return DeleteSummary{}, fmt.Errorf("database not available")
	}
	counts, err := stats.GetDeleteStatusCountsFromEvents(m.DB)
	if err != nil {
		return DeleteSummary{}, err
	}
	return DeleteSummary{
		SourceRootPath: sourceRootPath,
		Pending:        counts.Pending,
		Failed:         counts.Failed,
		Deleted:        counts.Deleted,
	}, nil
}

// StartDelete transitions copy-review->deleting and runs delete phase. Requires awaiting-copy-review or delete-suspended.
func (m *Migration) StartDelete(cfg Config) (queue.QueueStats, error) {
	prevPhase := m.Phase()
	validDeleteStartPhases := []string{PhaseCopyReview, PhaseDeleteSuspended}
	if !slices.Contains(validDeleteStartPhases, prevPhase) {
		return queue.QueueStats{}, fmt.Errorf("start delete requires awaiting-copy-review or delete-suspended phase, got %s", prevPhase)
	}
	if cfg.Source.Adapter == nil {
		return queue.QueueStats{}, fmt.Errorf("delete requires source adapter in config")
	}
	if err := m.transitionTo(PhaseDeleting); err != nil {
		return queue.QueueStats{}, err
	}
	if err := m.DB.NormalizeDeleteForestOps(); err != nil {
		return queue.QueueStats{}, fmt.Errorf("normalize delete forest before delete: %w", err)
	}
	return runPhase(m, cfg, RunOverrides{}, deletePhaseKind, func(rt PhaseRuntime) (queue.QueueStats, error) {
		resume := m.loadResume(prevPhase, deletePhaseKind)
		if resume == nil && prevPhase == PhaseDeleteSuspended {
			_ = m.SetUncleanInterrupt("delete")
		} else if m.HasUncleanInterrupt() {
			// keep unclean for delete frontier resume
		}
		return RunDeletePhase(DeletePhaseConfig{
			PhaseRuntime: rt,
			DB:           m.DB,
			SrcAdapter:   cfg.Source.Adapter,
			ResumeDelete: resume,
		})
	})
}

func (m *Migration) preparePhaseTransition(activePhase, reviewPhase, suspendedPhase, targetPhase, errMsg string) error {
	phase := m.Phase()
	if phase == activePhase {
		return nil
	}
	if phase != reviewPhase && phase != suspendedPhase {
		return fmt.Errorf("%s", errMsg)
	}
	return m.transitionTo(targetPhase)
}

// PreparePhaseKind selects which async phase prepare to run before returning HTTP 202.
type PreparePhaseKind string

const (
	PreparePhaseRetrySweep  PreparePhaseKind = "retry_sweep"
	PreparePhaseCopyRetry   PreparePhaseKind = "copy_retry"
	PreparePhaseDeleteRetry PreparePhaseKind = "delete_retry"
)

// PreparePhase transitions to the matching in-progress phase before an async retry/sweep.
// Call synchronously in the HTTP handler before returning 202 so polls see the correct phase
// before the background goroutine runs.
func (m *Migration) PreparePhase(kind PreparePhaseKind) error {
	switch kind {
	case PreparePhaseRetrySweep:
		return m.preparePhaseTransition(
			PhaseTraversing,
			PhaseTraversalReview,
			PhaseTraversalSuspended,
			PhaseTraversing,
			"prepare retry sweep requires awaiting-traversal-review or traversal-suspended phase",
		)
	case PreparePhaseCopyRetry:
		return m.preparePhaseTransition(
			PhaseCopying,
			PhaseCopyReview,
			PhaseCopySuspended,
			PhaseCopying,
			"prepare copy retry requires awaiting-copy-review or copy-suspended phase",
		)
	case PreparePhaseDeleteRetry:
		return m.preparePhaseTransition(
			PhaseDeleting,
			PhaseDeleteReview,
			PhaseDeleteSuspended,
			PhaseDeleting,
			"prepare delete retry requires awaiting-delete-review or delete-suspended phase",
		)
	default:
		return fmt.Errorf("unknown prepare phase kind %q", kind)
	}
}

// RunDeleteRetry runs delete retry for failed items only.
func (m *Migration) RunDeleteRetry(cfg Config, opts RunOverrides) (queue.QueueStats, error) {
	phase := m.Phase()
	if phase != PhaseDeleteReview && phase != PhaseDeleting && phase != PhaseDeleteSuspended {
		return queue.QueueStats{}, fmt.Errorf("delete retry requires awaiting-delete-review, delete-suspended, or prepared delete-in-progress")
	}
	if cfg.Source.Adapter == nil {
		return queue.QueueStats{}, fmt.Errorf("delete retry requires source adapter in config")
	}
	if phase == PhaseDeleteReview || phase == PhaseDeleteSuspended {
		if err := m.DB.NormalizeDeleteForestOps(); err != nil {
			return queue.QueueStats{}, fmt.Errorf("normalize delete forest before delete retry: %w", err)
		}
		if err := m.transitionTo(PhaseDeleting); err != nil {
			return queue.QueueStats{}, err
		}
	}
	return runPhase(m, cfg, opts, deletePhaseKind, func(rt PhaseRuntime) (queue.QueueStats, error) {
		return RunDeleteRetryPhase(DeletePhaseConfig{
			PhaseRuntime: rt,
			DB:           m.DB,
			SrcAdapter:   cfg.Source.Adapter,
		})
	})
}

// RunRetrySweep re-traverses failed or pending discovery items. opts.MaxKnownDepth 0 means auto-detect.
func (m *Migration) RunRetrySweep(cfg Config, opts RunOverrides) (RuntimeStats, error) {
	phase := m.Phase()
	if phase != PhaseTraversalReview && phase != PhaseTraversing && phase != PhaseTraversalSuspended {
		return RuntimeStats{}, fmt.Errorf("retry sweep requires awaiting-traversal-review, traversal-suspended, or prepared traversal-in-progress")
	}
	if cfg.Source.Adapter == nil || cfg.Destination.Adapter == nil {
		return RuntimeStats{}, fmt.Errorf("retry sweep requires source and destination adapters in config")
	}
	if phase == PhaseTraversalReview || phase == PhaseTraversalSuspended {
		if err := m.transitionTo(PhaseTraversing); err != nil {
			return RuntimeStats{}, err
		}
	}
	maxKnownDepth := -1
	if opts.MaxKnownDepth != 0 {
		maxKnownDepth = opts.MaxKnownDepth
	}
	return runPhase(m, cfg, opts, traversalPhaseKind, func(rt PhaseRuntime) (RuntimeStats, error) {
		return RunRetrySweep(SweepConfig{
			PhaseRuntime:  rt,
			DB:            m.DB,
			SrcAdapter:    cfg.Source.Adapter,
			DstAdapter:    cfg.Destination.Adapter,
			MaxKnownDepth: maxKnownDepth,
			FilterRuleset: cmp.Or(cfg.FilterRuleset, m.FilterRuleset()),
		})
	})
}

// RunCopyRetry runs the copy phase in retry mode (marked failed→pending items). On success transitions back to awaiting-copy-review.
func (m *Migration) RunCopyRetry(cfg Config, opts RunOverrides) (queue.QueueStats, error) {
	phase := m.Phase()
	if phase != PhaseCopyReview && phase != PhaseCopying && phase != PhaseCopySuspended {
		return queue.QueueStats{}, fmt.Errorf("copy retry requires awaiting-copy-review, copy-suspended, or prepared copy-in-progress")
	}
	if cfg.Source.Adapter == nil || cfg.Destination.Adapter == nil {
		return queue.QueueStats{}, fmt.Errorf("copy retry requires source and destination adapters in config")
	}
	if phase == PhaseCopyReview || phase == PhaseCopySuspended {
		if err := m.transitionTo(PhaseCopying); err != nil {
			return queue.QueueStats{}, err
		}
	}
	return runPhase(m, cfg, opts, copyPhaseKind, func(rt PhaseRuntime) (queue.QueueStats, error) {
		return RunCopyRetryPhase(CopyPhaseConfig{
			PhaseRuntime: rt,
			DB:           m.DB,
			SrcAdapter:   cfg.Source.Adapter,
			DstAdapter:   cfg.Destination.Adapter,
		})
	})
}

func (m *Migration) Stop() (StopResult, error) {
	m.mu.RLock()
	cancel := m.runCancel
	running := m.running
	phase := m.phase
	m.mu.RUnlock()

	result := StopResult{
		MigrationID:   m.ID,
		Phase:         m.Phase(),
		RuntimeStatus: m.GetRuntimeStatus(),
		Stopped:       running,
	}

	if !running {
		return result, nil
	}

	switch phase {
	case PhaseTraversing, PhaseCopying, PhaseDeleting:
		m.softSuspendRequested.Store(true)
		m.beginSoftStopProgress(phase)
		inProgress := m.pauseLiveQueues()
		m.setStopStepDetail(StopStepDraining, inProgress, drainWaitDetail(inProgress, "items"))
		m.startSoftStopProgressPump()
		result.SoftSuspendRequested = true
		return result, nil
	case PhaseTraversalFinalizing, PhaseCopyFinalizing, PhaseDeleteFinalizing:
		// Soft-drain does not apply; abort the fold/checkpoint so Resume can retry.
		return m.ForceStop()
	default:
		if cancel != nil {
			cancel()
		}
		return result, nil
	}
}

// ForceStop cancels the active run context and abandons in-flight queue work.
// Prefer soft Stop; use Abort/ForceStop only when soft stop is stuck.
func (m *Migration) ForceStop() (StopResult, error) {
	m.mu.RLock()
	cancel := m.runCancel
	running := m.running
	m.mu.RUnlock()

	result := StopResult{
		MigrationID:   m.ID,
		Phase:         m.Phase(),
		RuntimeStatus: m.GetRuntimeStatus(),
		Stopped:       running,
		ForceStopped:  running,
	}

	if !running {
		return result, nil
	}

	m.disarmStopGraceTimer()
	m.softSuspendRequested.Store(false)
	m.beginForceStopProgress()
	_ = m.pauseLiveQueues()
	if o := m.activeQueueObs.Load(); o != nil {
		o.AbandonRegisteredQueues()
	}
	// Abort durable seal teardown immediately so soft-stop Flush/rebuild/checkpoint cannot continue.
	if m.DB != nil {
		m.DB.AbortTraversalPhase()
	}
	if cancel != nil {
		cancel()
	}
	return result, nil
}

// Abort force-stops the live run and transitions to the matching *-suspended phase with
// unclean_interrupt so Resume can run a recovery sweep. Prefer soft Stop when possible.
func (m *Migration) Abort() (StopResult, error) {
	phaseBefore := m.Phase()
	kind := uncleanKindForPhase(phaseBefore)
	result, err := m.ForceStop()
	if err != nil {
		return result, err
	}
	target := suspendedPhaseForInProgress(phaseBefore)
	if target == "" && kind != "" {
		target = suspendedPhaseForKind(kind)
	}
	if target != "" {
		if kind != "" {
			_ = m.SetUncleanInterrupt(kind)
		}
		if e := m.transitionTo(target); e != nil {
			// Fall back to legacy aborted if suspend transition fails mid-race.
			if e2 := m.transitionTo(PhaseAborted); e2 == nil {
				result.Phase = PhaseAborted
				if kind != "" {
					_ = m.SetUncleanInterrupt(kind)
				}
			} else {
				return result, e
			}
		} else {
			result.Phase = target
		}
	}
	m.setStopStepDetail(StopStepAborted, 0, "")
	m.refreshRuntimeState()
	result.RuntimeStatus = m.GetRuntimeStatus()
	return result, nil
}
