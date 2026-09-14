// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"errors"
	"fmt"
	"slices"

	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
	"codeberg.org/Sylos/Migration-Engine/pkg/filter"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func firstFilter(a, b *filter.CompiledRuleset) *filter.CompiledRuleset {
	if a != nil {
		return a
	}
	return b
}

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

// StartTraversal begins traversal lifecycle and transitions to awaiting-traversal-review on success.
// Requires filters-set, or traversal-suspended to resume after soft suspend.
func (m *Migration) StartTraversal(cfg Config) (RuntimeStats, error) {
	prevPhase := m.Phase()
	if prevPhase != PhaseFiltersSet && prevPhase != PhaseTraversalSuspended && prevPhase != PhaseCreated {
		return RuntimeStats{}, fmt.Errorf("start traversal requires filters-set or traversal-suspended phase, got %s", prevPhase)
	}
	if err := m.transitionTo(PhaseTraversing); err != nil {
		return RuntimeStats{}, err
	}

	// Mark live immediately so concurrent GetMigration skips syncRecord and cannot race
	// a stale filters-set snapshot over this phase before RunMigration starts.
	runCtx, cancelRun := m.beginRun(cfg.ShutdownContext)
	defer func() {
		cancelRun()
		m.endRun()
		if updErr := m.store.updateUpdatedAt(m.ID); updErr != nil {
			fmt.Println("error updating updated at", updErr)
		}
	}()

	var resume *RuntimeSuspendV1
	if prevPhase == PhaseTraversalSuspended {
		if s, ok := parseRuntimeSuspendV1(m.runtimeStateJSON()); ok && s.Kind == "traversal" {
			resume = &s
		} else {
			resume = &RuntimeSuspendV1{Version: 1, Kind: "traversal"}
		}
	}

	srcRoot, err := normalizeRootFolder(cfg.Source.Root)
	if err != nil {
		return RuntimeStats{}, fmt.Errorf("source root: %w", err)
	}
	dstRoot, err := normalizeRootFolder(cfg.Destination.Root)
	if err != nil {
		return RuntimeStats{}, fmt.Errorf("destination root: %w", err)
	}

	cfgForRun := cfg
	cfgForRun.Source.Root = srcRoot
	cfgForRun.Destination.Root = dstRoot
	if err := m.UpdateConfig(cfgForRun); err != nil {
		return RuntimeStats{}, fmt.Errorf("persist migration config: %w", err)
	}
	m.setLastRunConfig(cfgForRun)

	stats, err := RunMigration(MigrationConfig{
		DB:                   m.DB,
		DBPath:               cfg.Database.Path,
		SrcAdapter:           cfg.Source.Adapter,
		DstAdapter:           cfg.Destination.Adapter,
		SrcRoot:              srcRoot,
		DstRoot:              dstRoot,
		SrcServiceName:       cfg.Source.Name,
		WorkerCount:          cfg.WorkerCount,
		MaxRetries:           cfg.MaxRetries,
		CoordinatorLead:      cfg.CoordinatorLead,
		LogAddress:           cfg.LogAddress,
		LogLevel:             cfg.LogLevel,
		SkipListener:         cfg.SkipListener,
		StartupDelay:         cfg.StartupDelay,
		ProgressTick:         cfg.ProgressTick,
		ShutdownContext:      runCtx,
		ResumeTraversal:      resume,
		RootPreparation:      cfg.RootPreparation,
		SoftSuspendRequested: func() bool { return m.softSuspendRequested.Load() },
		ReportStopProgress:   m.reportStopProgress,
		OnQueueObserver:      func(o *observe.QueueObserver) { m.activeQueueObs.Store(o) },
		OnAutoscaler:         m.bindAutoscaler,
		Autoscaler:           m.resolveAutoscalerConfig(cfg.Autoscaler),
		SrcService:           cfg.Source,
		DstService:           cfg.Destination,
		PathCheckTarget:      cfg.PathCheckTarget,
		WindowsCompat:        cfg.WindowsCompat,
		FilterRuleset:        firstFilter(cfg.FilterRuleset, m.FilterRuleset()),
	})
	if err != nil {
		if errors.Is(err, ErrTraversalSoftSuspended) {
			rstats, sus, ok := AsTraversalSuspended(err)
			if ok {
				if patch, e2 := suspendRuntimeMergePatch(sus); e2 == nil {
					_ = m.store.updateRuntimeState(m.ID, patch)
				}
				if e3 := m.transitionTo(PhaseTraversalSuspended); e3 != nil {
					if m.Phase() == PhaseAborted {
						m.softSuspendRequested.Store(false)
						m.refreshRuntimeState()
						return rstats, nil
					}
					return rstats, e3
				}
				m.softSuspendRequested.Store(false)
				m.refreshRuntimeState()
				return rstats, nil
			}
		}
		if m.Phase() == PhaseAborted {
			return RuntimeStats{}, nil
		}
		return RuntimeStats{}, err
	}
	if err := m.completeDurableFinalize(PhaseTraversalFinalizing, PhaseTraversalFinalizeFailed, PhaseTraversalReview, FinalizeOverrides{}); err != nil {
		m.refreshRuntimeState()
		return stats, err
	}
	return stats, nil
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
	if err := m.transitionTo(PhaseCopying); err != nil {
		return queue.QueueStats{}, err
	}

	var resumeCopy *RuntimeSuspendV1
	if prevPhase == PhaseCopySuspended {
		if s, ok := parseRuntimeSuspendV1(m.runtimeStateJSON()); ok && s.Kind == "copy" {
			resumeCopy = &s
		}
	}
	if cfg.Source.Adapter == nil || cfg.Destination.Adapter == nil {
		return queue.QueueStats{}, fmt.Errorf("copy requires source and destination adapters in config")
	}
	if err := m.UpdateConfig(cfg); err != nil {
		return queue.QueueStats{}, fmt.Errorf("persist migration config: %w", err)
	}
	m.setLastRunConfig(cfg)

	runCtx, cancelRun := m.beginRun(cfg.ShutdownContext)
	defer func() {
		cancelRun()
		m.endRun()
		err := m.store.updateUpdatedAt(m.ID)
		if err != nil {
			fmt.Println("error updating updated at", err)
		}
	}()
	stats, err := RunCopyPhase(CopyPhaseConfig{
		DuckDB:               m.DB,
		SrcAdapter:           cfg.Source.Adapter,
		DstAdapter:           cfg.Destination.Adapter,
		WorkerCount:          cfg.WorkerCount,
		MaxRetries:           cfg.MaxRetries,
		LogAddress:           cfg.LogAddress,
		LogLevel:             cfg.LogLevel,
		SkipListener:         cfg.SkipListener,
		StartupDelay:         cfg.StartupDelay,
		ProgressTick:         cfg.ProgressTick,
		ShutdownContext:      runCtx,
		ResumeCopy:           resumeCopy,
		SoftSuspendRequested: func() bool { return m.softSuspendRequested.Load() },
		ReportStopProgress:   m.reportStopProgress,
		OnQueueObserver:      func(o *observe.QueueObserver) { m.activeQueueObs.Store(o) },
		OnAutoscaler:         m.bindAutoscaler,
		Autoscaler:           m.resolveAutoscalerConfig(cfg.Autoscaler),
		SrcService:           cfg.Source,
		DstService:           cfg.Destination,
		PathCheckTarget:      cfg.PathCheckTarget,
		WindowsCompat:        cfg.WindowsCompat,
	})
	if err != nil {
		if errors.Is(err, ErrCopySoftSuspended) {
			cstats, sus, ok := AsCopySuspended(err)
			if ok {
				if patch, e2 := suspendRuntimeMergePatch(sus); e2 == nil {
					_ = m.store.updateRuntimeState(m.ID, patch)
				}
				if e3 := m.transitionTo(PhaseCopySuspended); e3 != nil {
					if m.Phase() == PhaseAborted {
						m.softSuspendRequested.Store(false)
						m.refreshRuntimeState()
						return cstats, nil
					}
					return cstats, e3
				}
				m.softSuspendRequested.Store(false)
				m.refreshRuntimeState()
				return cstats, nil
			}
		}
		if m.Phase() == PhaseAborted {
			return queue.QueueStats{}, nil
		}
		return queue.QueueStats{}, err
	}
	if err := m.completeDurableFinalize(PhaseCopyFinalizing, PhaseCopyFinalizeFailed, PhaseCopyReview, FinalizeOverrides{}); err != nil {
		m.refreshRuntimeState()
		return stats, err
	}
	return stats, nil
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
	counts, err := stats.GetEligibleDeleteStatusCounts(m.DB)
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
	if err := m.transitionTo(PhaseDeleting); err != nil {
		return queue.QueueStats{}, err
	}
	if err := m.DB.NormalizeDeleteForestOps(); err != nil {
		return queue.QueueStats{}, fmt.Errorf("normalize delete forest before delete: %w", err)
	}
	var resumeDelete *RuntimeSuspendV1
	if prevPhase == PhaseDeleteSuspended {
		if s, ok := parseRuntimeSuspendV1(m.runtimeStateJSON()); ok && s.Kind == "delete" {
			resumeDelete = &s
		}
	}
	if cfg.Source.Adapter == nil {
		return queue.QueueStats{}, fmt.Errorf("delete requires source adapter in config")
	}
	if err := m.UpdateConfig(cfg); err != nil {
		return queue.QueueStats{}, fmt.Errorf("persist migration config: %w", err)
	}
	m.setLastRunConfig(cfg)

	runCtx, cancelRun := m.beginRun(cfg.ShutdownContext)
	defer func() {
		cancelRun()
		m.endRun()
		_ = m.store.updateUpdatedAt(m.ID)
	}()
	stats, err := RunDeletePhase(DeletePhaseConfig{
		DuckDB:               m.DB,
		SrcAdapter:           cfg.Source.Adapter,
		WorkerCount:          cfg.WorkerCount,
		MaxRetries:           cfg.MaxRetries,
		LogAddress:           cfg.LogAddress,
		LogLevel:             cfg.LogLevel,
		SkipListener:         cfg.SkipListener,
		StartupDelay:         cfg.StartupDelay,
		ProgressTick:         cfg.ProgressTick,
		ShutdownContext:      runCtx,
		ResumeDelete:         resumeDelete,
		SoftSuspendRequested: func() bool { return m.softSuspendRequested.Load() },
		ReportStopProgress:   m.reportStopProgress,
		OnQueueObserver:      func(o *observe.QueueObserver) { m.activeQueueObs.Store(o) },
		OnAutoscaler:         m.bindAutoscaler,
		Autoscaler:           m.resolveAutoscalerConfig(cfg.Autoscaler),
		SrcService:           cfg.Source,
		DstService:           cfg.Destination,
		PathCheckTarget:      cfg.PathCheckTarget,
		WindowsCompat:        cfg.WindowsCompat,
	})
	if err != nil {
		if errors.Is(err, ErrDeleteSoftSuspended) {
			dstats, sus, ok := AsDeleteSuspended(err)
			if ok {
				if patch, e2 := suspendRuntimeMergePatch(sus); e2 == nil {
					_ = m.store.updateRuntimeState(m.ID, patch)
				}
				if e3 := m.transitionTo(PhaseDeleteSuspended); e3 != nil {
					if m.Phase() == PhaseAborted {
						m.softSuspendRequested.Store(false)
						m.refreshRuntimeState()
						return dstats, nil
					}
					return dstats, e3
				}
				m.softSuspendRequested.Store(false)
				m.refreshRuntimeState()
				return dstats, nil
			}
		}
		if m.Phase() == PhaseAborted {
			return queue.QueueStats{}, nil
		}
		return queue.QueueStats{}, err
	}
	if err := m.completeDurableFinalize(PhaseDeleteFinalizing, PhaseDeleteFinalizeFailed, PhaseDeleteReview, FinalizeOverrides{}); err != nil {
		m.refreshRuntimeState()
		return stats, err
	}
	m.refreshRuntimeState()
	return stats, nil
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
func (m *Migration) RunDeleteRetry(cfg Config, opts CopyPhaseOptions) (queue.QueueStats, error) {
	phase := m.Phase()
	if phase != PhaseDeleteReview && phase != PhaseDeleting && phase != PhaseDeleteSuspended {
		return queue.QueueStats{}, fmt.Errorf("delete retry requires awaiting-delete-review, delete-suspended, or prepared delete-in-progress")
	}
	if cfg.Source.Adapter == nil {
		return queue.QueueStats{}, fmt.Errorf("delete retry requires source adapter in config")
	}
	if err := m.UpdateConfig(cfg); err != nil {
		return queue.QueueStats{}, fmt.Errorf("persist migration config: %w", err)
	}
	m.setLastRunConfig(cfg)
	if phase == PhaseDeleteReview || phase == PhaseDeleteSuspended {
		if err := m.DB.NormalizeDeleteForestOps(); err != nil {
			return queue.QueueStats{}, fmt.Errorf("normalize delete forest before delete retry: %w", err)
		}
		if err := m.transitionTo(PhaseDeleting); err != nil {
			return queue.QueueStats{}, err
		}
	}
	runCtx, cancelRun := m.beginRun(cfg.ShutdownContext)
	defer func() {
		cancelRun()
		m.endRun()
		_ = m.store.updateUpdatedAt(m.ID)
	}()
	workerCount := opts.WorkerCount
	if workerCount <= 0 {
		workerCount = cfg.WorkerCount
	}
	maxRetries := opts.MaxRetries
	if maxRetries <= 0 {
		maxRetries = cfg.MaxRetries
	}
	stats, err := RunDeleteRetryPhase(DeletePhaseConfig{
		DuckDB:               m.DB,
		SrcAdapter:           cfg.Source.Adapter,
		WorkerCount:          workerCount,
		MaxRetries:           maxRetries,
		LogAddress:           cfg.LogAddress,
		LogLevel:             cfg.LogLevel,
		SkipListener:         opts.SkipListener || cfg.SkipListener,
		StartupDelay:         cfg.StartupDelay,
		ShutdownContext:      runCtx,
		SoftSuspendRequested: func() bool { return m.softSuspendRequested.Load() },
		ReportStopProgress:   m.reportStopProgress,
		OnQueueObserver:      func(o *observe.QueueObserver) { m.activeQueueObs.Store(o) },
		OnAutoscaler:         m.bindAutoscaler,
		Autoscaler:           m.resolveAutoscalerConfig(cfg.Autoscaler),
		SrcService:           cfg.Source,
		DstService:           cfg.Destination,
		PathCheckTarget:      cfg.PathCheckTarget,
		WindowsCompat:        cfg.WindowsCompat,
	})
	if err != nil {
		if errors.Is(err, ErrDeleteSoftSuspended) {
			dstats, sus, ok := AsDeleteSuspended(err)
			if ok {
				if patch, e2 := suspendRuntimeMergePatch(sus); e2 == nil {
					_ = m.store.updateRuntimeState(m.ID, patch)
				}
				if e3 := m.transitionTo(PhaseDeleteSuspended); e3 != nil {
					if m.Phase() == PhaseAborted {
						m.softSuspendRequested.Store(false)
						m.refreshRuntimeState()
						return dstats, nil
					}
					return dstats, e3
				}
				m.softSuspendRequested.Store(false)
				m.refreshRuntimeState()
				return dstats, nil
			}
		}
		if m.Phase() == PhaseAborted {
			return queue.QueueStats{}, nil
		}
		return queue.QueueStats{}, err
	}
	if err := m.completeDurableFinalize(PhaseDeleteFinalizing, PhaseDeleteFinalizeFailed, PhaseDeleteReview, FinalizeOverrides{}); err != nil {
		m.refreshRuntimeState()
		return stats, err
	}
	m.refreshRuntimeState()
	return stats, nil
}

func (m *Migration) RunRetrySweep(cfg Config, opts RetrySweepOptions) (RuntimeStats, error) {
	phase := m.Phase()
	if phase != PhaseTraversalReview && phase != PhaseTraversing && phase != PhaseTraversalSuspended {
		return RuntimeStats{}, fmt.Errorf("retry sweep requires awaiting-traversal-review, traversal-suspended, or prepared traversal-in-progress")
	}
	if cfg.Source.Adapter == nil || cfg.Destination.Adapter == nil {
		return RuntimeStats{}, fmt.Errorf("retry sweep requires source and destination adapters in config")
	}
	if err := m.UpdateConfig(cfg); err != nil {
		return RuntimeStats{}, fmt.Errorf("persist migration config: %w", err)
	}
	m.setLastRunConfig(cfg)
	if phase == PhaseTraversalReview || phase == PhaseTraversalSuspended {
		if err := m.transitionTo(PhaseTraversing); err != nil {
			return RuntimeStats{}, err
		}
	}
	runCtx, cancelRun := m.beginRun(cfg.ShutdownContext)
	defer func() {
		cancelRun()
		m.endRun()
		err := m.store.updateUpdatedAt(m.ID)
		if err != nil {
			fmt.Println("error updating updated at", err)
		}
	}()

	workerCount := opts.WorkerCount
	if workerCount <= 0 {
		workerCount = cfg.WorkerCount
	}
	maxRetries := opts.MaxRetries
	if maxRetries <= 0 {
		maxRetries = cfg.MaxRetries
	}
	logAddress := opts.LogAddress
	if logAddress == "" {
		logAddress = cfg.LogAddress
	}
	logLevel := opts.LogLevel
	if logLevel == "" {
		logLevel = cfg.LogLevel
	}

	stats, err := RunRetrySweep(SweepConfig{
		DuckDB:       m.DB,
		SrcAdapter:   cfg.Source.Adapter,
		DstAdapter:   cfg.Destination.Adapter,
		WorkerCount:  workerCount,
		MaxRetries:   maxRetries,
		LogAddress:   logAddress,
		LogLevel:     logLevel,
		SkipListener: opts.SkipListener || cfg.SkipListener,
		ProgressTick: cfg.ProgressTick,
		StartupDelay: cfg.StartupDelay,
		MaxKnownDepth: func() int {
			if opts.MaxKnownDepth != 0 {
				return opts.MaxKnownDepth
			}
			return -1
		}(),
		ShutdownContext:      runCtx,
		SoftSuspendRequested: func() bool { return m.softSuspendRequested.Load() },
		ReportStopProgress:   m.reportStopProgress,
		OnQueueObserver:      func(o *observe.QueueObserver) { m.activeQueueObs.Store(o) },
		OnAutoscaler:         m.bindAutoscaler,
		Autoscaler:           m.resolveAutoscalerConfig(cfg.Autoscaler),
		SrcService:           cfg.Source,
		DstService:           cfg.Destination,
		PathCheckTarget:      cfg.PathCheckTarget,
		WindowsCompat:        cfg.WindowsCompat,
		FilterRuleset:        firstFilter(cfg.FilterRuleset, m.FilterRuleset()),
	})
	if err != nil {
		if errors.Is(err, ErrTraversalSoftSuspended) {
			rstats, sus, ok := AsTraversalSuspended(err)
			if ok {
				if patch, e2 := suspendRuntimeMergePatch(sus); e2 == nil {
					_ = m.store.updateRuntimeState(m.ID, patch)
				}
				if e3 := m.transitionTo(PhaseTraversalSuspended); e3 != nil {
					if m.Phase() == PhaseAborted {
						m.softSuspendRequested.Store(false)
						m.refreshRuntimeState()
						return rstats, nil
					}
					return rstats, e3
				}
				m.softSuspendRequested.Store(false)
				m.refreshRuntimeState()
				return rstats, nil
			}
		}
		return RuntimeStats{}, err
	}
	if err := m.completeDurableFinalize(PhaseTraversalFinalizing, PhaseTraversalFinalizeFailed, PhaseTraversalReview, FinalizeOverrides{}); err != nil {
		m.refreshRuntimeState()
		return stats, err
	}
	return stats, nil
}

// RunCopyRetry runs the copy phase in retry mode (marked failed→pending items). On success transitions back to awaiting-copy-review.
func (m *Migration) RunCopyRetry(cfg Config, opts CopyPhaseOptions) (queue.QueueStats, error) {
	phase := m.Phase()
	if phase != PhaseCopyReview && phase != PhaseCopying && phase != PhaseCopySuspended {
		return queue.QueueStats{}, fmt.Errorf("copy retry requires awaiting-copy-review, copy-suspended, or prepared copy-in-progress")
	}
	if cfg.Source.Adapter == nil || cfg.Destination.Adapter == nil {
		return queue.QueueStats{}, fmt.Errorf("copy retry requires source and destination adapters in config")
	}
	if err := m.UpdateConfig(cfg); err != nil {
		return queue.QueueStats{}, fmt.Errorf("persist migration config: %w", err)
	}
	m.setLastRunConfig(cfg)
	if phase == PhaseCopyReview || phase == PhaseCopySuspended {
		if err := m.transitionTo(PhaseCopying); err != nil {
			return queue.QueueStats{}, err
		}
	}
	runCtx, cancelRun := m.beginRun(cfg.ShutdownContext)
	defer func() {
		cancelRun()
		m.endRun()
		if err := m.store.updateUpdatedAt(m.ID); err != nil {
			fmt.Println("error updating updated at", err)
		}
	}()
	workerCount := opts.WorkerCount
	if workerCount <= 0 {
		workerCount = cfg.WorkerCount
	}
	maxRetries := opts.MaxRetries
	if maxRetries <= 0 {
		maxRetries = cfg.MaxRetries
	}
	logAddress := opts.LogAddress
	if logAddress == "" {
		logAddress = cfg.LogAddress
	}
	logLevel := opts.LogLevel
	if logLevel == "" {
		logLevel = cfg.LogLevel
	}
	stats, err := RunCopyRetryPhase(CopyPhaseConfig{
		DuckDB:               m.DB,
		SrcAdapter:           cfg.Source.Adapter,
		DstAdapter:           cfg.Destination.Adapter,
		WorkerCount:          workerCount,
		MaxRetries:           maxRetries,
		LogAddress:           logAddress,
		LogLevel:             logLevel,
		SkipListener:         opts.SkipListener || cfg.SkipListener,
		StartupDelay:         cfg.StartupDelay,
		ProgressTick:         cfg.ProgressTick,
		ShutdownContext:      runCtx,
		SoftSuspendRequested: func() bool { return m.softSuspendRequested.Load() },
		ReportStopProgress:   m.reportStopProgress,
		OnQueueObserver:      func(o *observe.QueueObserver) { m.activeQueueObs.Store(o) },
		OnAutoscaler:         m.bindAutoscaler,
		Autoscaler:           m.resolveAutoscalerConfig(cfg.Autoscaler),
		SrcService:           cfg.Source,
		DstService:           cfg.Destination,
		PathCheckTarget:      cfg.PathCheckTarget,
		WindowsCompat:        cfg.WindowsCompat,
	})
	if err != nil {
		if errors.Is(err, ErrCopySoftSuspended) {
			cstats, sus, ok := AsCopySuspended(err)
			if ok {
				if patch, e2 := suspendRuntimeMergePatch(sus); e2 == nil {
					_ = m.store.updateRuntimeState(m.ID, patch)
				}
				if e3 := m.transitionTo(PhaseCopySuspended); e3 != nil {
					return cstats, e3
				}
				m.softSuspendRequested.Store(false)
				m.refreshRuntimeState()
				return cstats, nil
			}
		}
		return queue.QueueStats{}, err
	}
	if err := m.completeDurableFinalize(PhaseCopyFinalizing, PhaseCopyFinalizeFailed, PhaseCopyReview, FinalizeOverrides{}); err != nil {
		m.refreshRuntimeState()
		return stats, err
	}
	return stats, nil
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
	default:
		if cancel != nil {
			cancel()
		}
		return result, nil
	}
}

// ForceStop cancels the active run context and abandons in-flight queue work.
// Prefer soft Stop; use Abort/ForceStop only when the user chooses hard kill.
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

// Abort force-stops the live run and transitions to PhaseAborted (not resumable).
func (m *Migration) Abort() (StopResult, error) {
	result, err := m.ForceStop()
	if err != nil {
		return result, err
	}
	if !result.Stopped && !m.IsLive() {
		// Still try to mark aborted if we are mid-phase.
	}
	phase := m.Phase()
	switch phase {
	case PhaseTraversing, PhaseCopying, PhaseDeleting:
		if e := m.transitionTo(PhaseAborted); e != nil {
			return result, e
		}
		result.Phase = PhaseAborted
	}
	m.setStopStepDetail(StopStepAborted, 0, "")
	m.refreshRuntimeState()
	result.RuntimeStatus = m.GetRuntimeStatus()
	return result, nil
}
