// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/profile"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// MigrationConfig is the configuration passed to RunMigration.
type MigrationConfig struct {
	PhaseRuntime
	DB             *db.DB // DB instance (required; manager-owned)
	SrcAdapter     types.FSAdapter
	DstAdapter     types.FSAdapter
	SrcRoot        types.Folder
	DstRoot        types.Folder
	SrcServiceName string
	ResumeStatus   *MigrationStatus
	// RootPreparation selects start rounds after UI-injected depth-1 children at AddRoots.
	RootPreparation RootPreparation
	// ResumeTraversal: non-nil after traversal-suspended; continue the same traversal from saved round/cursor.
	ResumeTraversal *RuntimeSuspendV1
	// UncleanResume forces a recovery-round cursor reconstruct (ignore soft-stop cursors).
	UncleanResume  bool
	OnRecoveryDone func()
}

// RuntimeStats captures execution statistics at the end of a migration run.
type RuntimeStats struct {
	Duration time.Duration
	Src      queue.QueueStats
	Dst      queue.QueueStats
}

// RunMigration executes the migration traversal using the provided configuration.
func RunMigration(cfg MigrationConfig) (RuntimeStats, error) {
	database := cfg.DB
	if err := cfg.Begin(database, cfg.SrcAdapter, cfg.DstAdapter); err != nil {
		return RuntimeStats{}, err
	}

	coordinator := queue.NewQueueCoordinator()

	srcCtx := newScalingContext("src", queue.ScalingModeTraversal, queue.QueueModeTraversal, 0, cfg.SrcService, cfg.DstService)
	dstCtx := newScalingContext("dst", queue.ScalingModeTraversal, queue.QueueModeTraversal, 0, cfg.SrcService, cfg.DstService)
	srcSizing := queueSizingForScalingContext(srcCtx, cfg.ResumeTraversal)
	dstSizing := queueSizingForScalingContext(dstCtx, cfg.ResumeTraversal)
	srcWC := resolveWorkersForScalingContext(srcCtx, cfg.WorkerCount, cfg.ResumeTraversal)
	dstWC := resolveWorkersForScalingContext(dstCtx, cfg.WorkerCount, cfg.ResumeTraversal)
	suspendRetries := 0
	if cfg.ResumeTraversal != nil {
		suspendRetries = cfg.ResumeTraversal.MaxRetries
	}
	mr := effectiveInt(suspendRetries, cfg.MaxRetries)

	// DB-backed frontier: no LevelCache for traversal. Queues pull from DB in batches.
	srcQueue := queue.NewQueue("src", mr, srcWC, coordinator, srcSizing)
	dstQueue := queue.NewQueue("dst", mr, dstWC, coordinator, dstSizing)
	applyFSStallTimeouts(srcQueue, cfg.PhaseRuntime)
	applyFSStallTimeouts(dstQueue, cfg.PhaseRuntime)

	if cfg.ResumeTraversal != nil {
		maxKD := cfg.ResumeTraversal.MaxKnownDepth
		if maxKD <= 0 {
			if d, err := stats.GetMaxDepth(database, "SRC"); err == nil {
				maxKD = d
			}
		}
		srcQueue.SetMaxKnownDepth(maxKD)
		dstQueue.SetMaxKnownDepth(maxKD)
		seedQueueCountersFromDB(database, srcQueue, "src-traversal", db.QueueStatsPhaseTraversal)
		seedQueueCountersFromDB(database, dstQueue, "dst-traversal", db.QueueStatsPhaseTraversal)
	}

	srcQueue.SetDatabase(database)
	dstQueue.SetDatabase(database)
	if cfg.ResumeTraversal != nil {
		applyTraversalResume(database, cfg.ResumeTraversal, srcQueue, dstQueue, coordinator, cfg.UncleanResume)
		logTraversalResumePositionCheck(cfg.ResumeTraversal, srcQueue, dstQueue)
		if cfg.UncleanResume && cfg.OnRecoveryDone != nil {
			// Clear after queues are positioned for recovery; remaining work is normal BFS.
			cfg.OnRecoveryDone()
		}
	} else if err := initializeQueues(cfg, srcQueue, dstQueue, coordinator); err != nil {
		return RuntimeStats{}, err
	}

	srcQueue.Initialize(queue.PoolTraversal, database, cfg.SrcAdapter, nil, cfg.ShutdownContext)
	// Peer SRC adapter on DST enables optional hash-verify OpenRead during compare.
	dstQueue.Initialize(queue.PoolTraversal, database, cfg.DstAdapter, cfg.SrcAdapter, cfg.ShutdownContext)
	srcQueue.SetRateLimitTelemetry(rateLimitBridgeForAdapter(cfg.SrcAdapter))
	dstQueue.SetRateLimitTelemetry(rateLimitBridgeForAdapter(cfg.DstAdapter))

	srcListProfile := profile.ResolveEffectiveProfile(srcCtx, cfg.SrcAdapter, cfg.DstAdapter)
	dstListProfile := profile.ResolveEffectiveProfile(dstCtx, cfg.SrcAdapter, cfg.DstAdapter)
	profile.ApplyQueueListPagination(srcQueue, srcListProfile)
	profile.ApplyQueueListPagination(dstQueue, dstListProfile)

	if cfg.ResumeTraversal != nil {
		time.Sleep(500 * time.Millisecond)
		srcQueue.PullTasksIfNeeded(true)
		dstQueue.PullTasksIfNeeded(true)
	} else {
		time.Sleep(100 * time.Millisecond)
	}

	// Start traversal phase: drop indexes, persistent appenders. CHECKPOINT runs once after phase teardown (defer order below), not per-queue complete.
	if err := database.BeginTraversalPhase(cfg.ShutdownOrBackground()); err != nil {
		return RuntimeStats{}, fmt.Errorf("begin traversal phase: %w", err)
	}
	bulkPhaseClosed := false
	skipDurableTeardown := false
	defer func() {
		if bulkPhaseClosed || skipDurableTeardown {
			return
		}
		if err := database.CheckpointWithRetry(context.Background(), 8); err != nil {
			fmt.Println("checkpoint after traversal phase:", err)
		}
	}()
	defer func() {
		if bulkPhaseClosed || skipDurableTeardown {
			return
		}
		if err := database.EndTraversalPhase(); err != nil {
			fmt.Println("error ending traversal phase", err)
		}
	}()

	obsPoll := observerPollFromConfigAndSuspend(cfg.ObserverPollInterval, cfg.ResumeTraversal)
	observer := observe.NewQueueObserver(database, obsPoll)
	observer.Start()      // Start observer loop immediately
	defer observer.Stop() // Ensure observer is stopped on exit
	if cfg.OnQueueObserver != nil {
		defer cfg.OnQueueObserver(nil)
	}

	// Register queues with observer (observer will poll queues directly)
	srcQueue.SetObserver(observer)
	dstQueue.SetObserver(observer)
	if cfg.OnQueueObserver != nil {
		cfg.OnQueueObserver(observer)
	}

	asCtx := cfg.startAutoscaler(observer, database, srcQueue, dstQueue)
	if cfg.OnAutoscaler != nil {
		cfg.OnAutoscaler(asCtx.Autoscaler())
		defer cfg.OnAutoscaler(nil)
	}
	defer asCtx.stop()

	// Set up stats channels for UDP logging (after queues are running)
	srcStatsChan := make(chan queue.QueueStats, 10)
	dstStatsChan := make(chan queue.QueueStats, 10)
	srcQueue.SetStatsChannel(srcStatsChan)
	dstQueue.SetStatsChannel(dstStatsChan)

	printTraversalProgressFromQueues(srcQueue, dstQueue)

	statsCtx, statsCancel := context.WithCancel(context.Background())
	defer statsCancel()

	// Start stats consumer goroutine for progress updates (fmt output, not log service)
	go func() {
		var lastSrcStats *queue.QueueStats
		var lastDstStats *queue.QueueStats

		for {
			select {
			case <-statsCtx.Done():
				return
			case srcStats := <-srcStatsChan:
				lastSrcStats = &srcStats
				printTraversalProgress(srcQueue, dstQueue, lastSrcStats, lastDstStats)
			case dstStats := <-dstStatsChan:
				lastDstStats = &dstStats
				printTraversalProgress(srcQueue, dstQueue, lastSrcStats, lastDstStats)
			}
		}
	}()

	progressTick := progressTickFromConfigAndSuspend(cfg.ProgressTick, cfg.ResumeTraversal, time.Second)
	if progressTick <= 0 {
		progressTick = 1 * time.Second
	}
	progressTicker := time.NewTicker(progressTick)
	defer progressTicker.Stop()
	start := time.Now()

	for {
		// Hard shutdown: context canceled (fatal / external cancel).
		if cfg.ShutdownContext != nil {
			select {
			case <-cfg.ShutdownContext.Done():
				skipDurableTeardown = true
				performTraversalForceStop(srcQueue, dstQueue, observer)
				database.AbortTraversalPhase()
				srcStats, dstStats := snapshotTraversalQueueStats(database, coordinator)
				return RuntimeStats{
					Duration: time.Since(start),
					Src:      srcStats,
					Dst:      dstStats,
				}, fmt.Errorf("migration force stopped during %s", "traversal")
			default:
			}
		}

		if cfg.SoftSuspendRequested != nil && cfg.SoftSuspendRequested() {
			waitCtx, cancel := softSuspendWaitContext(cfg.ShutdownContext)
			stats, suspend, err := performTraversalSoftSuspend(waitCtx, database, srcQueue, dstQueue, observer, coordinator, &cfg.PhaseRuntime, start, srcWC, mr)
			cancel()
			if forceStopOverridesSoftSuspend(cfg.ShutdownContext, database) {
				skipDurableTeardown = true
				performTraversalForceStop(srcQueue, dstQueue, observer)
				database.AbortTraversalPhase()
				return RuntimeStats{
					Duration: time.Since(start),
					Src:      srcQueue.Stats(),
					Dst:      dstQueue.Stats(),
				}, fmt.Errorf("migration force stopped during %s", "traversal")
			}
			if err != nil {
				return stats, fmt.Errorf("traversal soft suspend: %w", err)
			}
			setDetail := waitDetailSetter(observer)
			bulkPhaseClosed = true
			finishCtx := cfg.ShutdownContext
			if finishCtx == nil {
				finishCtx = context.Background()
			}
			if err := finishSoftStopBulkPhase(finishCtx, database, cfg.ReportStopProgress, setDetail); err != nil {
				if forceStopOverridesSoftSuspend(cfg.ShutdownContext, database) {
					skipDurableTeardown = true
					performTraversalForceStop(srcQueue, dstQueue, observer)
					database.AbortTraversalPhase()
					return RuntimeStats{
						Duration: time.Since(start),
						Src:      srcQueue.Stats(),
						Dst:      dstQueue.Stats(),
					}, fmt.Errorf("migration force stopped during %s", "traversal")
				}
				return stats, fmt.Errorf("traversal soft suspend teardown: %w", err)
			}
			fmt.Print("\n")
			return stats, newSuspendedError(ErrTraversalSoftSuspended, stats, suspend)
		}

		// Check exhaustion status from coordinator (source of truth)
		// Queues call MarkSrcCompleted()/MarkDstCompleted() on coordinator when done
		bothCompleted := coordinator.IsCompleted("both")

		if bothCompleted {
			observer.Stop()
			time.Sleep(250 * time.Millisecond) // let observer loop exit before we close the logger
			// Durable teardown (indexes + checkpoint) runs in domain_phases after *-finalizing.
			bulkPhaseClosed = true
			return completeTraversalRun(database, coordinator, progressTicker, start), nil
		}

		select {
		case <-progressTicker.C:
			// Live progress each tick (survives scaling-event newlines and \r-only lines in some terminals).
			printTraversalProgressFromQueues(srcQueue, dstQueue)
			// Re-check exhaustion from coordinator (queues might have completed during tick)
			if coordinator.IsCompleted("both") {
				observer.Stop()
				time.Sleep(250 * time.Millisecond)
				bulkPhaseClosed = true
				return completeTraversalRun(database, coordinator, progressTicker, start), nil
			}

		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func printTraversalProgress(srcQueue, dstQueue *queue.Queue, lastSrcStats, lastDstStats *queue.QueueStats) {
	if lastSrcStats == nil || lastDstStats == nil {
		printTraversalProgressFromQueues(srcQueue, dstQueue)
		return
	}
	printTraversalProgressLine(srcQueue, dstQueue, *lastSrcStats, *lastDstStats)
}

func printTraversalProgressFromQueues(srcQueue, dstQueue *queue.Queue) {
	if srcQueue == nil || dstQueue == nil {
		return
	}
	printTraversalProgressLine(srcQueue, dstQueue, srcQueue.Stats(), dstQueue.Stats())
}

func printTraversalProgressLine(srcQueue, dstQueue *queue.Queue, srcLive, dstLive queue.QueueStats) {
	srcRoundStats := srcQueue.GetRoundStats(srcLive.Round)
	dstRoundStats := dstQueue.GetRoundStats(dstLive.Round)
	srcExpected, srcCompleted := 0, 0
	if srcRoundStats != nil {
		srcExpected = srcRoundStats.Expected
		srcCompleted = srcRoundStats.Completed
	}
	dstExpected, dstCompleted := 0, 0
	if dstRoundStats != nil {
		dstExpected = dstRoundStats.Expected
		dstCompleted = dstRoundStats.Completed
	}
	fmt.Printf("\r  Src: Round %d (Exp:%d Comp:%d Pend:%d W:%d) | Dst: Round %d (Exp:%d Comp:%d Pend:%d W:%d)   ",
		srcLive.Round, srcExpected, srcCompleted, srcLive.Pending, srcLive.Workers,
		dstLive.Round, dstExpected, dstCompleted, dstLive.Pending, dstLive.Workers)
}

func snapshotTraversalQueueStats(database *db.DB, coordinator *queue.QueueCoordinator) (queue.QueueStats, queue.QueueStats) {
	srcRound := coordinator.GetRound("src")
	dstRound := coordinator.GetRound("dst")
	srcPending := 0
	dstPending := 0
	c, err := stats.GetStatsCountAtDepth(database, "SRC", srcRound, db.StatsKey(db.StatsKindTraversal, db.StatusPending))
	if err != nil {
		fmt.Println("error getting stats count at depth", err)
		return queue.QueueStats{}, queue.QueueStats{}
	}
	srcPending = int(c)
	c, err = stats.GetStatsCountAtDepth(database, "DST", dstRound, db.StatsKey(db.StatsKindTraversal, db.StatusPending))
	if err != nil {
		fmt.Println("error getting stats count at depth", err)
		return queue.QueueStats{}, queue.QueueStats{}
	}
	dstPending = int(c)
	srcStats := queue.QueueStats{Name: "src", Round: srcRound, Pending: srcPending}
	dstStats := queue.QueueStats{Name: "dst", Round: dstRound, Pending: dstPending}
	return srcStats, dstStats
}

func completeTraversalRun(database *db.DB, coordinator *queue.QueueCoordinator, progressTicker *time.Ticker, start time.Time) RuntimeStats {
	if database != nil {
		_ = database.Flush(context.Background())
		_ = queue.FinalizeCopyWorkOnStop(database, coordinator)
	}
	srcStats, dstStats := snapshotTraversalQueueStats(database, coordinator)
	fmt.Println("\nTraversal complete!")
	progressTicker.Stop()
	closeGlobalLoggerWithTimeout(1 * time.Second)
	return RuntimeStats{
		Duration: time.Since(start),
		Src:      srcStats,
		Dst:      dstStats,
	}
}

func closeGlobalLoggerWithTimeout(timeout time.Duration) {
	if logservice.LS == nil {
		return
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), timeout)
	defer closeCancel()
	closeDone := make(chan struct{}, 1)
	go func() {
		err := logservice.LS.Close()
		if err != nil {
			fmt.Println("error closing global logger", err)
		}
		closeDone <- struct{}{}
	}()
	select {
	case <-closeDone:
	case <-closeCtx.Done():
	}
}

// initializeQueues sets up src/dst queues from runtime DB resume state.
func initializeQueues(cfg MigrationConfig, srcQueue *queue.Queue, dstQueue *queue.Queue, coordinator *queue.QueueCoordinator) error {
	srcRound := cfg.RootPreparation.SourceStartRound()
	dstRound := cfg.RootPreparation.DestStartRound()
	if cfg.ResumeStatus != nil {
		if cfg.ResumeStatus.MinPendingDepthSrc != nil {
			srcRound = *cfg.ResumeStatus.MinPendingDepthSrc
		}
		if cfg.ResumeStatus.MinPendingDepthDst != nil {
			dstRound = *cfg.ResumeStatus.MinPendingDepthDst
		}
	}

	// Set queue rounds
	srcQueue.SetRound(srcRound)
	dstQueue.SetRound(dstRound)

	// Set Expected from stats bucket (O(1) per queue; round 0 = 1 for traversal)
	srcQueue.SetExpectedFromStatsBucket(srcQueue.GetRound())
	dstQueue.SetExpectedFromStatsBucket(dstQueue.GetRound())

	if coordinator != nil {
		coordinator.UpdateRound("src", srcRound)
		if dstRound >= 0 {
			coordinator.UpdateRound("dst", dstRound)
		} else {
			coordinator.MarkCompleted("dst")
		}
	}

	if logservice.LS != nil {
		err := logservice.LS.Log("info",
			fmt.Sprintf("Initialized queues: src round %d, dst round %d", srcRound, dstRound),
			"migration",
			"init",
		)
		if err != nil {
			fmt.Println("error logging", err)
		}
	}

	return nil
}
