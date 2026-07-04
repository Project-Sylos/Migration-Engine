// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// MigrationConfig is the configuration passed to RunMigration.
type MigrationConfig struct {
	DB              *db.DB // DuckDB instance (required; manager-owned)
	DBPath          string // Reserved for compatibility; ignored when DB is set
	SrcAdapter      types.FSAdapter
	DstAdapter      types.FSAdapter
	SrcRoot         types.Folder
	DstRoot         types.Folder
	SrcServiceName  string
	WorkerCount     int
	MaxRetries      int
	CoordinatorLead int
	LogAddress      string
	LogLevel        string
	SkipListener    bool
	StartupDelay    time.Duration
	ProgressTick    time.Duration
	ResumeStatus    *MigrationStatus
	ShutdownContext context.Context
	// ResumeTraversal: non-nil after traversal-suspended; retry-style frontier rebuild (round 0, persisted max depth / sizing).
	ResumeTraversal *RuntimeSuspendV1
	// SoftSuspendRequested is polled in the run loop; when true, queues drain and state is flushed (see ErrTraversalSoftSuspended).
	SoftSuspendRequested func() bool
	ObserverPollInterval time.Duration
	// OnQueueObserver is called with the live observer after queues register, and with nil when the run exits (before observer.Stop).
	OnQueueObserver func(*queue.QueueObserver)

	Autoscaler AutoscalerConfig

	SrcService Service
	DstService Service
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
	if database == nil {
		return RuntimeStats{}, fmt.Errorf("migration DB is required")
	}

	if cfg.SrcAdapter == nil || cfg.DstAdapter == nil {
		return RuntimeStats{}, fmt.Errorf("source and destination adapters must be provided")
	}

	// Initialize log service (always initialize, even if listener is skipped)
	// When SkipListener: bind port and discard UDP packets so sender writes don't fail; no terminal, no display.
	// When !SkipListener: spawn listener in a new terminal that displays logs.
	if cfg.LogAddress != "" {
		startupDelay := cfg.StartupDelay
		if startupDelay <= 0 {
			startupDelay = 500 * time.Millisecond
		}
		if cfg.SkipListener {
			logservice.StartListenerDiscard(cfg.LogAddress)
			time.Sleep(startupDelay)
		} else {
			if err := logservice.StartListener(cfg.LogAddress); err != nil {
				// Non-fatal: continue without listener
			} else {
				time.Sleep(startupDelay)
			}
		}
		if err := logservice.InitGlobalLogger(database, cfg.LogAddress, cfg.LogLevel); err != nil {
			return RuntimeStats{}, fmt.Errorf("failed to initialize logger: %w", err)
		}
	}

	coordinator := queue.NewQueueCoordinator()

	srcCtx := scalingContextForTraversal("src", cfg.SrcService, cfg.DstService, queue.QueueModeTraversal)
	dstCtx := scalingContextForTraversal("dst", cfg.SrcService, cfg.DstService, queue.QueueModeTraversal)
	if cfg.ResumeTraversal != nil {
		srcCtx.Mode = queue.ScalingModeRetry
		dstCtx.Mode = queue.ScalingModeRetry
	}
	srcSizing := queueSizingForScalingContext(srcCtx, cfg.ResumeTraversal)
	dstSizing := queueSizingForScalingContext(dstCtx, cfg.ResumeTraversal)
	srcWC := resolveWorkersForScalingContext(srcCtx, cfg.WorkerCount, cfg.ResumeTraversal)
	dstWC := resolveWorkersForScalingContext(dstCtx, cfg.WorkerCount, cfg.ResumeTraversal)
	mr := effectiveMaxRetries(cfg.MaxRetries, cfg.ResumeTraversal)

	// DB-backed frontier: no LevelCache for traversal. Queues pull from DuckDB in batches.
	srcQueue := queue.NewQueue("src", mr, srcWC, coordinator, srcSizing)
	dstQueue := queue.NewQueue("dst", mr, dstWC, coordinator, dstSizing)

	if cfg.ResumeTraversal != nil {
		srcQueue.SetMode(queue.QueueModeRetry)
		dstQueue.SetMode(queue.QueueModeRetry)
		maxKD := cfg.ResumeTraversal.MaxKnownDepth
		if maxKD <= 0 {
			if d, err := database.GetMaxDepth("SRC"); err == nil {
				maxKD = d
			}
		}
		srcQueue.SetMaxKnownDepth(maxKD)
		dstQueue.SetMaxKnownDepth(maxKD)
	}

	srcQueue.InitializeWithContext(database, cfg.SrcAdapter, cfg.ShutdownContext)
	dstQueue.InitializeWithContext(database, cfg.DstAdapter, cfg.ShutdownContext)
	srcQueue.SetRateLimitTelemetry(rateLimitBridgeForAdapter(cfg.SrcAdapter))
	dstQueue.SetRateLimitTelemetry(rateLimitBridgeForAdapter(cfg.DstAdapter))

	srcListProfile := scaling.ResolveEffectiveProfile(
		scalingContextForTraversal("src", cfg.SrcService, cfg.DstService, queue.QueueModeTraversal),
		cfg.SrcAdapter, cfg.DstAdapter,
	)
	dstListProfile := scaling.ResolveEffectiveProfile(
		scalingContextForTraversal("dst", cfg.SrcService, cfg.DstService, queue.QueueModeTraversal),
		cfg.SrcAdapter, cfg.DstAdapter,
	)
	// Per-queue list pagination uses each side's operation profile at init.
	scaling.ApplyQueueListPagination(srcQueue, srcListProfile)
	scaling.ApplyQueueListPagination(dstQueue, dstListProfile)

	if cfg.ResumeTraversal != nil {
		srcQueue.SetRound(0)
		dstQueue.SetRound(0)
		srcQueue.EnsureRoundExpectedFromStats()
		dstQueue.EnsureRoundExpectedFromStats()
		srcQueue.SetTraversalCacheLoaded(true)
		dstQueue.SetTraversalCacheLoaded(true)
		time.Sleep(500 * time.Millisecond)
		srcQueue.PullTasksIfNeeded(true)
		dstQueue.PullTasksIfNeeded(true)
	} else {
		if err := initializeQueues(cfg, srcQueue, dstQueue, coordinator); err != nil {
			return RuntimeStats{}, err
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Start traversal phase: drop indexes, persistent appenders. CHECKPOINT runs once after phase teardown (defer order below), not per-queue complete.
	phaseCtx := context.Background()
	if cfg.ShutdownContext != nil {
		phaseCtx = cfg.ShutdownContext
	}
	if err := database.BeginTraversalPhase(phaseCtx); err != nil {
		return RuntimeStats{}, fmt.Errorf("begin traversal phase: %w", err)
	}
	defer func() {
		if err := database.CheckpointWithRetry(context.Background(), 8); err != nil {
			fmt.Println("checkpoint after traversal phase:", err)
		}
	}()
	defer func() {
		if err := database.EndTraversalPhase(); err != nil {
			fmt.Println("error ending traversal phase", err)
		}
	}()

	obsPoll := observerPollFromConfigAndSuspend(cfg.ObserverPollInterval, cfg.ResumeTraversal)
	observer := queue.NewQueueObserver(database, obsPoll)
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

	runCtx := cfg.ShutdownContext
	if runCtx == nil {
		runCtx = context.Background()
	}
	asCtx := startAutoscaler(runCtx, cfg.Autoscaler, observer, database, srcQueue, dstQueue, cfg.SrcService, cfg.DstService)
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
				performTraversalForceStop(database, srcQueue, dstQueue, observer)
				srcStats, dstStats := snapshotTraversalQueueStats(database, coordinator)
				return RuntimeStats{
					Duration: time.Since(start),
					Src:      srcStats,
					Dst:      dstStats,
				}, forceStopErr("traversal")
			default:
			}
		}

		if cfg.SoftSuspendRequested != nil && cfg.SoftSuspendRequested() {
			waitCtx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			stats, suspend, err := performTraversalSoftSuspend(waitCtx, database, srcQueue, dstQueue, observer, coordinator, cfg, start, srcWC, mr)
			cancel()
			if err != nil {
				return stats, fmt.Errorf("traversal soft suspend: %w", err)
			}
			fmt.Print("\n")
			return stats, newTraversalSuspendedError(stats, suspend)
		}

		// Check exhaustion status from coordinator (source of truth)
		// Queues call MarkSrcCompleted()/MarkDstCompleted() on coordinator when done
		bothCompleted := coordinator.IsCompleted("both")

		if bothCompleted {
			observer.Stop()
			time.Sleep(250 * time.Millisecond) // let observer loop exit before we close the logger
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
	c, err := database.GetStatsCountAtDepth("SRC", srcRound, db.StatsKeyTraversalStatus(db.StatusPending))
	if err != nil {
		fmt.Println("error getting stats count at depth", err)
		return queue.QueueStats{}, queue.QueueStats{}
	}
	srcPending = int(c)
	c, err = database.GetStatsCountAtDepth("DST", dstRound, db.StatsKeyTraversalStatus(db.StatusPending))
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
	srcRound := 0
	dstRound := 0
	if cfg.ResumeStatus != nil {
		if cfg.ResumeStatus.MinPendingDepthSrc != nil {
			srcRound = *cfg.ResumeStatus.MinPendingDepthSrc
		}
		if cfg.ResumeStatus.MinPendingDepthDst != nil {
			dstRound = *cfg.ResumeStatus.MinPendingDepthDst
		}
	}

	// DuckDB is the primary database - no SQLite seeding needed

	// Set queue rounds
	srcQueue.SetRound(srcRound)
	dstQueue.SetRound(dstRound)

	// Set Expected from stats bucket (O(1) per queue; round 0 = 1 for traversal)
	srcQueue.EnsureRoundExpectedFromStats()
	dstQueue.EnsureRoundExpectedFromStats()

	if coordinator != nil {
		coordinator.UpdateRound("src", srcRound)
		if dstRound >= 0 {
			coordinator.UpdateRound("dst", dstRound)
		} else {
			coordinator.MarkCompleted("dst")
		}
	}

	// Don't pull tasks here - let Run() handle the initial pull
	// DST will check coordinator when it needs to advance

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
