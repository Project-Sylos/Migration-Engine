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
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/loop"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/profile"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// Gotta, sweep sweep sweep!!! 🧹🧹🧹
// SweepConfig is the configuration for running retry sweeps.
type SweepConfig struct {
	DuckDB          *db.DB
	SrcAdapter      types.FSAdapter
	DstAdapter      types.FSAdapter
	WorkerCount     int
	MaxRetries      int
	LogAddress      string
	LogLevel        string
	SkipListener    bool
	StartupDelay    time.Duration
	ProgressTick    time.Duration
	ShutdownContext context.Context
	// For retry sweeps only
	MaxKnownDepth          int    // Maximum known depth from previous traversal (-1 to auto-detect)
	SkipAutoETLBeforeRetry bool   // If true, skip automatic ETL from DuckDB to DuckDB before retry sweep
	DuckDBPath             string // Optional: Path to DuckDB file (auto-derived from DuckDB path if empty and ETL is enabled)
	SoftSuspendRequested   func() bool
	ObserverPollInterval   time.Duration
	OnQueueObserver        func(*observe.QueueObserver)
	OnAutoscaler           func(*loop.Autoscaler)
	LeaseBatchSize         int
	RefillBatchSize        int
	Autoscaler             AutoscalerConfig
	SrcService             Service
	DstService             Service
	PathCheckTarget        string
	WindowsCompat          bool
}

// RunRetrySweep runs a retry sweep to re-process failed or pending tasks from a previous traversal.
// This allows re-traversing paths that previously failed (e.g., due to permissions) and discovering
// new content in those paths.
//
// The sweep checks all known levels up to maxKnownDepth (or auto-detects from DB if -1),
// then uses normal traversal logic for deeper levels discovered during retry.
//
// Example:
//
//	config := migration.SweepConfig{
//	    DuckDB:        dbInstance,
//	    SrcAdapter:    srcAdapter,
//	    DstAdapter:    dstAdapter,
//	    WorkerCount:   10,
//	    MaxRetries:    3,
//	    MaxKnownDepth: 5, // Or -1 to auto-detect
//	}
//	stats, err := migration.RunRetrySweep(config)
func RunRetrySweep(cfg SweepConfig) (RuntimeStats, error) {
	if cfg.DuckDB == nil {
		return RuntimeStats{}, fmt.Errorf("duckDB cannot be nil")
	}
	if cfg.SrcAdapter == nil || cfg.DstAdapter == nil {
		return RuntimeStats{}, fmt.Errorf("source and destination adapters must be provided")
	}

	duckDB := cfg.DuckDB

	// Initialize log service if address is provided.
	// When SkipListener: bind port and discard UDP packets so sender writes don't fail; no display.
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
		if err := logservice.InitGlobalLogger(duckDB, cfg.LogAddress, cfg.LogLevel); err != nil {
			return RuntimeStats{}, fmt.Errorf("failed to initialize logger: %w", err)
		}
	}

	// Create coordinator for round advancement gates (retry uses traversal-like coordination)
	coordinator := queue.NewQueueCoordinator()

	// Get max known depth from config or detect from stats table
	maxKnownDepth := cfg.MaxKnownDepth
	if maxKnownDepth < 0 {
		d, err := stats.GetMaxDepth(duckDB, "SRC")
		if err == nil {
			maxKnownDepth = d
		}
		if maxKnownDepth < 0 {
			maxKnownDepth = 0 // Default to 0 if no levels found
		}
	}

	var qsz *queue.QueueSizing
	if cfg.LeaseBatchSize > 0 || cfg.RefillBatchSize > 0 {
		qsz = &queue.QueueSizing{LeaseBatchSize: cfg.LeaseBatchSize, RefillBatchSize: cfg.RefillBatchSize}
	}

	srcCtx := scalingContextForTraversal("src", cfg.SrcService, cfg.DstService, queue.QueueModeRetry)
	dstCtx := scalingContextForTraversal("dst", cfg.SrcService, cfg.DstService, queue.QueueModeRetry)
	if qsz == nil {
		qsz = queueSizingForScalingContext(srcCtx, nil)
	}
	srcWC := resolveWorkersForScalingContext(srcCtx, cfg.WorkerCount, nil)
	dstWC := resolveWorkersForScalingContext(dstCtx, cfg.WorkerCount, nil)

	// Create queues in retry mode
	srcQueue := queue.NewQueue("src", cfg.MaxRetries, srcWC, coordinator, qsz)
	srcQueue.SetMode(queue.QueueModeRetry)
	srcQueue.SetMaxKnownDepth(maxKnownDepth)
	seedQueueCountersFromDB(duckDB, srcQueue, "src-traversal", db.QueueStatsPhaseTraversal)

	dstQueue := queue.NewQueue("dst", cfg.MaxRetries, dstWC, coordinator, qsz)
	dstQueue.SetMode(queue.QueueModeRetry)
	if cfg.MaxKnownDepth >= 0 {
		dstQueue.SetMaxKnownDepth(cfg.MaxKnownDepth)
	}
	seedQueueCountersFromDB(duckDB, dstQueue, "dst-traversal", db.QueueStatsPhaseTraversal)

	srcQueue.InitializeWithContext(duckDB, cfg.SrcAdapter, cfg.ShutdownContext)
	dstQueue.InitializeWithContext(duckDB, cfg.DstAdapter, cfg.ShutdownContext)

	srcListProfile := profile.ResolveEffectiveProfile(
		scalingContextForTraversal("src", cfg.SrcService, cfg.DstService, queue.QueueModeRetry),
		cfg.SrcAdapter, cfg.DstAdapter,
	)
	dstListProfile := profile.ResolveEffectiveProfile(
		scalingContextForTraversal("dst", cfg.SrcService, cfg.DstService, queue.QueueModeRetry),
		cfg.SrcAdapter, cfg.DstAdapter,
	)
	profile.ApplyQueueListPagination(srcQueue, srcListProfile)
	profile.ApplyQueueListPagination(dstQueue, dstListProfile)

	// Set initial rounds to 0 for retry sweep
	srcQueue.SetRound(0)
	dstQueue.SetRound(0)
	srcQueue.SetExpectedFromStatsBucket(srcQueue.GetRound())
	dstQueue.SetExpectedFromStatsBucket(dstQueue.GetRound())
	srcQueue.SetTraversalCacheLoaded(true)
	dstQueue.SetTraversalCacheLoaded(true)

	// Give queues a moment to start
	time.Sleep(500 * time.Millisecond)

	// Trigger initial pull from database (force=true to bypass low-water checks)
	// This is critical for event-driven Run() loop - without initial tasks, workers never trigger pulls
	srcQueue.PullTasksIfNeeded(true)
	dstQueue.PullTasksIfNeeded(true)

	obsPoll := cfg.ObserverPollInterval
	if obsPoll <= 0 {
		obsPoll = 500 * time.Millisecond
	}
	observer := observe.NewQueueObserver(duckDB, obsPoll)
	observer.Start()
	defer observer.Stop()
	if cfg.OnQueueObserver != nil {
		defer cfg.OnQueueObserver(nil)
	}

	srcQueue.SetObserver(observer)
	dstQueue.SetObserver(observer)
	if cfg.OnQueueObserver != nil {
		cfg.OnQueueObserver(observer)
	}

	runCtx := cfg.ShutdownContext
	if runCtx == nil {
		runCtx = context.Background()
	}
	asCtx := startAutoscaler(runCtx, cfg.Autoscaler, observer, duckDB, srcQueue, dstQueue, cfg.SrcService, cfg.DstService, cfg.PathCheckTarget, cfg.WindowsCompat)
	if cfg.OnAutoscaler != nil {
		cfg.OnAutoscaler(asCtx.Autoscaler())
		defer cfg.OnAutoscaler(nil)
	}
	defer asCtx.stop()

	// Set up stats channels for progress updates
	srcStatsChan := make(chan queue.QueueStats, 10)
	dstStatsChan := make(chan queue.QueueStats, 10)
	srcQueue.SetStatsChannel(srcStatsChan)
	dstQueue.SetStatsChannel(dstStatsChan)

	statsCtx, statsCancel := context.WithCancel(context.Background())
	defer statsCancel()

	// Start stats consumer goroutine for progress updates
	go func() {
		var lastSrcStats *queue.QueueStats
		var lastDstStats *queue.QueueStats

		for {
			select {
			case <-statsCtx.Done():
				return
			case srcStats := <-srcStatsChan:
				lastSrcStats = &srcStats
				if lastDstStats != nil {
					srcRoundStats := srcQueue.GetRoundStats(lastSrcStats.Round)
					dstRoundStats := dstQueue.GetRoundStats(lastDstStats.Round)

					srcExpected := 0
					srcCompleted := 0
					if srcRoundStats != nil {
						srcExpected = srcRoundStats.Expected
						srcCompleted = srcRoundStats.Completed
					}

					dstExpected := 0
					dstCompleted := 0
					if dstRoundStats != nil {
						dstExpected = dstRoundStats.Expected
						dstCompleted = dstRoundStats.Completed
					}

					fmt.Printf("\r  Retry Sweep - Src: Round %d (Expected:%d Completed:%d) | Dst: Round %d (Expected:%d Completed:%d)   ",
						lastSrcStats.Round, srcExpected, srcCompleted,
						lastDstStats.Round, dstExpected, dstCompleted)
				}
			case dstStats := <-dstStatsChan:
				lastDstStats = &dstStats
				if lastSrcStats != nil {
					srcRoundStats := srcQueue.GetRoundStats(lastSrcStats.Round)
					dstRoundStats := dstQueue.GetRoundStats(lastDstStats.Round)

					srcExpected := 0
					srcCompleted := 0
					if srcRoundStats != nil {
						srcExpected = srcRoundStats.Expected
						srcCompleted = srcRoundStats.Completed
					}

					dstExpected := 0
					dstCompleted := 0
					if dstRoundStats != nil {
						dstExpected = dstRoundStats.Expected
						dstCompleted = dstRoundStats.Completed
					}

					fmt.Printf("\r  Retry Sweep - Src: Round %d (Expected:%d Completed:%d) | Dst: Round %d (Expected:%d Completed:%d)   ",
						lastSrcStats.Round, srcExpected, srcCompleted,
						lastDstStats.Round, dstExpected, dstCompleted)
				}
			}
		}
	}()

	// Ensure ProgressTick is positive
	progressTick := cfg.ProgressTick
	if progressTick <= 0 {
		progressTick = 1 * time.Second
	}
	progressTicker := time.NewTicker(progressTick)
	defer progressTicker.Stop()
	start := time.Now()
	sweepStartNanos := start.UnixNano()

	mr := cfg.MaxRetries

	// Wait for both queues to complete
	for {
		if cfg.ShutdownContext != nil {
			select {
			case <-cfg.ShutdownContext.Done():
				_ = duckDB.Flush()
				_ = queue.FinalizeCopyWorkOnStop(duckDB, coordinator)
				srcQueue.SetState(queue.QueueStatePaused)
				dstQueue.SetState(queue.QueueStatePaused)

				time.Sleep(200 * time.Millisecond)

				srcStats := srcQueue.Stats()
				dstStats := dstQueue.Stats()

				return RuntimeStats{
					Duration: time.Since(start),
					Src:      srcStats,
					Dst:      dstStats,
				}, fmt.Errorf("retry sweep suspended by force shutdown")
			default:
			}
		}

		if cfg.SoftSuspendRequested != nil && cfg.SoftSuspendRequested() {
			mcfg := MigrationConfig{
				ProgressTick:         cfg.ProgressTick,
				ObserverPollInterval: cfg.ObserverPollInterval,
				ShutdownContext:      cfg.ShutdownContext,
			}
			if mcfg.ObserverPollInterval <= 0 {
				mcfg.ObserverPollInterval = obsPoll
			}
			waitCtx, cancel := softSuspendWaitContext(cfg.ShutdownContext)
			stats, suspend, err := performTraversalSoftSuspend(waitCtx, duckDB, srcQueue, dstQueue, observer, coordinator, mcfg, start, srcWC, mr)
			cancel()
			if err != nil {
				return stats, fmt.Errorf("retry sweep soft suspend: %w", err)
			}
			fmt.Print("\n")
			return stats, newTraversalSuspendedError(stats, suspend)
		}

		// Check if both queues are completed
		bothCompleted := coordinator.IsCompleted("both")

		if bothCompleted {
			srcStats := srcQueue.Stats()
			dstStats := dstQueue.Stats()

			fmt.Println("\nRetry sweep complete!")

			// Close log service before returning
			if logservice.LS != nil {
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer closeCancel()

				closeDone := make(chan struct{}, 1)
				go func() {
					err := logservice.LS.Close()
					if err != nil {
						fmt.Println("error closing logger", err)
					}
					closeDone <- struct{}{}
				}()

				select {
				case <-closeDone:
				case <-closeCtx.Done():
				}
			}

			if err := duckDB.EnsureBulkPhaseSecondaryIndexes(); err != nil {
				return RuntimeStats{
					Duration: time.Since(start),
					Src:      srcStats,
					Dst:      dstStats,
				}, fmt.Errorf("retry sweep complete but secondary indexes: %w", err)
			}
			if err := duckDB.CheckpointWithRetry(context.Background(), 8); err != nil {
				fmt.Println("checkpoint after retry sweep:", err)
			}
			rebuildCurrentSinceSweep(duckDB, sweepStartNanos)

			return RuntimeStats{
				Duration: time.Since(start),
				Src:      srcStats,
				Dst:      dstStats,
			}, nil
		}

		select {
		case <-progressTicker.C:
			// Re-check completion
			if coordinator.IsCompleted("both") {
				srcStats := srcQueue.Stats()
				dstStats := dstQueue.Stats()

				fmt.Println("\nRetry sweep complete!")

				// Close log service
				if logservice.LS != nil {
					closeCtx, closeCancel := context.WithTimeout(context.Background(), 1*time.Second)
					defer closeCancel()

					closeDone := make(chan struct{}, 1)
					go func() {
						err := logservice.LS.Close()
						if err != nil {
							fmt.Println("error closing logger", err)
						}
						closeDone <- struct{}{}
					}()

					select {
					case <-closeDone:
					case <-closeCtx.Done():
					}
				}

				if err := duckDB.EnsureBulkPhaseSecondaryIndexes(); err != nil {
					return RuntimeStats{
						Duration: time.Since(start),
						Src:      srcStats,
						Dst:      dstStats,
					}, fmt.Errorf("retry sweep complete but secondary indexes: %w", err)
				}
				if err := duckDB.CheckpointWithRetry(context.Background(), 8); err != nil {
					fmt.Println("checkpoint after retry sweep:", err)
				}
				rebuildCurrentSinceSweep(duckDB, sweepStartNanos)

				return RuntimeStats{
					Duration: time.Since(start),
					Src:      srcStats,
					Dst:      dstStats,
				}, nil
			}
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func rebuildCurrentSinceSweep(duckDB *db.DB, sweepStartNanos int64) {
	if err := duckDB.RebuildCurrentSince("SRC", sweepStartNanos); err != nil {
		fmt.Println("rebuild src_current after retry sweep:", err)
	}
	if err := duckDB.RebuildCurrentSince("DST", sweepStartNanos); err != nil {
		fmt.Println("rebuild dst_current after retry sweep:", err)
	}
}
