// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
	"codeberg.org/Sylos/Migration-Engine/pkg/filter"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/profile"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// Gotta, sweep sweep sweep!!! 🧹🧹🧹
// SweepConfig is the configuration for running retry sweeps.
type SweepConfig struct {
	PhaseRuntime
	DB         *db.DB
	SrcAdapter types.FSAdapter
	DstAdapter types.FSAdapter
	// For retry sweeps only
	MaxKnownDepth int // Maximum known depth from previous traversal (-1 to auto-detect)
	FilterRuleset *filter.CompiledRuleset
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
//	    DB:        dbInstance,
//	    SrcAdapter:    srcAdapter,
//	    DstAdapter:    dstAdapter,
//	    WorkerCount:   10,
//	    MaxRetries:    3,
//	    MaxKnownDepth: 5, // Or -1 to auto-detect
//	}
//	stats, err := migration.RunRetrySweep(config)
func RunRetrySweep(cfg SweepConfig) (RuntimeStats, error) {
	database := cfg.DB
	if err := cfg.Begin(database, cfg.SrcAdapter, cfg.DstAdapter); err != nil {
		return RuntimeStats{}, err
	}

	// Create coordinator for round advancement gates (retry uses traversal-like coordination)
	coordinator := queue.NewQueueCoordinator()

	// Get max known depth from config or detect from stats table
	maxKnownDepth := cfg.MaxKnownDepth
	if maxKnownDepth < 0 {
		d, err := stats.GetMaxDepth(database, "SRC")
		if err == nil {
			maxKnownDepth = d
		}
		if maxKnownDepth < 0 {
			maxKnownDepth = 0 // Default to 0 if no levels found
		}
	}

	srcCtx := newScalingContext("src", queue.ScalingModeTraversal, queue.QueueModeRetry, 0, cfg.SrcService, cfg.DstService)
	dstCtx := newScalingContext("dst", queue.ScalingModeTraversal, queue.QueueModeRetry, 0, cfg.SrcService, cfg.DstService)
	qsz := queueSizingForScalingContext(srcCtx, nil)
	srcWC := resolveWorkersForScalingContext(srcCtx, cfg.WorkerCount, nil)
	dstWC := resolveWorkersForScalingContext(dstCtx, cfg.WorkerCount, nil)

	// Create queues in retry mode
	srcQueue := queue.NewQueue("src", cfg.MaxRetries, srcWC, coordinator, qsz)
	applyFSStallTimeouts(srcQueue, cfg.PhaseRuntime)
	srcQueue.SetMode(queue.QueueModeRetry)
	srcQueue.SetMaxKnownDepth(maxKnownDepth)
	seedQueueCountersFromDB(database, srcQueue, "src-traversal", db.QueueStatsPhaseTraversal)

	dstQueue := queue.NewQueue("dst", cfg.MaxRetries, dstWC, coordinator, qsz)
	applyFSStallTimeouts(dstQueue, cfg.PhaseRuntime)
	dstQueue.SetMode(queue.QueueModeRetry)
	if cfg.MaxKnownDepth >= 0 {
		dstQueue.SetMaxKnownDepth(cfg.MaxKnownDepth)
	}
	seedQueueCountersFromDB(database, dstQueue, "dst-traversal", db.QueueStatsPhaseTraversal)

	srcQueue.Initialize(queue.PoolTraversal, database, cfg.SrcAdapter, nil, cfg.ShutdownContext)
	dstQueue.Initialize(queue.PoolTraversal, database, cfg.DstAdapter, cfg.SrcAdapter, cfg.ShutdownContext)

	srcListProfile := profile.ResolveEffectiveProfile(srcCtx, cfg.SrcAdapter, cfg.DstAdapter)
	dstListProfile := profile.ResolveEffectiveProfile(dstCtx, cfg.SrcAdapter, cfg.DstAdapter)
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
	observer := observe.NewQueueObserver(database, obsPoll)
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

	asCtx := cfg.startAutoscaler(observer, database, srcQueue, dstQueue)
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

	mr := cfg.MaxRetries

	// Wait for both queues to complete
	for {
		if cfg.ShutdownContext != nil {
			select {
			case <-cfg.ShutdownContext.Done():
				performTraversalForceStop(srcQueue, dstQueue, observer)
				database.AbortTraversalPhase()
				return RuntimeStats{
					Duration: time.Since(start),
					Src:      srcQueue.Stats(),
					Dst:      dstQueue.Stats(),
				}, fmt.Errorf("migration force stopped during %s", "retry sweep")
			default:
			}
		}

		if cfg.SoftSuspendRequested != nil && cfg.SoftSuspendRequested() {
			rt := cfg.PhaseRuntime
			if rt.ObserverPollInterval <= 0 {
				rt.ObserverPollInterval = obsPoll
			}
			waitCtx, cancel := softSuspendWaitContext(cfg.ShutdownContext)
			stats, suspend, err := performTraversalSoftSuspend(waitCtx, database, srcQueue, dstQueue, observer, coordinator, &rt, start, srcWC, mr)
			cancel()
			if forceStopOverridesSoftSuspend(cfg.ShutdownContext, database) {
				performTraversalForceStop(srcQueue, dstQueue, observer)
				database.AbortTraversalPhase()
				return RuntimeStats{
					Duration: time.Since(start),
					Src:      srcQueue.Stats(),
					Dst:      dstQueue.Stats(),
				}, fmt.Errorf("migration force stopped during %s", "retry sweep")
			}
			if err != nil {
				return stats, fmt.Errorf("retry sweep soft suspend: %w", err)
			}
			finishCtx := cfg.ShutdownContext
			if finishCtx == nil {
				finishCtx = context.Background()
			}
			setDetail := waitDetailSetter(observer)
			if err := finishSoftStopBulkPhase(finishCtx, database, cfg.ReportStopProgress, setDetail); err != nil {
				if forceStopOverridesSoftSuspend(cfg.ShutdownContext, database) {
					performTraversalForceStop(srcQueue, dstQueue, observer)
					database.AbortTraversalPhase()
					return RuntimeStats{
						Duration: time.Since(start),
						Src:      srcQueue.Stats(),
						Dst:      dstQueue.Stats(),
					}, fmt.Errorf("migration force stopped during %s", "retry sweep")
				}
				return stats, fmt.Errorf("retry sweep soft suspend teardown: %w", err)
			}
			fmt.Print("\n")
			return stats, newSuspendedError(ErrTraversalSoftSuspended, stats, suspend)
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

			if err := database.EnsureBulkPhaseSecondaryIndexes(); err != nil {
				return RuntimeStats{
					Duration: time.Since(start),
					Src:      srcStats,
					Dst:      dstStats,
				}, fmt.Errorf("retry sweep complete but secondary indexes: %w", err)
			}
			if err := database.CheckpointWithRetry(context.Background(), 8); err != nil {
				fmt.Println("checkpoint after retry sweep:", err)
			}

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

				if err := database.EnsureBulkPhaseSecondaryIndexes(); err != nil {
					return RuntimeStats{
						Duration: time.Since(start),
						Src:      srcStats,
						Dst:      dstStats,
					}, fmt.Errorf("retry sweep complete but secondary indexes: %w", err)
				}
				if err := database.CheckpointWithRetry(context.Background(), 8); err != nil {
					fmt.Println("checkpoint after retry sweep:", err)
				}

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
