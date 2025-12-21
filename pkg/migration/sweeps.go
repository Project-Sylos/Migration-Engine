// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"fmt"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
	"github.com/Project-Sylos/Migration-Engine/pkg/queue"
	"github.com/Project-Sylos/Sylos-DB/pkg/store"
	"github.com/Project-Sylos/Sylos-FS/pkg/types"
)

// Gotta, sweep sweep sweep!!! 🧹🧹🧹
// SweepConfig is the configuration for running exclusion or retry sweeps.
type SweepConfig struct {
	StoreInstance   *store.Store
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
	MaxKnownDepth int // Maximum known depth from previous traversal (-1 to auto-detect)
}

// RunExclusionSweep runs an exclusion sweep to propagate exclusion/unexclusion through affected subtrees.
// This should be called after the API has populated the exclusion-holding bucket with exclusion intents
// and before starting the copy phase.
//
// The sweep processes entries from the exclusion-holding bucket in BFS rounds, propagating
// inherited_excluded flags through the entire subtree for each excluded/unexcluded node.
//
// Example:
//
//	config := migration.SweepConfig{
//	    BoltDB:      dbInstance,
//	    SrcAdapter:  srcAdapter,
//	    DstAdapter:  dstAdapter,
//	    WorkerCount: 10,
//	    MaxRetries:  3,
//	}
//	stats, err := migration.RunExclusionSweep(config)
func RunExclusionSweep(cfg SweepConfig) (RuntimeStats, error) {
	if cfg.StoreInstance == nil {
		return RuntimeStats{}, fmt.Errorf("store instance cannot be nil")
	}
	if cfg.SrcAdapter == nil || cfg.DstAdapter == nil {
		return RuntimeStats{}, fmt.Errorf("source and destination adapters must be provided")
	}

	storeInstance := cfg.StoreInstance

	// Initialize log service if address is provided
	if cfg.LogAddress != "" {
		if !cfg.SkipListener {
			if err := logservice.StartListener(cfg.LogAddress); err != nil {
				// Non-fatal: continue without listener
			} else {
				startupDelay := cfg.StartupDelay
				if startupDelay <= 0 {
					startupDelay = 500 * time.Millisecond
				}
				time.Sleep(startupDelay)
			}
		}
		if err := logservice.InitGlobalLogger(storeInstance, cfg.LogAddress, cfg.LogLevel); err != nil {
			return RuntimeStats{}, fmt.Errorf("failed to initialize logger: %w", err)
		}
	}

	// Get max known depth from levels bucket for SRC
	// Exclusion sweep should scan all existing levels, then complete
	maxKnownDepth, err := storeInstance.GetMaxKnownDepth("SRC")
	if err != nil {
		return RuntimeStats{}, fmt.Errorf("failed to get max known depth: %w", err)
	}
	if maxKnownDepth < 0 {
		maxKnownDepth = 0 // Default to 0 if no levels found
	}

	// Create coordinator (minimal - exclusion sweeps don't need round coordination)
	coordinator := queue.NewQueueCoordinator()

	// Create queues in exclusion mode
	srcQueue := queue.NewQueue("src", cfg.MaxRetries, cfg.WorkerCount, coordinator)
	srcQueue.SetMode(queue.QueueModeExclusion)
	srcQueue.SetMaxKnownDepth(maxKnownDepth)
	srcQueue.InitializeWithContext(storeInstance, cfg.SrcAdapter, cfg.ShutdownContext)

	dstQueue := queue.NewQueue("dst", cfg.MaxRetries, cfg.WorkerCount, coordinator)
	dstQueue.SetMode(queue.QueueModeExclusion)
	dstQueue.InitializeWithContext(storeInstance, cfg.DstAdapter, cfg.ShutdownContext)

	// Set initial rounds to 0 for exclusion sweep (starts from root level)
	srcQueue.SetRound(0)
	dstQueue.SetRound(0)

	// Give queues a moment to start
	time.Sleep(100 * time.Millisecond)

	// Trigger initial pull from exclusion-holding buckets (force=true to bypass low-water checks)
	// This is critical for event-driven Run() loop - without initial tasks, workers never trigger pulls
	srcQueue.PullTasksIfNeeded(true)
	dstQueue.PullTasksIfNeeded(true)

	// Create observer for stats publishing
	observer := queue.NewQueueObserver(storeInstance, 200*time.Millisecond)
	observer.Start()
	defer observer.Stop()

	srcQueue.SetObserver(observer)
	dstQueue.SetObserver(observer)

	// Set up stats channels for progress updates
	srcStatsChan := make(chan queue.QueueStats, 10)
	dstStatsChan := make(chan queue.QueueStats, 10)
	srcQueue.SetStatsChannel(srcStatsChan)
	dstQueue.SetStatsChannel(dstStatsChan)

	// Start stats consumer goroutine for progress updates
	go func() {
		var lastSrcStats *queue.QueueStats
		var lastDstStats *queue.QueueStats

		for {
			select {
			case srcStats := <-srcStatsChan:
				lastSrcStats = &srcStats
				if lastDstStats != nil {
					fmt.Printf("\r  Exclusion Sweep - Src: Round %d (Pending:%d InProgress:%d) | Dst: Round %d (Pending:%d InProgress:%d)   ",
						lastSrcStats.Round, lastSrcStats.Pending, lastSrcStats.InProgress,
						lastDstStats.Round, lastDstStats.Pending, lastDstStats.InProgress)
				}
			case dstStats := <-dstStatsChan:
				lastDstStats = &dstStats
				if lastSrcStats != nil {
					fmt.Printf("\r  Exclusion Sweep - Src: Round %d (Pending:%d InProgress:%d) | Dst: Round %d (Pending:%d InProgress:%d)   ",
						lastSrcStats.Round, lastSrcStats.Pending, lastSrcStats.InProgress,
						lastDstStats.Round, lastDstStats.Pending, lastDstStats.InProgress)
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

	// Wait for both queues to complete
	for {
		// Check for force shutdown
		if cfg.ShutdownContext != nil {
			select {
			case <-cfg.ShutdownContext.Done():
				// Force shutdown
				srcQueue.Pause()
				dstQueue.Pause()

				time.Sleep(200 * time.Millisecond)

				srcStats := srcQueue.Stats()
				dstStats := dstQueue.Stats()

				return RuntimeStats{
					Duration: time.Since(start),
					Src:      srcStats,
					Dst:      dstStats,
				}, fmt.Errorf("exclusion sweep suspended by force shutdown")
			default:
			}
		}

		// Check if both queues are completed
		if srcQueue.IsExhausted() && dstQueue.IsExhausted() {
			srcStats := srcQueue.Stats()
			dstStats := dstQueue.Stats()

			fmt.Println("\nExclusion sweep complete!")

			// Close log service before returning
			if logservice.LS != nil {
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer closeCancel()

				closeDone := make(chan struct{}, 1)
				go func() {
					_ = logservice.LS.Close()
					closeDone <- struct{}{}
				}()

				select {
				case <-closeDone:
				case <-closeCtx.Done():
				}
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
			if srcQueue.IsExhausted() && dstQueue.IsExhausted() {
				srcStats := srcQueue.Stats()
				dstStats := dstQueue.Stats()

				fmt.Println("\nExclusion sweep complete!")

				// Close log service
				if logservice.LS != nil {
					closeCtx, closeCancel := context.WithTimeout(context.Background(), 1*time.Second)
					defer closeCancel()

					closeDone := make(chan struct{}, 1)
					go func() {
						_ = logservice.LS.Close()
						closeDone <- struct{}{}
					}()

					select {
					case <-closeDone:
					case <-closeCtx.Done():
					}
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
//	    BoltDB:        dbInstance,
//	    SrcAdapter:    srcAdapter,
//	    DstAdapter:    dstAdapter,
//	    WorkerCount:   10,
//	    MaxRetries:    3,
//	    MaxKnownDepth: 5, // Or -1 to auto-detect
//	}
//	stats, err := migration.RunRetrySweep(config)
func RunRetrySweep(cfg SweepConfig) (RuntimeStats, error) {
	if cfg.StoreInstance == nil {
		return RuntimeStats{}, fmt.Errorf("store instance cannot be nil")
	}
	if cfg.SrcAdapter == nil || cfg.DstAdapter == nil {
		return RuntimeStats{}, fmt.Errorf("source and destination adapters must be provided")
	}

	storeInstance := cfg.StoreInstance

	// Initialize log service if address is provided
	if cfg.LogAddress != "" {
		if !cfg.SkipListener {
			if err := logservice.StartListener(cfg.LogAddress); err != nil {
				// Non-fatal: continue without listener
			} else {
				startupDelay := cfg.StartupDelay
				if startupDelay <= 0 {
					startupDelay = 500 * time.Millisecond
				}
				time.Sleep(startupDelay)
			}
		}
		if err := logservice.InitGlobalLogger(storeInstance, cfg.LogAddress, cfg.LogLevel); err != nil {
			return RuntimeStats{}, fmt.Errorf("failed to initialize logger: %w", err)
		}
	}

	// Create coordinator for round advancement gates (retry uses traversal-like coordination)
	coordinator := queue.NewQueueCoordinator()

	// Get max known depth from config or detect from levels bucket
	maxKnownDepth := cfg.MaxKnownDepth
	if maxKnownDepth < 0 {
		// Auto-detect from levels bucket
		maxKnownDepth, err := storeInstance.GetMaxKnownDepth("SRC")
		if err != nil || maxKnownDepth < 0 {
			maxKnownDepth = 0 // Default to 0 if no levels found
		}
	}

	// Create queues in retry mode
	srcQueue := queue.NewQueue("src", cfg.MaxRetries, cfg.WorkerCount, coordinator)
	srcQueue.SetMode(queue.QueueModeRetry)
	srcQueue.SetMaxKnownDepth(maxKnownDepth)
	srcQueue.InitializeWithContext(storeInstance, cfg.SrcAdapter, cfg.ShutdownContext)

	dstQueue := queue.NewQueue("dst", cfg.MaxRetries, cfg.WorkerCount, coordinator)
	dstQueue.SetMode(queue.QueueModeRetry)
	if cfg.MaxKnownDepth >= 0 {
		dstQueue.SetMaxKnownDepth(cfg.MaxKnownDepth)
	}
	dstQueue.InitializeWithContext(storeInstance, cfg.DstAdapter, cfg.ShutdownContext)

	// Set initial rounds to 0 for retry sweep
	srcQueue.SetRound(0)
	dstQueue.SetRound(0)

	// Give queues a moment to start
	time.Sleep(100 * time.Millisecond)

	// Trigger initial pull from database (force=true to bypass low-water checks)
	// This is critical for event-driven Run() loop - without initial tasks, workers never trigger pulls
	srcQueue.PullTasksIfNeeded(true)
	dstQueue.PullTasksIfNeeded(true)

	// Create observer for stats publishing
	observer := queue.NewQueueObserver(storeInstance, 200*time.Millisecond)
	observer.Start()
	defer observer.Stop()

	srcQueue.SetObserver(observer)
	dstQueue.SetObserver(observer)

	// Set up stats channels for progress updates
	srcStatsChan := make(chan queue.QueueStats, 10)
	dstStatsChan := make(chan queue.QueueStats, 10)
	srcQueue.SetStatsChannel(srcStatsChan)
	dstQueue.SetStatsChannel(dstStatsChan)

	// Start stats consumer goroutine for progress updates
	go func() {
		var lastSrcStats *queue.QueueStats
		var lastDstStats *queue.QueueStats

		for {
			select {
			case srcStats := <-srcStatsChan:
				lastSrcStats = &srcStats
				if lastDstStats != nil {
					srcRoundStats := srcQueue.RoundStats(lastSrcStats.Round)
					dstRoundStats := dstQueue.RoundStats(lastDstStats.Round)

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

					fmt.Printf("\r  Retry Sweep - Src: Round %d (Pending:%d InProgress:%d Expected:%d Completed:%d) | Dst: Round %d (Pending:%d InProgress:%d Expected:%d Completed:%d)   ",
						lastSrcStats.Round, lastSrcStats.Pending, lastSrcStats.InProgress, srcExpected, srcCompleted,
						lastDstStats.Round, lastDstStats.Pending, lastDstStats.InProgress, dstExpected, dstCompleted)
				}
			case dstStats := <-dstStatsChan:
				lastDstStats = &dstStats
				if lastSrcStats != nil {
					srcRoundStats := srcQueue.RoundStats(lastSrcStats.Round)
					dstRoundStats := dstQueue.RoundStats(lastDstStats.Round)

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

					fmt.Printf("\r  Retry Sweep - Src: Round %d (Pending:%d InProgress:%d Expected:%d Completed:%d) | Dst: Round %d (Pending:%d InProgress:%d Expected:%d Completed:%d)   ",
						lastSrcStats.Round, lastSrcStats.Pending, lastSrcStats.InProgress, srcExpected, srcCompleted,
						lastDstStats.Round, lastDstStats.Pending, lastDstStats.InProgress, dstExpected, dstCompleted)
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

	// Wait for both queues to complete
	for {
		// Check for force shutdown
		if cfg.ShutdownContext != nil {
			select {
			case <-cfg.ShutdownContext.Done():
				// Force shutdown
				srcQueue.Pause()
				dstQueue.Pause()

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
					_ = logservice.LS.Close()
					closeDone <- struct{}{}
				}()

				select {
				case <-closeDone:
				case <-closeCtx.Done():
				}
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
						_ = logservice.LS.Close()
						closeDone <- struct{}{}
					}()

					select {
					case <-closeDone:
					case <-closeCtx.Done():
					}
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
