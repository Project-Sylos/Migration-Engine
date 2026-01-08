// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"fmt"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/db/etl"
	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
	"github.com/Project-Sylos/Migration-Engine/pkg/queue"
	"github.com/Project-Sylos/Sylos-FS/pkg/types"
)

// Gotta, sweep sweep sweep!!! 🧹🧹🧹
// SweepConfig is the configuration for running retry sweeps.
type SweepConfig struct {
	BoltDB          *db.DB
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
	MaxKnownDepth          int                  // Maximum known depth from previous traversal (-1 to auto-detect)
	SkipAutoETLBeforeRetry bool                 // If true, skip automatic ETL from DuckDB to BoltDB before retry sweep
	DuckDBPath             string               // Optional: Path to DuckDB file (auto-derived from BoltDB path if empty and ETL is enabled)
	YAMLConfig             *MigrationConfigYAML // Optional: YAML config for status updates
	ConfigPath             string               // Optional: Path to YAML config file for status updates
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
	if cfg.BoltDB == nil {
		return RuntimeStats{}, fmt.Errorf("boltDB cannot be nil")
	}
	if cfg.SrcAdapter == nil || cfg.DstAdapter == nil {
		return RuntimeStats{}, fmt.Errorf("source and destination adapters must be provided")
	}

	boltDB := cfg.BoltDB

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
		if err := logservice.InitGlobalLogger(boltDB, cfg.LogAddress, cfg.LogLevel); err != nil {
			return RuntimeStats{}, fmt.Errorf("failed to initialize logger: %w", err)
		}
	}

	// Create coordinator for round advancement gates (retry uses traversal-like coordination)
	coordinator := queue.NewQueueCoordinator()

	// Get max known depth from config or detect from levels bucket
	maxKnownDepth := cfg.MaxKnownDepth
	if maxKnownDepth < 0 {
		// Auto-detect from levels bucket
		maxKnownDepth = boltDB.GetMaxKnownDepth(db.BucketSrc)
		if maxKnownDepth < 0 {
			maxKnownDepth = 0 // Default to 0 if no levels found
		}
	}

	// Run ETL from DuckDB to BoltDB if not skipped
	if !cfg.SkipAutoETLBeforeRetry {
		// Derive DuckDB path from BoltDB path if not provided
		duckDBPath := cfg.DuckDBPath
		if duckDBPath == "" {
			boltPath := boltDB.Path()
			if boltPath == "" {
				return RuntimeStats{}, fmt.Errorf("cannot derive DuckDB path: BoltDB path is not available")
			}
			duckDBPath = deriveDuckDBPath(boltPath, boltDB)
			if duckDBPath == "" {
				return RuntimeStats{}, fmt.Errorf("failed to derive DuckDB path from BoltDB path: %s", boltPath)
			}
		}

		// Update status to ETL in progress
		if cfg.YAMLConfig != nil && cfg.ConfigPath != "" {
			SetStatusETLDuckToBoltInProgress(cfg.YAMLConfig)
			_ = SaveMigrationConfig(cfg.ConfigPath, cfg.YAMLConfig)
		}

		// Run ETL with status callbacks
		etlCfg := etl.DuckToBoltConfig{
			BoltDB:      boltDB,
			DuckDBPath:  duckDBPath,
			Overwrite:   true,
			RequireOpen: true,
			OnETLStart: func() error {
				// Status already set above, but ensure it's saved
				if cfg.YAMLConfig != nil && cfg.ConfigPath != "" {
					return SaveMigrationConfig(cfg.ConfigPath, cfg.YAMLConfig)
				}
				return nil
			},
			OnETLComplete: func() error {
				// Update status to Filters-Set (ready for retry) when ETL completes
				if cfg.YAMLConfig != nil && cfg.ConfigPath != "" {
					SetStatusFiltersSet(cfg.YAMLConfig, true, maxKnownDepth)
					return SaveMigrationConfig(cfg.ConfigPath, cfg.YAMLConfig)
				}
				return nil
			},
		}

		if err := etl.RunDuckToBolt(etlCfg); err != nil {
			return RuntimeStats{}, fmt.Errorf("failed to run ETL before retry sweep: %w", err)
		}
	}

	// Create queues in retry mode
	srcQueue := queue.NewQueue("src", cfg.MaxRetries, cfg.WorkerCount, coordinator)
	srcQueue.SetMode(queue.QueueModeRetry)
	srcQueue.SetMaxKnownDepth(maxKnownDepth)
	srcQueue.InitializeWithContext(boltDB, cfg.SrcAdapter, cfg.ShutdownContext)

	dstQueue := queue.NewQueue("dst", cfg.MaxRetries, cfg.WorkerCount, coordinator)
	dstQueue.SetMode(queue.QueueModeRetry)
	if cfg.MaxKnownDepth >= 0 {
		dstQueue.SetMaxKnownDepth(cfg.MaxKnownDepth)
	}
	dstQueue.InitializeWithContext(boltDB, cfg.DstAdapter, cfg.ShutdownContext)

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
	observer := queue.NewQueueObserver(boltDB, 200*time.Millisecond)
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
