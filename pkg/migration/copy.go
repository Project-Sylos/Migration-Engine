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
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// CopyPhaseConfig configures the copy phase execution.
type CopyPhaseConfig struct {
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
}

// RunCopyPhase executes the copy phase (two-pass: folders then files).
func RunCopyPhase(cfg CopyPhaseConfig) (queue.QueueStats, error) {
	duckDB := cfg.DuckDB
	if duckDB == nil {
		return queue.QueueStats{}, fmt.Errorf("DuckDB must be provided")
	}

	if cfg.SrcAdapter == nil || cfg.DstAdapter == nil {
		return queue.QueueStats{}, fmt.Errorf("source and destination adapters must be provided")
	}

	// Initialize log service if address provided.
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
			return queue.QueueStats{}, fmt.Errorf("failed to initialize logger: %w", err)
		}
	}

	// Create copy queue (single queue, not dual like traversal)
	copyQueue := queue.NewQueue("copy", cfg.MaxRetries, cfg.WorkerCount, nil) // No coordinator needed for copy
	copyQueue.SetMode(queue.QueueModeCopy)
	copyQueue.SetCopyPass(1) // Start with pass 1 (folders)

	// Find minimum level with pending copy tasks (skip round 0)
	// Start at round 1 since round 0 (root) is skipped
	// Use -1 as sentinel to indicate we haven't found any pending level yet
	minLevel := -1
	levels, err := db.GetAllLevels(duckDB, "SRC")
	if err == nil && len(levels) > 0 {
		// Find minimum level with pending copy tasks (start with folders since pass 1 is folders)
		for _, level := range levels {
			if level == 0 {
				continue // Skip round 0
			}
			// Check folder tasks (pass 1 starts with folders)
			c, err := duckDB.GetCopyCountAtDepth(level, db.NodeTypeFolder, db.CopyStatusPending)
			if err == nil && c > 0 {
				// First pending level found OR current level is smaller than what we've found
				if minLevel == -1 || level < minLevel {
					minLevel = level
				}
			}
		}
	}

	// If no pending levels found, default to level 1
	if minLevel == -1 {
		minLevel = 1
	}
	copyQueue.SetRound(minLevel) // Set initial round
	copyQueue.EnsureRoundExpectedFromStats()

	// Set max known depth from DB so copy completion and round advancement know the full depth range.
	// Must be set before any tasks are pulled or completion checks run.
	if maxDepth, err := duckDB.GetMaxDepth("SRC"); err == nil {
		copyQueue.SetMaxKnownDepth(maxDepth)
	}

	// CRITICAL: Ensure root folder (level 0) has join-lookup mapping (DuckDB: join_id on DST node).
	srcID, _, srcOk := db.GetRootNode(duckDB, "SRC")
	srcRootID := ""
	if srcOk {
		srcRootID = srcID
	}
	if srcRootID == "" {
		if logservice.LS != nil {
			err := logservice.LS.Log("warn", fmt.Sprintf("Failed to ensure root join-lookup mapping: %v", fmt.Errorf("could not find SRC root node")), "migration", "copy", "copy")
			if err != nil {
				fmt.Println("error logging", err)
			}
		}
	} else {
		dstID, _, dstOk := db.GetRootNode(duckDB, "DST")
		dstRootID := ""
		if dstOk {
			dstRootID = dstID
		}
		if dstRootID == "" {
			if logservice.LS != nil {
				err := logservice.LS.Log("warn", fmt.Sprintf("Failed to ensure root join-lookup mapping: %v", fmt.Errorf("could not find DST root node")), "migration", "copy", "copy")
				if err != nil {
					fmt.Println("error logging", err)
				}
			}
		}
	}

	// Initialize copy queue with both source and destination adapters
	shutdownCtx := cfg.ShutdownContext
	if shutdownCtx == nil {
		shutdownCtx = context.Background()
	}
	copyQueue.InitializeCopyWithContext(duckDB, cfg.SrcAdapter, cfg.DstAdapter, shutdownCtx)

	// Create observer for stats publishing
	observer := queue.NewQueueObserver(duckDB, 200*time.Millisecond)
	observer.Start()
	defer observer.Stop()

	// Register copy queue with observer
	observer.RegisterQueue("copy", copyQueue)
	copyQueue.SetObserver(observer)

	// Set up stats channel for progress updates
	statsChan := make(chan queue.QueueStats, 10)
	copyQueue.SetStatsChannel(statsChan)

	// Start progress ticker
	progressTick := cfg.ProgressTick
	if progressTick <= 0 {
		progressTick = 2 * time.Second
	}
	progressTicker := time.NewTicker(progressTick)
	defer progressTicker.Stop()

	// Start stats consumer goroutine
	go func() {
		var lastStats *queue.QueueStats
		for {
			select {
			case stats := <-statsChan:
				lastStats = &stats
			case <-progressTicker.C:
				if lastStats != nil {
					copyPass := copyQueue.GetCopyPass()
					passName := "folders"
					if copyPass == 2 {
						passName = "files"
					}
					// Get round stats for expected/completed counts (similar to traversal)
					roundStats := copyQueue.GetRoundStats(lastStats.Round)
					expected := 0
					completed := 0
					if roundStats != nil {
						expected = roundStats.Expected
						completed = roundStats.Completed
					}
					fmt.Printf("\r  Copy: Pass %d (%s) Round %d (Expected:%d Completed:%d)   ",
						copyPass, passName, lastStats.Round, expected, completed)
				}
			}
		}
	}()

	// Wait for copy phase completion
	start := time.Now()
	lastRound := minLevel // Initialize to starting round
	tickCount := 0
	for {
		// Check for shutdown
		if shutdownCtx != nil {
			select {
			case <-shutdownCtx.Done():
				copyQueue.Pause()
				return queue.QueueStats{}, fmt.Errorf("copy phase shutdown requested")
			default:
			}
		}

		// Check if queue is completed
		if copyQueue.IsExhausted() {
			stats := copyQueue.Stats()

			// Stop progress ticker
			progressTicker.Stop()

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

			fmt.Printf("\nCopy phase complete! Duration: %v\n", time.Since(start))
			return stats, nil
		}

		// Track round advancement for runtime observability.
		currentRound := copyQueue.GetRound()
		if currentRound != lastRound {
			lastRound = currentRound
		}

		tickCount++

		// Sleep briefly before checking again
		time.Sleep(100 * time.Millisecond)
	}
}
