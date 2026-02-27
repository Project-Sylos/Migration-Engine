// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"errors"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
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

	// Create coordinator for round advancement gates
	coordinator := queue.NewQueueCoordinator()

	// Create queues
	srcQueue := queue.NewQueue("src", cfg.MaxRetries, cfg.WorkerCount, coordinator)
	srcQueue.InitializeWithContext(database, cfg.SrcAdapter, cfg.ShutdownContext)
	// Note: Queues clean themselves up when they complete (Run() exits when state=QueueStateCompleted)
	// We only need to explicitly close for forced shutdowns, which is handled via Pause() + shutdown context

	dstQueue := queue.NewQueue("dst", cfg.MaxRetries, cfg.WorkerCount, coordinator)
	dstQueue.InitializeWithContext(database, cfg.DstAdapter, cfg.ShutdownContext)
	// Note: Queues clean themselves up when they complete (Run() exits when state=QueueStateCompleted)
	// We only need to explicitly close for forced shutdowns, which is handled via Pause() + shutdown context

	// Initialize queues with resume state if available
	if err := initializeQueues(cfg, srcQueue, dstQueue, coordinator); err != nil {
		return RuntimeStats{}, err
	}

	// Give queues a moment to start their Run() goroutines
	time.Sleep(100 * time.Millisecond)

	// Create observer for database stats publishing (200ms update interval)
	observer := queue.NewQueueObserver(database, 200*time.Millisecond)
	observer.Start()      // Start observer loop immediately
	defer observer.Stop() // Ensure observer is stopped on exit

	// Register queues with observer (observer will poll queues directly)
	srcQueue.SetObserver(observer)
	dstQueue.SetObserver(observer)

	// Set up stats channels for UDP logging (after queues are running)
	srcStatsChan := make(chan queue.QueueStats, 10)
	dstStatsChan := make(chan queue.QueueStats, 10)
	srcQueue.SetStatsChannel(srcStatsChan)
	dstQueue.SetStatsChannel(dstStatsChan)

	// Start stats consumer goroutine for progress updates (fmt output, not log service)
	// Accumulate stats from both channels and print them together
	go func() {
		var lastSrcStats *queue.QueueStats
		var lastDstStats *queue.QueueStats

		for {
			select {
			case srcStats := <-srcStatsChan:
				lastSrcStats = &srcStats
				printTraversalProgress(srcQueue, dstQueue, lastSrcStats, lastDstStats)
			case dstStats := <-dstStatsChan:
				lastDstStats = &dstStats
				printTraversalProgress(srcQueue, dstQueue, lastSrcStats, lastDstStats)
			}
		}
	}()

	// Ensure ProgressTick is positive (default to 1 second if not set)
	progressTick := cfg.ProgressTick
	if progressTick <= 0 {
		progressTick = 1 * time.Second
	}
	progressTicker := time.NewTicker(progressTick)
	defer progressTicker.Stop()
	start := time.Now()

	for {
		// Check for force shutdown (context cancellation)
		if cfg.ShutdownContext != nil {
			select {
			case <-cfg.ShutdownContext.Done():
				// Force shutdown: pause queues, checkpoint DB, and save suspended state
				// Use timeout context to prevent hanging on cleanup operations
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer cleanupCancel()

				// Pause queues to stop new task leasing (workers will exit via shutdown context)
				srcQueue.Pause()
				dstQueue.Pause()

				// Give workers a moment to finish current tasks (they check shutdown context in their loop)
				// This is non-blocking - workers will exit when they check context in next iteration
				select {
				case <-time.After(200 * time.Millisecond):
					// Continue with cleanup
				case <-cleanupCtx.Done():
					// Timeout - skip cleanup and exit immediately
					fmt.Printf("⚠️  Cleanup timeout - exiting immediately to prevent hang\n")
					srcStats, dstStats := snapshotTraversalQueueStats(database, coordinator)
					return RuntimeStats{
						Duration: time.Since(start),
						Src:      srcStats,
						Dst:      dstStats,
					}, errors.New("migration suspended by force shutdown (cleanup timeout)")
				}

				// Get stats directly (non-blocking)
				srcStats, dstStats := snapshotTraversalQueueStats(database, coordinator)

				// DuckDB doesn't need checkpointing - data is already persisted

				return RuntimeStats{
					Duration: time.Since(start),
					Src:      srcStats,
					Dst:      dstStats,
				}, errors.New("migration suspended by force shutdown")
			default:
				// Continue normal execution
			}
		}

		// Check exhaustion status from coordinator (source of truth)
		// Queues call MarkSrcCompleted()/MarkDstCompleted() on coordinator when done
		bothCompleted := coordinator.IsCompleted("both")

		if bothCompleted {
			return completeTraversalRun(database, coordinator, progressTicker, start), nil
		}

		select {
		case <-progressTicker.C:
			// Re-check exhaustion from coordinator (queues might have completed during tick)
			if coordinator.IsCompleted("both") {
				return completeTraversalRun(database, coordinator, progressTicker, start), nil
			}

		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func printTraversalProgress(srcQueue, dstQueue *queue.Queue, lastSrcStats, lastDstStats *queue.QueueStats) {
	if lastSrcStats == nil || lastDstStats == nil {
		return
	}
	srcRoundStats := srcQueue.GetRoundStats(lastSrcStats.Round)
	dstRoundStats := dstQueue.GetRoundStats(lastDstStats.Round)
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
	fmt.Printf("\r  Src: Round %d (Expected:%d Completed:%d) | Dst: Round %d (Expected:%d Completed:%d)   ",
		lastSrcStats.Round, srcExpected, srcCompleted,
		lastDstStats.Round, dstExpected, dstCompleted)
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
	fmt.Println("\nMigration complete!")
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

	// Update coordinator state
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
