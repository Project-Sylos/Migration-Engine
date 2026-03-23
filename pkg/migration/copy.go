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
	DuckDB               *db.DB
	SrcAdapter           types.FSAdapter
	DstAdapter           types.FSAdapter
	WorkerCount          int
	MaxRetries           int
	LogAddress           string
	LogLevel             string
	SkipListener         bool
	StartupDelay         time.Duration
	ProgressTick         time.Duration
	ShutdownContext      context.Context
	ResumeCopy           *RuntimeSuspendV1
	SoftSuspendRequested func() bool
	ObserverPollInterval time.Duration
	OnQueueObserver      func(*queue.QueueObserver)
}

// applyCopyResumeDstExistenceWindow enables the copy queue's one-shot dst ListChildren precheck when
// restarting after partial copy progress (see queue.SetCopyResumeDstExistenceWindow).
// Uses GetCopyStatusCountsFromEvents: requires both successful and pending SRC copy rows.
// If any folder copy is still pending, anchors pass 1 at startRound; if only file copies are pending,
// anchors pass 2 at the minimum depth that still has pending files (so empty shallow file rounds
// do not consume the window before real work runs).
func applyCopyResumeDstExistenceWindow(q *queue.Queue, duckDB *db.DB, startRound, minFolderPendingLevel, minFilePendingLevel int) {
	counts, err := duckDB.GetCopyStatusCountsFromEvents()
	if err != nil || counts.Successful <= 0 || counts.Pending <= 0 {
		return
	}
	if minFolderPendingLevel != -1 {
		q.SetCopyResumeDstExistenceWindow(1, startRound)
		return
	}
	if minFilePendingLevel != -1 {
		q.SetCopyResumeDstExistenceWindow(2, minFilePendingLevel)
	}
}

// RunCopyRetryPhase runs the copy phase in retry mode: only copy_status = failed items are pulled.
// Uses the same two-pass BFS and max-depth guarded completion as traversal retry.
func RunCopyRetryPhase(cfg CopyPhaseConfig) (queue.QueueStats, error) {
	duckDB := cfg.DuckDB
	if duckDB == nil {
		return queue.QueueStats{}, fmt.Errorf("DuckDB must be provided")
	}
	if cfg.SrcAdapter == nil || cfg.DstAdapter == nil {
		return queue.QueueStats{}, fmt.Errorf("source and destination adapters must be provided")
	}
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
			} else {
				time.Sleep(startupDelay)
			}
		}
		if err := logservice.InitGlobalLogger(duckDB, cfg.LogAddress, cfg.LogLevel); err != nil {
			return queue.QueueStats{}, fmt.Errorf("failed to initialize logger: %w", err)
		}
	}
	copyQueue := queue.NewQueue("copy", cfg.MaxRetries, cfg.WorkerCount, nil, nil)
	copyQueue.SetMode(queue.QueueModeCopyRetry)
	copyQueue.SetCopyPass(1)
	minLevel := -1
	levels, err := db.GetAllLevels(duckDB, "SRC")
	if err == nil && len(levels) > 0 {
		for _, level := range levels {
			if level == 0 {
				continue
			}
			c, err := duckDB.GetCopyCountAtDepth(level, db.NodeTypeFolder, db.CopyStatusFailed, true)
			if err == nil && c > 0 {
				if minLevel == -1 || level < minLevel {
					minLevel = level
				}
			}
		}
	}
	if minLevel == -1 {
		return queue.QueueStats{}, nil
	}
	copyQueue.SetRound(minLevel)
	copyQueue.EnsureRoundExpectedFromStats()
	if maxDepth, err := duckDB.GetMaxDepth("SRC"); err == nil {
		copyQueue.SetMaxKnownDepth(maxDepth)
	}
	shutdownCtx := cfg.ShutdownContext
	if shutdownCtx == nil {
		shutdownCtx = context.Background()
	}
	copyQueue.InitializeCopyWithContext(duckDB, cfg.SrcAdapter, cfg.DstAdapter, shutdownCtx)
	if err := duckDB.BeginCopyPhase(shutdownCtx); err != nil {
		return queue.QueueStats{}, fmt.Errorf("begin copy phase: %w", err)
	}
	defer func() {
		if err := duckDB.CheckpointWithRetry(context.Background(), 8); err != nil {
			fmt.Println("checkpoint after copy retry phase:", err)
		}
	}()
	defer func() {
		if err := duckDB.EndCopyPhase(); err != nil {
			fmt.Println("error ending copy phase", err)
		}
	}()
	obsPoll := observerPollFromConfigAndSuspend(cfg.ObserverPollInterval, nil)
	observer := queue.NewQueueObserver(duckDB, obsPoll)
	observer.Start()
	defer observer.Stop()
	if cfg.OnQueueObserver != nil {
		defer cfg.OnQueueObserver(nil)
	}
	copyQueue.SetObserver(observer)
	if cfg.OnQueueObserver != nil {
		cfg.OnQueueObserver(observer)
	}
	statsChan := make(chan queue.QueueStats, 10)
	copyQueue.SetStatsChannel(statsChan)
	progressTick := progressTickFromConfigAndSuspend(cfg.ProgressTick, nil, 2*time.Second)
	if progressTick <= 0 {
		progressTick = 2 * time.Second
	}
	progressTicker := time.NewTicker(progressTick)
	defer progressTicker.Stop()
	progressCtx, progressCancel := context.WithCancel(context.Background())
	defer progressCancel()
	go func() {
		var lastStats *queue.QueueStats
		for {
			select {
			case <-progressCtx.Done():
				return
			case stats := <-statsChan:
				lastStats = &stats
			case <-progressTicker.C:
				if lastStats != nil {
					copyPass := copyQueue.GetCopyPass()
					passName := "folders"
					if copyPass == 2 {
						passName = "files"
					}
					roundStats := copyQueue.GetRoundStats(lastStats.Round)
					expected, completed := 0, 0
					if roundStats != nil {
						expected = roundStats.Expected
						completed = roundStats.Completed
					}
					pending := copyQueue.GetPendingCount()
					inProgress := copyQueue.InProgressCount()
					lastPartial := copyQueue.GetLastPullWasPartial()
					workers := copyQueue.GetWorkerCount()
					fmt.Printf("\r  Copy retry: Pass %d (%s) Round %d | Exp:%d Comp:%d | Pend:%d InProg:%d | Partial:%v Workers:%d   ",
						copyPass, passName, lastStats.Round, expected, completed, pending, inProgress, lastPartial, workers)
				}
			}
		}
	}()
	copyQueue.PullTasksIfNeeded(true)
	start := time.Now()
	wc, mr := cfg.WorkerCount, cfg.MaxRetries
	for {
		if shutdownCtx != nil {
			select {
			case <-shutdownCtx.Done():
				copyQueue.Pause()
				return queue.QueueStats{}, fmt.Errorf("copy retry shutdown requested")
			default:
			}
		}

		if cfg.SoftSuspendRequested != nil && cfg.SoftSuspendRequested() {
			waitCtx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			stats, suspend, err := performCopySoftSuspend(waitCtx, duckDB, copyQueue, observer, cfg, wc, mr)
			cancel()
			if err != nil {
				return stats, fmt.Errorf("copy retry soft suspend: %w", err)
			}
			fmt.Print("\n")
			return stats, newCopySuspendedError(stats, suspend)
		}
		if copyQueue.IsExhausted() {
			stats := copyQueue.Stats()
			progressTicker.Stop()
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
			fmt.Printf("\nCopy retry complete! Duration: %v\n", time.Since(start))
			return stats, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
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

	sizing := queueSizingFromSuspend(cfg.ResumeCopy)
	wc := effectiveWorkerCount(cfg.WorkerCount, cfg.ResumeCopy)
	mr := effectiveMaxRetries(cfg.MaxRetries, cfg.ResumeCopy)

	// Create copy queue (single queue, not dual like traversal)
	copyQueue := queue.NewQueue("copy", mr, wc, nil, sizing) // No coordinator needed for copy
	copyQueue.SetMode(queue.QueueModeCopy)
	copyQueue.SetCopyPass(1) // Start with pass 1 (folders)

	// Minimum depth with pending folder / file copy (skip round 0). Used for start round and resume dst precheck anchor.
	minFolderPendingLevel := -1
	minFilePendingLevel := -1
	levels, err := db.GetAllLevels(duckDB, "SRC")
	if err == nil && len(levels) > 0 {
		for _, level := range levels {
			if level == 0 {
				continue // Skip round 0
			}
			cf, err1 := duckDB.GetCopyCountAtDepth(level, db.NodeTypeFolder, db.CopyStatusPending, true)
			if err1 == nil && cf > 0 {
				if minFolderPendingLevel == -1 || level < minFolderPendingLevel {
					minFolderPendingLevel = level
				}
			}
			cn, err2 := duckDB.GetCopyCountAtDepth(level, db.NodeTypeFile, db.CopyStatusPending, true)
			if err2 == nil && cn > 0 {
				if minFilePendingLevel == -1 || level < minFilePendingLevel {
					minFilePendingLevel = level
				}
			}
		}
	}

	startRound := 1
	if minFolderPendingLevel != -1 {
		startRound = minFolderPendingLevel
	}
	copyQueue.SetRound(startRound)
	applyCopyResumeDstExistenceWindow(copyQueue, duckDB, startRound, minFolderPendingLevel, minFilePendingLevel)
	copyQueue.EnsureRoundExpectedFromStats()

	// Set max known depth from DB so copy completion and round advancement know the full depth range.
	// Must be set before any tasks are pulled or completion checks run.
	if cfg.ResumeCopy != nil && cfg.ResumeCopy.MaxKnownDepth > 0 {
		copyQueue.SetMaxKnownDepth(cfg.ResumeCopy.MaxKnownDepth)
	} else if maxDepth, err := duckDB.GetMaxDepth("SRC"); err == nil {
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
			err := logservice.LS.Log("warning", fmt.Sprintf("Failed to ensure root join-lookup mapping: %v", fmt.Errorf("could not find SRC root node")), "migration", "copy", "copy")
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
				err := logservice.LS.Log("warning", fmt.Sprintf("Failed to ensure root join-lookup mapping: %v", fmt.Errorf("could not find DST root node")), "migration", "copy", "copy")
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

	// Start copy phase: drop indexes, persistent appenders. CHECKPOINT once after phase teardown (defer order below).
	if err := duckDB.BeginCopyPhase(shutdownCtx); err != nil {
		return queue.QueueStats{}, fmt.Errorf("begin copy phase: %w", err)
	}
	defer func() {
		if err := duckDB.CheckpointWithRetry(context.Background(), 8); err != nil {
			fmt.Println("checkpoint after copy phase:", err)
		}
	}()
	defer func() {
		if err := duckDB.EndCopyPhase(); err != nil {
			fmt.Println("error ending copy phase", err)
		}
	}()

	obsPoll := observerPollFromConfigAndSuspend(cfg.ObserverPollInterval, cfg.ResumeCopy)
	observer := queue.NewQueueObserver(duckDB, obsPoll)
	observer.Start()
	defer observer.Stop()
	if cfg.OnQueueObserver != nil {
		defer cfg.OnQueueObserver(nil)
	}

	// Register copy queue with observer
	observer.RegisterQueue("copy", copyQueue)
	copyQueue.SetObserver(observer)
	if cfg.OnQueueObserver != nil {
		cfg.OnQueueObserver(observer)
	}

	// Set up stats channel for progress updates
	statsChan := make(chan queue.QueueStats, 10)
	copyQueue.SetStatsChannel(statsChan)

	progressTick := progressTickFromConfigAndSuspend(cfg.ProgressTick, cfg.ResumeCopy, 2*time.Second)
	if progressTick <= 0 {
		progressTick = 2 * time.Second
	}
	progressTicker := time.NewTicker(progressTick)
	defer progressTicker.Stop()

	progressCtx, progressCancel := context.WithCancel(context.Background())
	defer progressCancel()

	// Start stats consumer goroutine
	go func() {
		var lastStats *queue.QueueStats
		for {
			select {
			case <-progressCtx.Done():
				return
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
					pending := copyQueue.GetPendingCount()
					inProgress := copyQueue.InProgressCount()
					lastPartial := copyQueue.GetLastPullWasPartial()
					workers := copyQueue.GetWorkerCount()
					fmt.Printf("\r  Copy: Pass %d (%s) Round %d | Exp:%d Comp:%d | Pend:%d InProg:%d | Partial:%v Workers:%d   ",
						copyPass, passName, lastStats.Round, expected, completed, pending, inProgress, lastPartial, workers)
				}
			}
		}
	}()

	// Wait for copy phase completion
	start := time.Now()
	lastRound := startRound // Initialize to starting round
	tickCount := 0
	for {
		if shutdownCtx != nil {
			select {
			case <-shutdownCtx.Done():
				copyQueue.Pause()
				return queue.QueueStats{}, fmt.Errorf("copy phase shutdown requested")
			default:
			}
		}

		if cfg.SoftSuspendRequested != nil && cfg.SoftSuspendRequested() {
			waitCtx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			stats, suspend, err := performCopySoftSuspend(waitCtx, duckDB, copyQueue, observer, cfg, wc, mr)
			cancel()
			if err != nil {
				return stats, fmt.Errorf("copy soft suspend: %w", err)
			}
			fmt.Print("\n")
			return stats, newCopySuspendedError(stats, suspend)
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
