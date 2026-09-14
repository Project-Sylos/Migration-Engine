// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"
	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/loop"
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
	ReportStopProgress   func(step string, inProgress int)
	ObserverPollInterval time.Duration
	OnQueueObserver      func(*observe.QueueObserver)
	OnAutoscaler         func(*loop.Autoscaler)
	Autoscaler           AutoscalerConfig
	SrcService           Service
	DstService           Service
	PathCheckTarget      string
	WindowsCompat        bool
}

// applyCopyResumeDstExistenceWindow enables the copy queue's one-shot dst ListChildren precheck when
// restarting after real copy progress (Successful = actual copy-phase completes, not DST matches).
// Uses GetCopyStatusCountsFromEvents (src_current): requires both successful and pending SRC copy rows.
// If any folder copy is still pending, anchors pass 1 at startRound; if only file copies are pending,
// anchors pass 2 at the minimum depth that still has pending files (so empty shallow file rounds
// do not consume the window before real work runs).
func applyCopyResumeDstExistenceWindow(q *queue.Queue, duckDB *db.DB, startRound, minFolderPendingLevel, minFilePendingLevel int) {
	counts, err := stats.GetCopyStatusCountsFromEvents(duckDB)
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

// RunCopyRetryPhase runs copy retry: pulls copy_status=pending (user marks convert
// failed→pending). Unmarked failures are left alone until marked.
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
		if err := logservice.InitGlobalLogger(duckDB.LogsDBForWrite(), cfg.LogAddress, cfg.LogLevel); err != nil {
			return queue.QueueStats{}, fmt.Errorf("failed to initialize logger: %w", err)
		}
	}
	copyCtx := scalingContextForCopy(cfg.SrcService, cfg.DstService, 1, queue.QueueModeCopyRetry)
	sizing := queueSizingForScalingContext(copyCtx, cfg.ResumeCopy)
	wc := resolveWorkersForScalingContext(copyCtx, cfg.WorkerCount, cfg.ResumeCopy)
	suspendRetries := 0
	if cfg.ResumeCopy != nil {
		suspendRetries = cfg.ResumeCopy.MaxRetries
	}
	mr := effectiveInt(suspendRetries, cfg.MaxRetries)

	copyQueue := queue.NewQueue("copy", mr, wc, nil, sizing)
	copyQueue.SetMode(queue.QueueModeCopyRetry)
	copyQueue.SetCopyPass(1)
	seedQueueCountersFromDB(duckDB, copyQueue, "copy", db.QueueStatsPhaseCopy)
	seedQueueWorkTotalsFromDB(duckDB, copyQueue, "copy")
	minLevel := -1
	levels, err := pull.GetAllLevels(duckDB, "SRC")
	if err == nil && len(levels) > 0 {
		for _, level := range levels {
			if level == 0 {
				continue
			}
			c, err := stats.GetCopyCountAtDepth(duckDB, level, db.NodeTypeFolder, db.CopyStatusPending, true)
			if err == nil && c > 0 {
				if minLevel == -1 || level < minLevel {
					minLevel = level
				}
			}
			cf, err := stats.GetCopyCountAtDepth(duckDB, level, db.NodeTypeFile, db.CopyStatusPending, true)
			if err == nil && cf > 0 {
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
	copyQueue.SetExpectedFromStatsBucket(copyQueue.GetRound())
	if maxDepth, err := stats.GetMaxDepth(duckDB, "SRC"); err == nil {
		copyQueue.SetMaxKnownDepth(maxDepth)
	}
	shutdownCtx := cfg.ShutdownContext
	if shutdownCtx == nil {
		shutdownCtx = context.Background()
	}
	if err := duckDB.BeginTraversalPhase(shutdownCtx); err != nil {
		return queue.QueueStats{}, fmt.Errorf("begin copy phase: %w", err)
	}
	bulkPhaseClosed := false
	skipDurableTeardown := false
	defer func() {
		if bulkPhaseClosed || skipDurableTeardown {
			return
		}
		if err := duckDB.CheckpointWithRetry(context.Background(), 8); err != nil {
			fmt.Println("checkpoint after copy retry phase:", err)
		}
	}()
	defer func() {
		if bulkPhaseClosed || skipDurableTeardown {
			return
		}
		if err := duckDB.EndTraversalPhase(); err != nil {
			fmt.Println("error ending copy phase", err)
		}
	}()
	copyQueue.InitializeCopyWithContext(duckDB, cfg.SrcAdapter, cfg.DstAdapter, shutdownCtx)
	copyQueue.SetRateLimitTelemetry(
		rateLimitBridgeForAdapter(cfg.SrcAdapter),
		rateLimitBridgeForAdapter(cfg.DstAdapter),
	)
	obsPoll := observerPollFromConfigAndSuspend(cfg.ObserverPollInterval, nil)
	observer := observe.NewQueueObserver(duckDB, obsPoll)
	observer.Start()
	defer observer.Stop()
	if cfg.OnQueueObserver != nil {
		defer cfg.OnQueueObserver(nil)
	}
	observer.RegisterQueue("copy", copyQueue)
	copyQueue.SetObserver(observer)
	if cfg.OnQueueObserver != nil {
		cfg.OnQueueObserver(observer)
	}

	runCtx := shutdownCtx
	if runCtx == nil {
		runCtx = context.Background()
	}
	asCtx := startCopyAutoscaler(runCtx, cfg.Autoscaler, observer, duckDB, copyQueue, cfg.SrcService, cfg.DstService, cfg.PathCheckTarget, cfg.WindowsCompat)
	if cfg.OnAutoscaler != nil {
		cfg.OnAutoscaler(asCtx.Autoscaler())
		defer cfg.OnAutoscaler(nil)
	}
	defer asCtx.stop()

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
					fmt.Printf("\r  Copy retry: Pass %d (%s) Round %d | Exp:%d Comp:%d | Pend:%d InProg:%d | Partial:%v Workers:%d%s   ",
						copyPass, passName, lastStats.Round, expected, completed, pending, inProgress, lastPartial, workers,
						FormatCatalogSyncSuffix(duckDB))
				}
			}
		}
	}()
	copyQueue.PullTasksIfNeeded(true)
	start := time.Now()
	for {
		if shutdownCtx != nil {
			select {
			case <-shutdownCtx.Done():
				skipDurableTeardown = true
				performCopyForceStop(copyQueue, observer)
				duckDB.AbortTraversalPhase()
				return copyForceStopStats(copyQueue), fmt.Errorf("migration force stopped during %s", "copy retry")
			default:
			}
		}

		if cfg.SoftSuspendRequested != nil && cfg.SoftSuspendRequested() {
			waitCtx, cancel := softSuspendWaitContext(cfg.ShutdownContext)
			stats, suspend, err := performCopySoftSuspend(waitCtx, duckDB, copyQueue, observer, cfg, wc, mr)
			cancel()
			if forceStopOverridesSoftSuspend(cfg.ShutdownContext, duckDB) {
				skipDurableTeardown = true
				performCopyForceStop(copyQueue, observer)
				duckDB.AbortTraversalPhase()
				return copyForceStopStats(copyQueue), fmt.Errorf("migration force stopped during %s", "copy retry")
			}
			if err != nil {
				return stats, fmt.Errorf("copy retry soft suspend: %w", err)
			}
			setDetail := func(d string) {
				if observer != nil {
					observer.SetWaitReason(d)
				}
			}
			bulkPhaseClosed = true
			finishCtx := cfg.ShutdownContext
			if finishCtx == nil {
				finishCtx = context.Background()
			}
			if err := finishSoftStopBulkPhase(finishCtx, duckDB, cfg.ReportStopProgress, setDetail); err != nil {
				if forceStopOverridesSoftSuspend(cfg.ShutdownContext, duckDB) {
					skipDurableTeardown = true
					performCopyForceStop(copyQueue, observer)
					duckDB.AbortTraversalPhase()
					return copyForceStopStats(copyQueue), fmt.Errorf("migration force stopped during %s", "copy retry")
				}
				return stats, fmt.Errorf("copy retry soft suspend teardown: %w", err)
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
			bulkPhaseClosed = true
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
		if err := logservice.InitGlobalLogger(duckDB.LogsDBForWrite(), cfg.LogAddress, cfg.LogLevel); err != nil {
			return queue.QueueStats{}, fmt.Errorf("failed to initialize logger: %w", err)
		}
	}

	copyCtx := scalingContextForCopy(cfg.SrcService, cfg.DstService, 1, queue.QueueModeCopy)
	sizing := queueSizingForScalingContext(copyCtx, cfg.ResumeCopy)
	wc := resolveWorkersForScalingContext(copyCtx, cfg.WorkerCount, cfg.ResumeCopy)
	suspendRetries := 0
	if cfg.ResumeCopy != nil {
		suspendRetries = cfg.ResumeCopy.MaxRetries
	}
	mr := effectiveInt(suspendRetries, cfg.MaxRetries)

	// Create copy queue (single queue, not dual like traversal)
	copyQueue := queue.NewQueue("copy", mr, wc, nil, sizing) // No coordinator needed for copy
	copyQueue.SetMode(queue.QueueModeCopy)
	copyQueue.SetCopyPass(1) // Start with pass 1 (folders)
	if cfg.ResumeCopy != nil {
		seedQueueCountersFromDB(duckDB, copyQueue, "copy", db.QueueStatsPhaseCopy)
	}
	seedQueueWorkTotalsFromDB(duckDB, copyQueue, "copy")

	// Minimum depth with pending folder / file copy (skip round 0). Used for start round and resume dst precheck anchor.
	minFolderPendingLevel := -1
	minFilePendingLevel := -1
	levels, err := pull.GetAllLevels(duckDB, "SRC")
	if err == nil && len(levels) > 0 {
		for _, level := range levels {
			if level == 0 {
				continue // Skip round 0
			}
			cf, err1 := stats.GetCopyCountAtDepth(duckDB, level, db.NodeTypeFolder, db.CopyStatusPending, true)
			if err1 == nil && cf > 0 {
				if minFolderPendingLevel == -1 || level < minFolderPendingLevel {
					minFolderPendingLevel = level
				}
			}
			cn, err2 := stats.GetCopyCountAtDepth(duckDB, level, db.NodeTypeFile, db.CopyStatusPending, true)
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
	if cfg.ResumeCopy != nil && cfg.ResumeCopy.LastKnownCopyRound > 0 {
		startRound = cfg.ResumeCopy.LastKnownCopyRound
	}
	if cfg.ResumeCopy != nil && cfg.ResumeCopy.CopyPass > 0 {
		copyQueue.SetCopyPass(cfg.ResumeCopy.CopyPass)
	}
	copyQueue.SetRound(startRound)
	if cfg.ResumeCopy != nil && cfg.ResumeCopy.CopyKeysetCursor != "" {
		copyQueue.SetKeysetCursor(cfg.ResumeCopy.CopyKeysetCursor)
	}
	applyCopyResumeDstExistenceWindow(copyQueue, duckDB, startRound, minFolderPendingLevel, minFilePendingLevel)
	copyQueue.SetExpectedFromStatsBucket(copyQueue.GetRound())
	logCopyResumePositionCheck(cfg.ResumeCopy, copyQueue, startRound)

	// Set max known depth from DB so copy completion and round advancement know the full depth range.
	// Must be set before any tasks are pulled or completion checks run.
	if cfg.ResumeCopy != nil && cfg.ResumeCopy.MaxKnownDepth > 0 {
		copyQueue.SetMaxKnownDepth(cfg.ResumeCopy.MaxKnownDepth)
	} else if maxDepth, err := stats.GetMaxDepth(duckDB, "SRC"); err == nil {
		copyQueue.SetMaxKnownDepth(maxDepth)
	}

	// CRITICAL: Ensure root folder (level 0) has join-lookup mapping (DuckDB: join_id on DST node).
	srcID, _, srcOk := pull.GetRootNode(duckDB, "SRC")
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
		dstID, _, dstOk := pull.GetRootNode(duckDB, "DST")
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

	// Begin copy phase before starting workers so pulls see copy-phase DB state.
	shutdownCtx := cfg.ShutdownContext
	if shutdownCtx == nil {
		shutdownCtx = context.Background()
	}
	if err := duckDB.BeginTraversalPhase(shutdownCtx); err != nil {
		return queue.QueueStats{}, fmt.Errorf("begin copy phase: %w", err)
	}
	bulkPhaseClosed := false
	skipDurableTeardown := false
	defer func() {
		if bulkPhaseClosed || skipDurableTeardown {
			return
		}
		if err := duckDB.CheckpointWithRetry(context.Background(), 8); err != nil {
			fmt.Println("checkpoint after copy phase:", err)
		}
	}()
	defer func() {
		if bulkPhaseClosed || skipDurableTeardown {
			return
		}
		if err := duckDB.EndTraversalPhase(); err != nil {
			fmt.Println("error ending copy phase", err)
		}
	}()

	copyQueue.InitializeCopyWithContext(duckDB, cfg.SrcAdapter, cfg.DstAdapter, shutdownCtx)
	copyQueue.SetRateLimitTelemetry(
		rateLimitBridgeForAdapter(cfg.SrcAdapter),
		rateLimitBridgeForAdapter(cfg.DstAdapter),
	)

	obsPoll := observerPollFromConfigAndSuspend(cfg.ObserverPollInterval, cfg.ResumeCopy)
	observer := observe.NewQueueObserver(duckDB, obsPoll)
	observer.Start()
	defer observer.Stop()
	if cfg.OnQueueObserver != nil {
		defer cfg.OnQueueObserver(nil)
	}

	observer.RegisterQueue("copy", copyQueue)
	copyQueue.SetObserver(observer)
	if cfg.OnQueueObserver != nil {
		cfg.OnQueueObserver(observer)
	}

	runCtx := shutdownCtx
	if runCtx == nil {
		runCtx = context.Background()
	}
	asCtx := startCopyAutoscaler(runCtx, cfg.Autoscaler, observer, duckDB, copyQueue, cfg.SrcService, cfg.DstService, cfg.PathCheckTarget, cfg.WindowsCompat)
	if cfg.OnAutoscaler != nil {
		cfg.OnAutoscaler(asCtx.Autoscaler())
		defer cfg.OnAutoscaler(nil)
	}
	defer asCtx.stop()

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
					fmt.Printf("\r  Copy: Pass %d (%s) Round %d | Exp:%d Comp:%d | Pend:%d InProg:%d | Partial:%v Workers:%d%s   ",
						copyPass, passName, lastStats.Round, expected, completed, pending, inProgress, lastPartial, workers,
						FormatCatalogSyncSuffix(duckDB))
				}
			}
		}
	}()

	copyQueue.PullTasksIfNeeded(true)
	start := time.Now()
	lastRound := startRound // Initialize to starting round
	tickCount := 0
	for {
		if shutdownCtx != nil {
			select {
			case <-shutdownCtx.Done():
				skipDurableTeardown = true
				performCopyForceStop(copyQueue, observer)
				duckDB.AbortTraversalPhase()
				return copyForceStopStats(copyQueue), fmt.Errorf("migration force stopped during %s", "copy")
			default:
			}
		}

		if cfg.SoftSuspendRequested != nil && cfg.SoftSuspendRequested() {
			waitCtx, cancel := softSuspendWaitContext(cfg.ShutdownContext)
			stats, suspend, err := performCopySoftSuspend(waitCtx, duckDB, copyQueue, observer, cfg, wc, mr)
			cancel()
			if forceStopOverridesSoftSuspend(cfg.ShutdownContext, duckDB) {
				skipDurableTeardown = true
				performCopyForceStop(copyQueue, observer)
				duckDB.AbortTraversalPhase()
				return copyForceStopStats(copyQueue), fmt.Errorf("migration force stopped during %s", "copy")
			}
			if err != nil {
				return stats, fmt.Errorf("copy soft suspend: %w", err)
			}
			setDetail := func(d string) {
				if observer != nil {
					observer.SetWaitReason(d)
				}
			}
			bulkPhaseClosed = true
			finishCtx := cfg.ShutdownContext
			if finishCtx == nil {
				finishCtx = context.Background()
			}
			if err := finishSoftStopBulkPhase(finishCtx, duckDB, cfg.ReportStopProgress, setDetail); err != nil {
				if forceStopOverridesSoftSuspend(cfg.ShutdownContext, duckDB) {
					skipDurableTeardown = true
					performCopyForceStop(copyQueue, observer)
					duckDB.AbortTraversalPhase()
					return copyForceStopStats(copyQueue), fmt.Errorf("migration force stopped during %s", "copy")
				}
				return stats, fmt.Errorf("copy soft suspend teardown: %w", err)
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
			bulkPhaseClosed = true
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
