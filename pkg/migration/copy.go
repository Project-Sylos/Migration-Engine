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
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// CopyPhaseConfig configures the copy phase execution.
type CopyPhaseConfig struct {
	PhaseRuntime
	DB             *db.DB
	SrcAdapter     types.FSAdapter
	DstAdapter     types.FSAdapter
	ResumeCopy     *RuntimeSuspendV1
	UncleanResume  bool   // crash/force-stop recovery: existence window + HashVerify
	OnRecoveryDone func() // cleared unclean_interrupt when recovery round leaves
}

// applyCopyResumeDstExistenceWindow enables the copy queue's one-shot dst ListChildren precheck when
// restarting after real copy progress (Successful = actual copy-phase completes, not DST matches),
// or on unclean resume whenever Pending > 0 (even if Successful == 0).
// If any folder copy is still pending, anchors pass 1 at startRound; if only file copies are pending,
// anchors pass 2 at the minimum depth that still has pending files (so empty shallow file rounds
// do not consume the window before real work runs).
func applyCopyResumeDstExistenceWindow(q *queue.Queue, database *db.DB, startRound, minFolderPendingLevel, minFilePendingLevel int, unclean bool, onDone func()) {
	counts, err := stats.GetCopyStatusCountsFromEvents(database)
	if err != nil || counts.Pending <= 0 {
		return
	}
	if !unclean && counts.Successful <= 0 {
		return
	}
	if unclean {
		q.SetCopyResumeRecoveryHashVerify(true)
		if onDone != nil {
			q.SetOnLeaveCopyResumeWindow(onDone)
		}
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
	database := cfg.DB
	if err := cfg.Begin(database, cfg.SrcAdapter, cfg.DstAdapter); err != nil {
		return queue.QueueStats{}, err
	}
	copyCtx := newScalingContext("copy", queue.ScalingModeCopy, queue.QueueModeCopyRetry, 1, cfg.SrcService, cfg.DstService)
	sizing := queueSizingForScalingContext(copyCtx, cfg.ResumeCopy)
	wc := resolveWorkersForScalingContext(copyCtx, cfg.WorkerCount, cfg.ResumeCopy)
	suspendRetries := 0
	if cfg.ResumeCopy != nil {
		suspendRetries = cfg.ResumeCopy.MaxRetries
	}
	mr := effectiveInt(suspendRetries, cfg.MaxRetries)

	copyQueue := queue.NewQueue("copy", mr, wc, nil, sizing)
	applyFSStallTimeouts(copyQueue, cfg.PhaseRuntime)
	copyQueue.SetMode(queue.QueueModeCopyRetry)
	copyQueue.SetCopyPass(1)
	seedQueueCountersFromDB(database, copyQueue, "copy", db.QueueStatsPhaseCopy)
	seedQueueWorkTotalsFromDB(database, copyQueue, "copy")
	minLevel := -1
	levels, err := pull.GetAllLevels(database, "SRC")
	if err == nil && len(levels) > 0 {
		for _, level := range levels {
			if level == 0 {
				continue
			}
			c, err := stats.GetCopyCountAtDepth(database, level, db.NodeTypeFolder, db.CopyStatusPending, true)
			if err == nil && c > 0 {
				if minLevel == -1 || level < minLevel {
					minLevel = level
				}
			}
			cf, err := stats.GetCopyCountAtDepth(database, level, db.NodeTypeFile, db.CopyStatusPending, true)
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
	if maxDepth, err := stats.GetMaxDepth(database, "SRC"); err == nil {
		copyQueue.SetMaxKnownDepth(maxDepth)
	}
	shutdownCtx := cfg.ShutdownOrBackground()
	if err := database.BeginTraversalPhase(shutdownCtx); err != nil {
		return queue.QueueStats{}, fmt.Errorf("begin copy phase: %w", err)
	}
	bulkPhaseClosed := false
	skipDurableTeardown := false
	defer func() {
		if bulkPhaseClosed || skipDurableTeardown {
			return
		}
		if err := database.CheckpointWithRetry(context.Background(), 8); err != nil {
			fmt.Println("checkpoint after copy retry phase:", err)
		}
	}()
	defer func() {
		if bulkPhaseClosed || skipDurableTeardown {
			return
		}
		if err := database.EndTraversalPhase(); err != nil {
			fmt.Println("error ending copy phase", err)
		}
	}()
	copyQueue.Initialize(queue.PoolCopy, database, cfg.SrcAdapter, cfg.DstAdapter, shutdownCtx)
	copyQueue.SetRateLimitTelemetry(
		rateLimitBridgeForAdapter(cfg.SrcAdapter),
		rateLimitBridgeForAdapter(cfg.DstAdapter),
	)
	obsPoll := observerPollFromConfigAndSuspend(cfg.ObserverPollInterval, nil)
	observer := observe.NewQueueObserver(database, obsPoll)
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

	asCtx := cfg.startAutoscaler(observer, database, copyQueue)
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
					fmt.Printf("\r  Copy retry: Pass %d (%s) Round %d | Exp:%d Comp:%d | Pend:%d InProg:%d | Partial:%v Workers:%d   ",
						copyPass, passName, lastStats.Round, expected, completed, pending, inProgress, lastPartial, workers)
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
				database.AbortTraversalPhase()
				return copyForceStopStats(copyQueue), fmt.Errorf("migration force stopped during %s", "copy retry")
			default:
			}
		}

		if cfg.SoftSuspendRequested != nil && cfg.SoftSuspendRequested() {
			waitCtx, cancel := softSuspendWaitContext(cfg.ShutdownContext)
			stats, suspend, err := performQueueSoftSuspend(waitCtx, database, copyQueue, observer, &cfg.PhaseRuntime, "copies", "copy", wc, mr, drainCopyQueue(copyQueue, cfg.ShutdownContext))
			cancel()
			if forceStopOverridesSoftSuspend(cfg.ShutdownContext, database) {
				skipDurableTeardown = true
				performCopyForceStop(copyQueue, observer)
				database.AbortTraversalPhase()
				return copyForceStopStats(copyQueue), fmt.Errorf("migration force stopped during %s", "copy retry")
			}
			if err != nil {
				return stats, fmt.Errorf("copy retry soft suspend: %w", err)
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
					performCopyForceStop(copyQueue, observer)
					database.AbortTraversalPhase()
					return copyForceStopStats(copyQueue), fmt.Errorf("migration force stopped during %s", "copy retry")
				}
				return stats, fmt.Errorf("copy retry soft suspend teardown: %w", err)
			}
			fmt.Print("\n")
			return stats, newSuspendedError(ErrCopySoftSuspended, stats, suspend)
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
	database := cfg.DB
	if err := cfg.Begin(database, cfg.SrcAdapter, cfg.DstAdapter); err != nil {
		return queue.QueueStats{}, err
	}

	copyCtx := newScalingContext("copy", queue.ScalingModeCopy, queue.QueueModeCopy, 1, cfg.SrcService, cfg.DstService)
	sizing := queueSizingForScalingContext(copyCtx, cfg.ResumeCopy)
	wc := resolveWorkersForScalingContext(copyCtx, cfg.WorkerCount, cfg.ResumeCopy)
	suspendRetries := 0
	if cfg.ResumeCopy != nil {
		suspendRetries = cfg.ResumeCopy.MaxRetries
	}
	mr := effectiveInt(suspendRetries, cfg.MaxRetries)

	// Create copy queue (single queue, not dual like traversal)
	copyQueue := queue.NewQueue("copy", mr, wc, nil, sizing) // No coordinator needed for copy
	applyFSStallTimeouts(copyQueue, cfg.PhaseRuntime)
	copyQueue.SetMode(queue.QueueModeCopy)
	copyQueue.SetCopyPass(1) // Start with pass 1 (folders)
	if cfg.ResumeCopy != nil {
		seedQueueCountersFromDB(database, copyQueue, "copy", db.QueueStatsPhaseCopy)
	}
	seedQueueWorkTotalsFromDB(database, copyQueue, "copy")

	// Minimum depth with pending folder / file copy (skip round 0). Used for start round and resume dst precheck anchor.
	minFolderPendingLevel := -1
	minFilePendingLevel := -1
	levels, err := pull.GetAllLevels(database, "SRC")
	if err == nil && len(levels) > 0 {
		for _, level := range levels {
			if level == 0 {
				continue // Skip round 0
			}
			cf, err1 := stats.GetCopyCountAtDepth(database, level, db.NodeTypeFolder, db.CopyStatusPending, true)
			if err1 == nil && cf > 0 {
				if minFolderPendingLevel == -1 || level < minFolderPendingLevel {
					minFolderPendingLevel = level
				}
			}
			cn, err2 := stats.GetCopyCountAtDepth(database, level, db.NodeTypeFile, db.CopyStatusPending, true)
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
	applyCopyResumeDstExistenceWindow(copyQueue, database, startRound, minFolderPendingLevel, minFilePendingLevel, cfg.UncleanResume, cfg.OnRecoveryDone)
	copyQueue.SetExpectedFromStatsBucket(copyQueue.GetRound())
	logCopyResumePositionCheck(cfg.ResumeCopy, copyQueue, startRound)

	// Set max known depth from DB so copy completion and round advancement know the full depth range.
	// Must be set before any tasks are pulled or completion checks run.
	if cfg.ResumeCopy != nil && cfg.ResumeCopy.MaxKnownDepth > 0 {
		copyQueue.SetMaxKnownDepth(cfg.ResumeCopy.MaxKnownDepth)
	} else if maxDepth, err := stats.GetMaxDepth(database, "SRC"); err == nil {
		copyQueue.SetMaxKnownDepth(maxDepth)
	}

	// CRITICAL: Ensure root folder (level 0) has join-lookup mapping (DB: join_id on DST node).
	srcID, _, srcOk := pull.GetRootNode(database, "SRC")
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
		dstID, _, dstOk := pull.GetRootNode(database, "DST")
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
	shutdownCtx := cfg.ShutdownOrBackground()
	if err := database.BeginTraversalPhase(shutdownCtx); err != nil {
		return queue.QueueStats{}, fmt.Errorf("begin copy phase: %w", err)
	}
	bulkPhaseClosed := false
	skipDurableTeardown := false
	defer func() {
		if bulkPhaseClosed || skipDurableTeardown {
			return
		}
		if err := database.CheckpointWithRetry(context.Background(), 8); err != nil {
			fmt.Println("checkpoint after copy phase:", err)
		}
	}()
	defer func() {
		if bulkPhaseClosed || skipDurableTeardown {
			return
		}
		if err := database.EndTraversalPhase(); err != nil {
			fmt.Println("error ending copy phase", err)
		}
	}()

	copyQueue.Initialize(queue.PoolCopy, database, cfg.SrcAdapter, cfg.DstAdapter, shutdownCtx)
	copyQueue.SetRateLimitTelemetry(
		rateLimitBridgeForAdapter(cfg.SrcAdapter),
		rateLimitBridgeForAdapter(cfg.DstAdapter),
	)

	obsPoll := observerPollFromConfigAndSuspend(cfg.ObserverPollInterval, cfg.ResumeCopy)
	observer := observe.NewQueueObserver(database, obsPoll)
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

	asCtx := cfg.startAutoscaler(observer, database, copyQueue)
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
					fmt.Printf("\r  Copy: Pass %d (%s) Round %d | Exp:%d Comp:%d | Pend:%d InProg:%d | Partial:%v Workers:%d   ",
						copyPass, passName, lastStats.Round, expected, completed, pending, inProgress, lastPartial, workers)
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
				database.AbortTraversalPhase()
				return copyForceStopStats(copyQueue), fmt.Errorf("migration force stopped during %s", "copy")
			default:
			}
		}

		if cfg.SoftSuspendRequested != nil && cfg.SoftSuspendRequested() {
			waitCtx, cancel := softSuspendWaitContext(cfg.ShutdownContext)
			stats, suspend, err := performQueueSoftSuspend(waitCtx, database, copyQueue, observer, &cfg.PhaseRuntime, "copies", "copy", wc, mr, drainCopyQueue(copyQueue, cfg.ShutdownContext))
			cancel()
			if forceStopOverridesSoftSuspend(cfg.ShutdownContext, database) {
				skipDurableTeardown = true
				performCopyForceStop(copyQueue, observer)
				database.AbortTraversalPhase()
				return copyForceStopStats(copyQueue), fmt.Errorf("migration force stopped during %s", "copy")
			}
			if err != nil {
				return stats, fmt.Errorf("copy soft suspend: %w", err)
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
					performCopyForceStop(copyQueue, observer)
					database.AbortTraversalPhase()
					return copyForceStopStats(copyQueue), fmt.Errorf("migration force stopped during %s", "copy")
				}
				return stats, fmt.Errorf("copy soft suspend teardown: %w", err)
			}
			fmt.Print("\n")
			return stats, newSuspendedError(ErrCopySoftSuspended, stats, suspend)
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
