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
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// DeletePhaseConfig configures delete phase execution.
type DeletePhaseConfig struct {
	PhaseRuntime
	DB           *db.DB
	SrcAdapter   types.FSAdapter
	ResumeDelete *RuntimeSuspendV1
}

func findDeleteStartRound(database *db.DB, retry bool) int {
	maxDepth, err := stats.GetMaxDepth(database, "SRC")
	if err != nil || maxDepth < 1 {
		return 1
	}
	// Delete sweeps depth 1 → maxDepth; depth 0 (root) is metadata-only and never processed.
	if !retry {
		return 1
	}
	status := db.DeleteStatusFailed
	minLevel := -1
	levels, err := pull.GetAllLevels(database, "SRC")
	if err != nil {
		return 1
	}
	for _, level := range levels {
		if level == 0 {
			continue
		}
		for _, nt := range []string{db.NodeTypeFile, db.NodeTypeFolder} {
			c, err := stats.GetDeleteCountAtDepth(database, level, nt, status, true)
			if err == nil && c > 0 {
				if minLevel == -1 || level < minLevel {
					minLevel = level
				}
			}
		}
	}
	if minLevel == -1 {
		return 1
	}
	return minLevel
}

// RunDeletePhase runs the delete phase (forward BFS, global passes: folders then files, depths 1→max; depth 0 root excluded).
func RunDeletePhase(cfg DeletePhaseConfig) (queue.QueueStats, error) {
	database := cfg.DB
	if err := cfg.Begin(database, cfg.SrcAdapter); err != nil {
		return queue.QueueStats{}, err
	}

	deleteCtx := newScalingContext("delete", queue.ScalingModeDelete, queue.QueueModeDelete, 0, cfg.SrcService, cfg.DstService)
	sizing := queueSizingForScalingContext(deleteCtx, nil)
	wc := resolveWorkersForScalingContext(deleteCtx, cfg.WorkerCount, nil)
	mr := effectiveInt(0, cfg.MaxRetries)

	deleteQueue := queue.NewQueue("delete", mr, wc, nil, sizing)
	applyFSStallTimeouts(deleteQueue, cfg.PhaseRuntime)
	deleteQueue.SetMode(queue.QueueModeDelete)
	deleteQueue.SetCopyPass(1) // pass 1 = folders for delete (recursive)

	startRound := findDeleteStartRound(database, false)
	if cfg.ResumeDelete != nil && cfg.ResumeDelete.LastKnownDeleteRound > 0 {
		startRound = cfg.ResumeDelete.LastKnownDeleteRound
	}
	if cfg.ResumeDelete != nil && cfg.ResumeDelete.CopyPass > 0 {
		deleteQueue.SetCopyPass(cfg.ResumeDelete.CopyPass)
	}
	deleteQueue.SetRound(startRound)
	if cfg.ResumeDelete != nil && cfg.ResumeDelete.CopyKeysetCursor != "" {
		deleteQueue.SetKeysetCursor(cfg.ResumeDelete.CopyKeysetCursor)
	}
	deleteQueue.SetExpectedFromStatsBucket(deleteQueue.GetRound())
	if maxDepth, err := stats.GetMaxDepth(database, "SRC"); err == nil {
		deleteQueue.SetMaxKnownDepth(maxDepth)
	}

	// Freeze delete-work denominators once at phase start (retry must not shrink).
	if err := stats.SnapshotDeleteWorkAtPhaseStart(database); err != nil {
		return queue.QueueStats{}, fmt.Errorf("snapshot delete work: %w", err)
	}
	seedQueueWorkTotalsFromDB(database, deleteQueue, "delete")

	shutdownCtx := cfg.ShutdownOrBackground()
	if err := database.BeginTraversalPhase(shutdownCtx); err != nil {
		return queue.QueueStats{}, fmt.Errorf("begin delete phase: %w", err)
	}
	_ = database.Flush(context.Background())
	bulkPhaseClosed := false
	skipDurableTeardown := false
	defer func() {
		if bulkPhaseClosed || skipDurableTeardown {
			return
		}
		_ = database.CheckpointWithRetry(context.Background(), 8)
	}()
	defer func() {
		if bulkPhaseClosed || skipDurableTeardown {
			return
		}
		if err := database.EndTraversalPhase(); err != nil {
			fmt.Println("error ending delete phase", err)
		}
	}()

	deleteQueue.Initialize(queue.PoolDelete, database, cfg.SrcAdapter, nil, shutdownCtx)
	deleteQueue.SetRateLimitTelemetry(rateLimitBridgeForAdapter(cfg.SrcAdapter))

	obsPoll := observerPollFromConfigAndSuspend(cfg.ObserverPollInterval, nil)
	observer := observe.NewQueueObserver(database, obsPoll)
	observer.Start()
	defer observer.Stop()
	if cfg.OnQueueObserver != nil {
		defer cfg.OnQueueObserver(nil)
	}
	observer.RegisterQueue("delete", deleteQueue)
	deleteQueue.SetObserver(observer)
	if cfg.OnQueueObserver != nil {
		cfg.OnQueueObserver(observer)
	}

	asCtx := cfg.startAutoscaler(observer, database, deleteQueue)
	if cfg.OnAutoscaler != nil {
		cfg.OnAutoscaler(asCtx.Autoscaler())
		defer cfg.OnAutoscaler(nil)
	}
	defer asCtx.stop()

	seedQueueCountersFromDB(database, deleteQueue, "delete", db.QueueStatsPhaseDelete)

	statsChan := make(chan queue.QueueStats, 10)
	deleteQueue.SetStatsChannel(statsChan)

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
				if lastStats == nil {
					continue
				}
				deletePass := deleteQueue.GetCopyPass()
				passName := "folders"
				if deletePass == 2 {
					passName = "files"
				}
				roundStats := deleteQueue.GetRoundStats(lastStats.Round)
				expected, completed := 0, 0
				if roundStats != nil {
					expected = roundStats.Expected
					completed = roundStats.Completed
				}
				pending := deleteQueue.GetPendingCount()
				inProgress := deleteQueue.InProgressCount()
				lastPartial := deleteQueue.GetLastPullWasPartial()
				workers := deleteQueue.GetWorkerCount()
				fmt.Printf("\r  Delete: Pass %d (%s) Depth %d | Exp:%d Comp:%d | Pend:%d InProg:%d | Partial:%v Workers:%d   ",
					deletePass, passName, lastStats.Round, expected, completed, pending, inProgress, lastPartial, workers)
			}
		}
	}()

	deleteQueue.PullTasksIfNeeded(true)

	start := time.Now()
	for {
		if shutdownCtx != nil {
			select {
			case <-shutdownCtx.Done():
				skipDurableTeardown = true
				abandonQueuesDBOnly(deleteQueue)
				database.AbortTraversalPhase()
				return deleteQueue.Stats(), fmt.Errorf("migration force stopped during %s", "delete")
			default:
			}
		}
		if cfg.SoftSuspendRequested != nil && cfg.SoftSuspendRequested() {
			waitCtx, cancel := softSuspendWaitContext(cfg.ShutdownContext)
			stats, suspend, err := performQueueSoftSuspend(waitCtx, database, deleteQueue, observer, &cfg.PhaseRuntime, "removals", "delete", wc, mr, drainSoftStopWithGrace(cfg.ShutdownContext, deleteQueue))
			cancel()
			if forceStopOverridesSoftSuspend(cfg.ShutdownContext, database) {
				skipDurableTeardown = true
				abandonQueuesDBOnly(deleteQueue)
				database.AbortTraversalPhase()
				return deleteQueue.Stats(), fmt.Errorf("migration force stopped during %s", "delete")
			}
			if err != nil {
				return stats, fmt.Errorf("delete soft suspend: %w", err)
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
					abandonQueuesDBOnly(deleteQueue)
					database.AbortTraversalPhase()
					return deleteQueue.Stats(), fmt.Errorf("migration force stopped during %s", "delete")
				}
				return stats, fmt.Errorf("delete soft suspend teardown: %w", err)
			}
			fmt.Print("\n")
			return stats, newSuspendedError(ErrDeleteSoftSuspended, stats, suspend)
		}
		if deleteQueue.IsExhausted() {
			progressTicker.Stop()
			fmt.Print("\n")
			fmt.Printf("\nDelete phase complete! Duration: %v\n", time.Since(start))
			bulkPhaseClosed = true
			return deleteQueue.Stats(), nil
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// RunDeleteRetryPhase runs delete retry (only delete_status = failed).
func RunDeleteRetryPhase(cfg DeletePhaseConfig) (queue.QueueStats, error) {
	database := cfg.DB
	if err := cfg.Begin(database, cfg.SrcAdapter); err != nil {
		return queue.QueueStats{}, err
	}
	deleteCtx := newScalingContext("delete", queue.ScalingModeDelete, queue.QueueModeDeleteRetry, 0, cfg.SrcService, cfg.DstService)
	sizing := queueSizingForScalingContext(deleteCtx, nil)
	wc := resolveWorkersForScalingContext(deleteCtx, cfg.WorkerCount, nil)
	mr := effectiveInt(0, cfg.MaxRetries)

	deleteQueue := queue.NewQueue("delete", mr, wc, nil, sizing)
	applyFSStallTimeouts(deleteQueue, cfg.PhaseRuntime)
	deleteQueue.SetMode(queue.QueueModeDeleteRetry)
	deleteQueue.SetCopyPass(1)
	startRound := findDeleteStartRound(database, true)
	if startRound < 1 {
		return queue.QueueStats{}, nil
	}
	deleteQueue.SetRound(startRound)
	deleteQueue.SetExpectedFromStatsBucket(deleteQueue.GetRound())
	if maxDepth, err := stats.GetMaxDepth(database, "SRC"); err == nil {
		deleteQueue.SetMaxKnownDepth(maxDepth)
	}
	// Idempotent: keeps grand delete totals if already sealed; fills them if missing.
	if err := stats.SnapshotDeleteWorkAtPhaseStart(database); err != nil {
		return queue.QueueStats{}, fmt.Errorf("snapshot delete work: %w", err)
	}
	seedQueueWorkTotalsFromDB(database, deleteQueue, "delete")
	shutdownCtx := cfg.ShutdownOrBackground()
	if err := database.BeginTraversalPhase(shutdownCtx); err != nil {
		return queue.QueueStats{}, err
	}
	bulkPhaseClosed := false
	skipDurableTeardown := false
	defer func() {
		if bulkPhaseClosed || skipDurableTeardown {
			return
		}
		_ = database.CheckpointWithRetry(context.Background(), 8)
	}()
	defer func() {
		if bulkPhaseClosed || skipDurableTeardown {
			return
		}
		if err := database.EndTraversalPhase(); err != nil {
			fmt.Println("error ending delete phase", err)
		}
	}()
	deleteQueue.Initialize(queue.PoolDelete, database, cfg.SrcAdapter, nil, shutdownCtx)
	deleteQueue.SetRateLimitTelemetry(rateLimitBridgeForAdapter(cfg.SrcAdapter))
	obsPoll := observerPollFromConfigAndSuspend(cfg.ObserverPollInterval, nil)
	observer := observe.NewQueueObserver(database, obsPoll)
	observer.Start()
	defer observer.Stop()
	if cfg.OnQueueObserver != nil {
		defer cfg.OnQueueObserver(nil)
	}
	observer.RegisterQueue("delete", deleteQueue)
	deleteQueue.SetObserver(observer)
	if cfg.OnQueueObserver != nil {
		cfg.OnQueueObserver(observer)
	}

	asCtx := cfg.startAutoscaler(observer, database, deleteQueue)
	if cfg.OnAutoscaler != nil {
		cfg.OnAutoscaler(asCtx.Autoscaler())
		defer cfg.OnAutoscaler(nil)
	}
	defer asCtx.stop()

	seedQueueCountersFromDB(database, deleteQueue, "delete", db.QueueStatsPhaseDelete)
	deleteQueue.PullTasksIfNeeded(true)
	start := time.Now()
	for {
		if shutdownCtx != nil {
			select {
			case <-shutdownCtx.Done():
				skipDurableTeardown = true
				abandonQueuesDBOnly(deleteQueue)
				database.AbortTraversalPhase()
				return deleteQueue.Stats(), fmt.Errorf("migration force stopped during %s", "delete retry")
			default:
			}
		}
		if cfg.SoftSuspendRequested != nil && cfg.SoftSuspendRequested() {
			waitCtx, cancel := softSuspendWaitContext(cfg.ShutdownContext)
			stats, suspend, err := performQueueSoftSuspend(waitCtx, database, deleteQueue, observer, &cfg.PhaseRuntime, "removals", "delete", wc, mr, drainSoftStopWithGrace(cfg.ShutdownContext, deleteQueue))
			cancel()
			if forceStopOverridesSoftSuspend(cfg.ShutdownContext, database) {
				skipDurableTeardown = true
				abandonQueuesDBOnly(deleteQueue)
				database.AbortTraversalPhase()
				return deleteQueue.Stats(), fmt.Errorf("migration force stopped during %s", "delete retry")
			}
			if err != nil {
				return stats, fmt.Errorf("delete soft suspend: %w", err)
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
					abandonQueuesDBOnly(deleteQueue)
					database.AbortTraversalPhase()
					return deleteQueue.Stats(), fmt.Errorf("migration force stopped during %s", "delete retry")
				}
				return stats, fmt.Errorf("delete soft suspend teardown: %w", err)
			}
			return stats, newSuspendedError(ErrDeleteSoftSuspended, stats, suspend)
		}
		if deleteQueue.IsExhausted() {
			fmt.Printf("\nDelete retry complete! Duration: %v\n", time.Since(start))
			bulkPhaseClosed = true
			return deleteQueue.Stats(), nil
		}
		time.Sleep(100 * time.Millisecond)
	}
}
