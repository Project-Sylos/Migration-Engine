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

// DeletePhaseConfig configures delete phase execution.
type DeletePhaseConfig struct {
	DuckDB               *db.DB
	SrcAdapter           types.FSAdapter
	WorkerCount          int
	MaxRetries           int
	LogAddress           string
	LogLevel             string
	SkipListener         bool
	StartupDelay         time.Duration
	ProgressTick         time.Duration
	ShutdownContext      context.Context
	ResumeDelete         *RuntimeSuspendV1
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

func findDeleteStartRound(duckDB *db.DB, retry bool) int {
	maxDepth, err := stats.GetMaxDepth(duckDB, "SRC")
	if err != nil || maxDepth < 1 {
		return 1
	}
	// Delete sweeps depth 1 → maxDepth; depth 0 (root) is metadata-only and never processed.
	if !retry {
		return 1
	}
	status := db.DeleteStatusFailed
	minLevel := -1
	levels, err := pull.GetAllLevels(duckDB, "SRC")
	if err != nil {
		return 1
	}
	for _, level := range levels {
		if level == 0 {
			continue
		}
		for _, nt := range []string{db.NodeTypeFile, db.NodeTypeFolder} {
			c, err := stats.GetDeleteCountAtDepth(duckDB, level, nt, status, true)
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
	duckDB := cfg.DuckDB
	if duckDB == nil {
		return queue.QueueStats{}, fmt.Errorf("DuckDB must be provided")
	}
	if cfg.SrcAdapter == nil {
		return queue.QueueStats{}, fmt.Errorf("source adapter must be provided")
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
			if err := logservice.StartListener(cfg.LogAddress); err == nil {
				time.Sleep(startupDelay)
			}
		}
		if err := logservice.InitGlobalLogger(duckDB.LogsDBForWrite(), cfg.LogAddress, cfg.LogLevel); err != nil {
			return queue.QueueStats{}, fmt.Errorf("failed to initialize logger: %w", err)
		}
	}

	deleteCtx := scalingContextForDelete(cfg.SrcService, cfg.DstService, queue.QueueModeDelete)
	sizing := queueSizingForScalingContext(deleteCtx, nil)
	wc := resolveWorkersForScalingContext(deleteCtx, cfg.WorkerCount, nil)
	mr := effectiveInt(0, cfg.MaxRetries)

	deleteQueue := queue.NewQueue("delete", mr, wc, nil, sizing)
	deleteQueue.SetMode(queue.QueueModeDelete)
	deleteQueue.SetCopyPass(1) // pass 1 = folders for delete (recursive)

	startRound := findDeleteStartRound(duckDB, false)
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
	if maxDepth, err := stats.GetMaxDepth(duckDB, "SRC"); err == nil {
		deleteQueue.SetMaxKnownDepth(maxDepth)
	}

	// Freeze delete-work denominators once at phase start (retry must not shrink).
	if err := stats.SnapshotDeleteWorkAtPhaseStart(duckDB); err != nil {
		return queue.QueueStats{}, fmt.Errorf("snapshot delete work: %w", err)
	}
	seedQueueWorkTotalsFromDB(duckDB, deleteQueue, "delete")

	shutdownCtx := cfg.ShutdownContext
	if shutdownCtx == nil {
		shutdownCtx = context.Background()
	}
	if err := duckDB.BeginTraversalPhase(shutdownCtx); err != nil {
		return queue.QueueStats{}, fmt.Errorf("begin delete phase: %w", err)
	}
	_ = duckDB.Flush(context.Background())
	bulkPhaseClosed := false
	skipDurableTeardown := false
	defer func() {
		if bulkPhaseClosed || skipDurableTeardown {
			return
		}
		_ = duckDB.CheckpointWithRetry(context.Background(), 8)
	}()
	defer func() {
		if bulkPhaseClosed || skipDurableTeardown {
			return
		}
		if err := duckDB.EndTraversalPhase(); err != nil {
			fmt.Println("error ending delete phase", err)
		}
	}()

	deleteQueue.InitializeDeleteWithContext(duckDB, cfg.SrcAdapter, shutdownCtx)
	deleteQueue.SetRateLimitTelemetry(rateLimitBridgeForAdapter(cfg.SrcAdapter))

	obsPoll := observerPollFromConfigAndSuspend(cfg.ObserverPollInterval, nil)
	observer := observe.NewQueueObserver(duckDB, obsPoll)
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

	runCtx := shutdownCtx
	if runCtx == nil {
		runCtx = context.Background()
	}
	asCtx := startDeleteAutoscaler(runCtx, cfg.Autoscaler, observer, duckDB, deleteQueue, cfg.SrcService, cfg.DstService, cfg.PathCheckTarget, cfg.WindowsCompat)
	if cfg.OnAutoscaler != nil {
		cfg.OnAutoscaler(asCtx.Autoscaler())
		defer cfg.OnAutoscaler(nil)
	}
	defer asCtx.stop()

	seedQueueCountersFromDB(duckDB, deleteQueue, "delete", db.QueueStatsPhaseDelete)

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
				fmt.Printf("\r  Delete: Pass %d (%s) Depth %d | Exp:%d Comp:%d | Pend:%d InProg:%d | Partial:%v Workers:%d%s   ",
					deletePass, passName, lastStats.Round, expected, completed, pending, inProgress, lastPartial, workers,
					FormatCatalogSyncSuffix(duckDB))
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
				duckDB.AbortTraversalPhase()
				return deleteQueue.Stats(), fmt.Errorf("migration force stopped during %s", "delete")
			default:
			}
		}
		if cfg.SoftSuspendRequested != nil && cfg.SoftSuspendRequested() {
			waitCtx, cancel := softSuspendWaitContext(cfg.ShutdownContext)
			stats, suspend, err := performDeleteSoftSuspend(waitCtx, duckDB, deleteQueue, observer, cfg, wc, mr)
			cancel()
			if forceStopOverridesSoftSuspend(cfg.ShutdownContext, duckDB) {
				skipDurableTeardown = true
				abandonQueuesDBOnly(deleteQueue)
				duckDB.AbortTraversalPhase()
				return deleteQueue.Stats(), fmt.Errorf("migration force stopped during %s", "delete")
			}
			if err != nil {
				return stats, fmt.Errorf("delete soft suspend: %w", err)
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
					abandonQueuesDBOnly(deleteQueue)
					duckDB.AbortTraversalPhase()
					return deleteQueue.Stats(), fmt.Errorf("migration force stopped during %s", "delete")
				}
				return stats, fmt.Errorf("delete soft suspend teardown: %w", err)
			}
			fmt.Print("\n")
			return stats, newDeleteSuspendedError(stats, suspend)
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
	duckDB := cfg.DuckDB
	if duckDB == nil {
		return queue.QueueStats{}, fmt.Errorf("DuckDB must be provided")
	}
	if cfg.SrcAdapter == nil {
		return queue.QueueStats{}, fmt.Errorf("source adapter must be provided")
	}
	deleteCtx := scalingContextForDelete(cfg.SrcService, cfg.DstService, queue.QueueModeDeleteRetry)
	sizing := queueSizingForScalingContext(deleteCtx, nil)
	wc := resolveWorkersForScalingContext(deleteCtx, cfg.WorkerCount, nil)
	mr := effectiveInt(0, cfg.MaxRetries)

	deleteQueue := queue.NewQueue("delete", mr, wc, nil, sizing)
	deleteQueue.SetMode(queue.QueueModeDeleteRetry)
	deleteQueue.SetCopyPass(1)
	startRound := findDeleteStartRound(duckDB, true)
	if startRound < 1 {
		return queue.QueueStats{}, nil
	}
	deleteQueue.SetRound(startRound)
	deleteQueue.SetExpectedFromStatsBucket(deleteQueue.GetRound())
	if maxDepth, err := stats.GetMaxDepth(duckDB, "SRC"); err == nil {
		deleteQueue.SetMaxKnownDepth(maxDepth)
	}
	// Idempotent: keeps grand delete totals if already sealed; fills them if missing.
	if err := stats.SnapshotDeleteWorkAtPhaseStart(duckDB); err != nil {
		return queue.QueueStats{}, fmt.Errorf("snapshot delete work: %w", err)
	}
	seedQueueWorkTotalsFromDB(duckDB, deleteQueue, "delete")
	shutdownCtx := cfg.ShutdownContext
	if shutdownCtx == nil {
		shutdownCtx = context.Background()
	}
	if err := duckDB.BeginTraversalPhase(shutdownCtx); err != nil {
		return queue.QueueStats{}, err
	}
	bulkPhaseClosed := false
	skipDurableTeardown := false
	defer func() {
		if bulkPhaseClosed || skipDurableTeardown {
			return
		}
		_ = duckDB.CheckpointWithRetry(context.Background(), 8)
	}()
	defer func() {
		if bulkPhaseClosed || skipDurableTeardown {
			return
		}
		if err := duckDB.EndTraversalPhase(); err != nil {
			fmt.Println("error ending delete phase", err)
		}
	}()
	deleteQueue.InitializeDeleteWithContext(duckDB, cfg.SrcAdapter, shutdownCtx)
	deleteQueue.SetRateLimitTelemetry(rateLimitBridgeForAdapter(cfg.SrcAdapter))
	obsPoll := observerPollFromConfigAndSuspend(cfg.ObserverPollInterval, nil)
	observer := observe.NewQueueObserver(duckDB, obsPoll)
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

	runCtx := shutdownCtx
	if runCtx == nil {
		runCtx = context.Background()
	}
	asCtx := startDeleteAutoscaler(runCtx, cfg.Autoscaler, observer, duckDB, deleteQueue, cfg.SrcService, cfg.DstService, cfg.PathCheckTarget, cfg.WindowsCompat)
	if cfg.OnAutoscaler != nil {
		cfg.OnAutoscaler(asCtx.Autoscaler())
		defer cfg.OnAutoscaler(nil)
	}
	defer asCtx.stop()

	seedQueueCountersFromDB(duckDB, deleteQueue, "delete", db.QueueStatsPhaseDelete)
	deleteQueue.PullTasksIfNeeded(true)
	start := time.Now()
	for {
		if shutdownCtx != nil {
			select {
			case <-shutdownCtx.Done():
				skipDurableTeardown = true
				abandonQueuesDBOnly(deleteQueue)
				duckDB.AbortTraversalPhase()
				return deleteQueue.Stats(), fmt.Errorf("migration force stopped during %s", "delete retry")
			default:
			}
		}
		if cfg.SoftSuspendRequested != nil && cfg.SoftSuspendRequested() {
			waitCtx, cancel := softSuspendWaitContext(cfg.ShutdownContext)
			stats, suspend, err := performDeleteSoftSuspend(waitCtx, duckDB, deleteQueue, observer, cfg, wc, mr)
			cancel()
			if forceStopOverridesSoftSuspend(cfg.ShutdownContext, duckDB) {
				skipDurableTeardown = true
				abandonQueuesDBOnly(deleteQueue)
				duckDB.AbortTraversalPhase()
				return deleteQueue.Stats(), fmt.Errorf("migration force stopped during %s", "delete retry")
			}
			if err != nil {
				return stats, fmt.Errorf("delete soft suspend: %w", err)
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
					abandonQueuesDBOnly(deleteQueue)
					duckDB.AbortTraversalPhase()
					return deleteQueue.Stats(), fmt.Errorf("migration force stopped during %s", "delete retry")
				}
				return stats, fmt.Errorf("delete soft suspend teardown: %w", err)
			}
			return stats, newDeleteSuspendedError(stats, suspend)
		}
		if deleteQueue.IsExhausted() {
			fmt.Printf("\nDelete retry complete! Duration: %v\n", time.Since(start))
			bulkPhaseClosed = true
			return deleteQueue.Stats(), nil
		}
		time.Sleep(100 * time.Millisecond)
	}
}
