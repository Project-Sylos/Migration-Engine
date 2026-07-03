// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

func queueSizingFromSuspend(s *RuntimeSuspendV1) *queue.QueueSizing {
	if s == nil {
		return nil
	}
	if s.LeaseBatchSize <= 0 && s.RefillBatchSize <= 0 {
		return nil
	}
	return &queue.QueueSizing{
		LeaseBatchSize:  s.LeaseBatchSize,
		RefillBatchSize: s.RefillBatchSize,
	}
}

func effectiveWorkerCount(cfgWorker int, s *RuntimeSuspendV1) int {
	if s != nil && s.WorkerCount > 0 {
		return s.WorkerCount
	}
	return cfgWorker
}

func effectiveMaxRetries(cfgRetries int, s *RuntimeSuspendV1) int {
	if s != nil && s.MaxRetries > 0 {
		return s.MaxRetries
	}
	return cfgRetries
}

func observerPollFromConfigAndSuspend(cfg time.Duration, s *RuntimeSuspendV1) time.Duration {
	if s != nil && s.ObserverPollMs > 0 {
		return time.Duration(s.ObserverPollMs) * time.Millisecond
	}
	if cfg > 0 {
		return cfg
	}
	return 200 * time.Millisecond
}

func progressTickFromConfigAndSuspend(cfg time.Duration, s *RuntimeSuspendV1, fallback time.Duration) time.Duration {
	if s != nil && s.ProgressTickMs > 0 {
		return time.Duration(s.ProgressTickMs) * time.Millisecond
	}
	if cfg > 0 {
		return cfg
	}
	return fallback
}

func performTraversalSoftSuspend(
	waitCtx context.Context,
	database *db.DB,
	srcQueue, dstQueue *queue.Queue,
	observer *queue.QueueObserver,
	coordinator *queue.QueueCoordinator,
	cfg MigrationConfig,
	start time.Time,
	workerCount, maxRetries int,
) (RuntimeStats, RuntimeSuspendV1, error) {
	if observer != nil {
		observer.Stop()
	}
	srcQueue.Pause()
	dstQueue.Pause()
	srcQueue.StopWatchdog()
	dstQueue.StopWatchdog()
	srcQueue.ClearPendingBufferForSuspend()
	dstQueue.ClearPendingBufferForSuspend()

	mergedCtx, cancelMerged := mergeWaitContexts(waitCtx, cfg.ShutdownContext)
	defer cancelMerged()

	if err := srcQueue.WaitInProgressZero(mergedCtx, 50*time.Millisecond); err != nil {
		if cfg.ShutdownContext != nil && cfg.ShutdownContext.Err() != nil {
			srcQueue.AbandonInProgressTasks()
			dstQueue.AbandonInProgressTasks()
		}
		return RuntimeStats{}, RuntimeSuspendV1{}, fmt.Errorf("src queue drain in-flight: %w", err)
	}
	if err := dstQueue.WaitInProgressZero(mergedCtx, 50*time.Millisecond); err != nil {
		if cfg.ShutdownContext != nil && cfg.ShutdownContext.Err() != nil {
			srcQueue.AbandonInProgressTasks()
			dstQueue.AbandonInProgressTasks()
		}
		return RuntimeStats{}, RuntimeSuspendV1{}, fmt.Errorf("dst queue drain in-flight: %w", err)
	}

	if err := database.FlushSealBuffer(); err != nil {
		return RuntimeStats{}, RuntimeSuspendV1{}, fmt.Errorf("flush seal buffer: %w", err)
	}
	if err := database.FlushAppenderBuffer(); err != nil {
		return RuntimeStats{}, RuntimeSuspendV1{}, fmt.Errorf("flush appender buffer: %w", err)
	}
	if err := database.CheckpointWithRetry(waitCtx, 5); err != nil {
		return RuntimeStats{}, RuntimeSuspendV1{}, fmt.Errorf("checkpoint: %w", err)
	}

	srcStats, dstStats := snapshotTraversalQueueStats(database, coordinator)
	maxD := srcQueue.GetMaxKnownDepth()
	if d := dstQueue.GetMaxKnownDepth(); d > maxD {
		maxD = d
	}
	if maxD < 0 {
		if d, err := database.GetMaxDepth("SRC"); err == nil {
			maxD = d
		}
	}

	obsMs := int(cfg.ObserverPollInterval / time.Millisecond)
	if obsMs <= 0 {
		obsMs = 200
	}
	progMs := int(cfg.ProgressTick / time.Millisecond)
	if progMs <= 0 {
		progMs = 1000
	}

	suspend := newTraversalSuspendState(
		workerCount, maxRetries,
		srcQueue.EffectiveLeaseBatchSize(), srcQueue.EffectiveRefillBatchSize(),
		obsMs, progMs,
		srcStats.Round, dstStats.Round, maxD,
	)

	stats := RuntimeStats{
		Duration: time.Since(start),
		Src:      srcStats,
		Dst:      dstStats,
	}
	return stats, suspend, nil
}

func performCopySoftSuspend(
	waitCtx context.Context,
	database *db.DB,
	copyQueue *queue.Queue,
	observer *queue.QueueObserver,
	cfg CopyPhaseConfig,
	workerCount, maxRetries int,
) (queue.QueueStats, RuntimeSuspendV1, error) {
	if observer != nil {
		observer.Stop()
	}
	copyQueue.Pause()
	copyQueue.StopWatchdog()
	copyQueue.ClearPendingBufferForSuspend()

	mergedCtx, cancelMerged := mergeWaitContexts(waitCtx, cfg.ShutdownContext)
	defer cancelMerged()

	if err := copyQueue.WaitInProgressZero(mergedCtx, 50*time.Millisecond); err != nil {
		if cfg.ShutdownContext != nil && cfg.ShutdownContext.Err() != nil {
			copyQueue.AbandonInProgressTasks()
		}
		return queue.QueueStats{}, RuntimeSuspendV1{}, fmt.Errorf("copy queue drain in-flight: %w", err)
	}

	if err := database.FlushSealBuffer(); err != nil {
		return queue.QueueStats{}, RuntimeSuspendV1{}, fmt.Errorf("flush seal buffer: %w", err)
	}
	if err := database.FlushAppenderBuffer(); err != nil {
		return queue.QueueStats{}, RuntimeSuspendV1{}, fmt.Errorf("flush appender buffer: %w", err)
	}
	if err := database.CheckpointWithRetry(waitCtx, 5); err != nil {
		return queue.QueueStats{}, RuntimeSuspendV1{}, fmt.Errorf("checkpoint: %w", err)
	}

	stats := copyQueue.Stats()
	obsMs := int(cfg.ObserverPollInterval / time.Millisecond)
	if obsMs <= 0 {
		obsMs = 200
	}
	progMs := int(cfg.ProgressTick / time.Millisecond)
	if progMs <= 0 {
		progMs = 2000
	}

	suspend := newCopySuspendState(
		workerCount, maxRetries,
		copyQueue.EffectiveLeaseBatchSize(), copyQueue.EffectiveRefillBatchSize(),
		obsMs, progMs,
		copyQueue.GetCopyPass(), stats.Round, copyQueue.GetMaxKnownDepth(),
	)

	return stats, suspend, nil
}
