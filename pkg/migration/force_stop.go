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

func mergeWaitContexts(primary context.Context, shutdown context.Context) (context.Context, context.CancelFunc) {
	if primary == nil && shutdown == nil {
		return context.WithCancel(context.Background())
	}
	if primary == nil {
		return context.WithCancel(shutdown)
	}
	if shutdown == nil {
		return context.WithCancel(primary)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-primary.Done():
		case <-shutdown.Done():
		}
		cancel()
	}()
	return ctx, cancel
}

func performTraversalForceStop(
	database *db.DB,
	srcQueue, dstQueue *queue.Queue,
	observer *queue.QueueObserver,
) {
	if observer != nil {
		observer.Stop()
	}
	srcQueue.Pause()
	dstQueue.Pause()
	srcQueue.StopWatchdog()
	dstQueue.StopWatchdog()
	srcQueue.ClearPendingBufferForSuspend()
	dstQueue.ClearPendingBufferForSuspend()
	srcQueue.AbandonInProgressTasks()
	dstQueue.AbandonInProgressTasks()
	_ = srcQueue.WaitInProgressZero(context.Background(), 50*time.Millisecond)
	_ = dstQueue.WaitInProgressZero(context.Background(), 50*time.Millisecond)
	flushCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = database.FlushSealBuffer()
	_ = database.FlushAppenderBuffer()
	_ = database.CheckpointWithRetry(flushCtx, 1)
}

func performCopyForceStop(
	database *db.DB,
	copyQueue *queue.Queue,
	observer *queue.QueueObserver,
) {
	if observer != nil {
		observer.Stop()
	}
	copyQueue.Pause()
	copyQueue.StopWatchdog()
	copyQueue.ClearPendingBufferForSuspend()
	copyQueue.AbandonInProgressTasks()
	_ = copyQueue.WaitInProgressZero(context.Background(), 50*time.Millisecond)
	flushCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = database.FlushSealBuffer()
	_ = database.FlushAppenderBuffer()
	_ = database.CheckpointWithRetry(flushCtx, 1)
}

func traversalForceStopStats(database *db.DB, coordinator *queue.QueueCoordinator) RuntimeStats {
	srcStats, dstStats := snapshotTraversalQueueStats(database, coordinator)
	return RuntimeStats{Src: srcStats, Dst: dstStats}
}

func copyForceStopStats(copyQueue *queue.Queue) queue.QueueStats {
	if copyQueue == nil {
		return queue.QueueStats{}
	}
	return copyQueue.Stats()
}

func forceStopErr(phase string) error {
	return fmt.Errorf("migration force stopped during %s", phase)
}
