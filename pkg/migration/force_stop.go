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
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer drainCancel()
	_ = srcQueue.WaitInProgressZero(drainCtx, 50*time.Millisecond)
	_ = dstQueue.WaitInProgressZero(drainCtx, 50*time.Millisecond)
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer flushCancel()
	flushDone := make(chan struct{})
	go func() {
		_ = database.FlushSealBuffer()
		close(flushDone)
	}()
	select {
	case <-flushDone:
	case <-flushCtx.Done():
	}
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
	copyQueue.EnterStopAbandonWindow(queue.DefaultSpinDownGrace)
	copyQueue.RequestForceCheckoutAllWorkersForStop()
	copyQueue.AbandonInProgressTasks()
	copyQueue.ClearStopAbandonWindow()
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer drainCancel()
	_ = copyQueue.WaitInProgressZero(drainCtx, 50*time.Millisecond)
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer flushCancel()
	flushDone := make(chan struct{})
	go func() {
		_ = database.FlushSealBuffer()
		close(flushDone)
	}()
	select {
	case <-flushDone:
	case <-flushCtx.Done():
	}
	_ = database.CheckpointWithRetry(flushCtx, 1)
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
