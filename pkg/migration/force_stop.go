// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"
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

// performTraversalForceStop abandons in-flight work only. No Flush / rebuild / checkpoint:
// hard kill must not block on DuckDB writeMu after Abort (soft-then-hard UX).
func performTraversalForceStop(
	srcQueue, dstQueue *queue.Queue,
	observer *observe.QueueObserver,
) {
	if observer != nil {
		observer.Stop()
	}
	abandonQueuesDBOnly(srcQueue, dstQueue)
}

// performCopyForceStop abandons in-flight copy work only (no Flush / checkpoint).
func performCopyForceStop(copyQueue *queue.Queue, observer *observe.QueueObserver) {
	if observer != nil {
		observer.Stop()
	}
	if copyQueue == nil {
		return
	}
	copyQueue.SetState(queue.QueueStatePaused)
	copyQueue.StopWatchdog()
	copyQueue.Spin.AbandonDBOnly.Store(true)
	copyQueue.ClearPendingBufferForSuspend()
	copyQueue.RequestForceCheckoutAllWorkersForStop()
	copyQueue.CancelBusyWorkerContexts()
	copyQueue.AbandonInProgressTasks()
	copyQueue.ClearPendingBufferForSuspend()
}

func copyForceStopStats(copyQueue *queue.Queue) queue.QueueStats {
	if copyQueue == nil {
		return queue.QueueStats{}
	}
	return copyQueue.Stats()
}

// HandleShutdownSignals sets up signal handlers for SIGINT (Ctrl+C) and SIGTERM.
// When either signal is received, it cancels the provided context.
// If shutdown doesn't complete within 10 seconds, it hard-kills the process.
// This function should be called in a goroutine at the start of the migration.
// On Windows, SIGINT is supported but SIGTERM may not be available.
func HandleShutdownSignals(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)

	// Register signals based on OS
	// SIGINT (Ctrl+C) works on both Windows and Unix
	// SIGTERM works on Unix systems
	signals := []os.Signal{os.Interrupt, syscall.SIGTERM}
	signal.Notify(sigChan, signals...)

	if sig := <-sigChan; sig != nil {
		fmt.Printf("\n⚠️  Shutdown signal received (%v). Initiating graceful shutdown...\n", sig)
		fmt.Println("   (Press Ctrl+C again to force exit if needed)")
		cancel()
	}

	select {
	case sig := <-sigChan:
		if sig != nil {
			fmt.Println("\n⚠️  Second interrupt received. Forcing immediate exit...")
			os.Exit(1)
		}
	case <-time.After(10 * time.Second):
		fmt.Println("\n❌ Shutdown timeout reached (10 seconds). Forcing exit to prevent hang...")
		os.Exit(1)
	}
}
