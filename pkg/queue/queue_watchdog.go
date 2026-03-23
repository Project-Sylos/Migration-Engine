// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"
	"sync/atomic"
	"time"
)

const (
	defaultQueueStallTimeout = 30 * time.Second
	queueWatchdogCheckInterval = 5 * time.Second
)

// QueueWatchdog monitors queue progress and dumps state when stalled.
// A stall is detected when no tasks complete for the configured timeout
// while tasks remain in-progress or pending.
type QueueWatchdog struct {
	queue        *Queue
	stallTimeout time.Duration
	lastProgress atomic.Int64 // UnixNano of last progress
	stopCh       chan struct{}
	stopped      atomic.Bool
}

// NewQueueWatchdog creates a watchdog for the given queue.
func NewQueueWatchdog(q *Queue, stallTimeout time.Duration) *QueueWatchdog {
	if stallTimeout <= 0 {
		stallTimeout = defaultQueueStallTimeout
	}
	wd := &QueueWatchdog{
		queue:        q,
		stallTimeout: stallTimeout,
		stopCh:       make(chan struct{}),
	}
	wd.Beat() // Initialize with current time
	return wd
}

// Beat records progress (call when a task completes).
func (wd *QueueWatchdog) Beat() {
	wd.lastProgress.Store(time.Now().UnixNano())
}

// Start begins monitoring in a background goroutine.
func (wd *QueueWatchdog) Start() {
	go wd.monitor()
}

// Stop stops the watchdog monitoring.
func (wd *QueueWatchdog) Stop() {
	if wd.stopped.CompareAndSwap(false, true) {
		close(wd.stopCh)
	}
}

func (wd *QueueWatchdog) monitor() {
	ticker := time.NewTicker(queueWatchdogCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-wd.stopCh:
			return
		case <-ticker.C:
			wd.checkForStall()
		}
	}
}

func (wd *QueueWatchdog) checkForStall() {
	if wd.queue.State() == QueueStatePaused {
		wd.Beat()
		return
	}
	if wd.queue.sealIOWaitActive() {
		wd.Beat()
		return
	}

	lastBeat := time.Unix(0, wd.lastProgress.Load())
	elapsed := time.Since(lastBeat)

	if elapsed < wd.stallTimeout {
		return // Not stalled yet
	}

	// Check if queue has work that should be progressing
	inProgress := wd.queue.InProgressCount()
	pending := wd.queue.GetPendingCount()

	if inProgress == 0 && pending == 0 {
		return // No work, not a stall
	}

	// Queue appears stalled - dump state
	wd.dumpState(elapsed, inProgress, pending)

	// Reset beat to avoid spamming (will dump again in another stallTimeout if still stuck)
	wd.Beat()
}

func (wd *QueueWatchdog) dumpState(stalledFor time.Duration, inProgress, pending int) {
	fmt.Printf("\n")
	fmt.Printf("========================================\n")
	fmt.Printf("QUEUE WATCHDOG: STALL DETECTED\n")
	fmt.Printf("========================================\n")
	fmt.Printf("Queue: %s\n", wd.queue.Name())
	fmt.Printf("Stalled for: %v\n", stalledFor.Round(time.Second))
	fmt.Printf("State: %s\n", wd.queue.State())
	fmt.Printf("Mode: %s\n", wd.queue.GetMode())
	fmt.Printf("Round: %d\n", wd.queue.GetRound())
	fmt.Printf("Pending: %d\n", pending)
	fmt.Printf("InProgress: %d\n", inProgress)
	fmt.Printf("LastPullWasPartial: %v\n", wd.queue.GetLastPullWasPartial())
	fmt.Printf("Workers: %d\n", wd.queue.GetWorkerCount())

	// Get round stats
	round := wd.queue.GetRound()
	if stats := wd.queue.GetRoundStats(round); stats != nil {
		fmt.Printf("RoundStats[%d]: Expected=%d Completed=%d Failed=%d\n",
			round, stats.Expected, stats.Completed, stats.Failed)
	}

	// Dump in-progress tasks
	fmt.Printf("\n--- IN-PROGRESS TASKS ---\n")
	wd.queue.mu.RLock()
	if len(wd.queue.inProgress) == 0 {
		fmt.Printf("  (none)\n")
	} else {
		for id, task := range wd.queue.inProgress {
			taskType := "file"
			if task.IsFolder() {
				taskType = "folder"
			}
			leaseAge := time.Since(task.LeaseTime).Round(time.Second)
			workerResult := task.WorkerResult
			if workerResult == "" {
				workerResult = "(executing)"
			}
			fmt.Printf("  ID: %s\n", id)
			fmt.Printf("    Path: %s\n", task.LocationPath())
			fmt.Printf("    Type: %s\n", taskType)
			fmt.Printf("    Round: %d\n", task.Round)
			fmt.Printf("    CopyPass: %d\n", task.CopyPass)
			fmt.Printf("    Attempts: %d\n", task.Attempts)
			fmt.Printf("    LeaseAge: %v\n", leaseAge)
			fmt.Printf("    Locked: %v\n", task.Locked)
			fmt.Printf("    WorkerResult: %s\n", workerResult)
			fmt.Printf("    Status: %s\n", task.Status)
			if task.LastError != "" {
				fmt.Printf("    LastError: %s\n", task.LastError)
			}
		}
	}
	wd.queue.mu.RUnlock()

	// Dump first few pending tasks
	fmt.Printf("\n--- PENDING TASKS (first 5) ---\n")
	wd.queue.mu.RLock()
	if len(wd.queue.pendingBuff) == 0 {
		fmt.Printf("  (none)\n")
	} else {
		count := len(wd.queue.pendingBuff)
		if count > 5 {
			count = 5
		}
		for i := 0; i < count; i++ {
			task := wd.queue.pendingBuff[i]
			fmt.Printf("  [%d] %s (round=%d)\n", i, task.LocationPath(), task.Round)
		}
		if len(wd.queue.pendingBuff) > 5 {
			fmt.Printf("  ... and %d more\n", len(wd.queue.pendingBuff)-5)
		}
	}
	wd.queue.mu.RUnlock()

	fmt.Printf("========================================\n\n")
}
