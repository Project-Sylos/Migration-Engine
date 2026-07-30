// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"
	"time"
)

// DumpStallState prints in-progress and pending task diagnostics for the queue watchdog.
func (q *Queue) DumpStallState(stalledFor time.Duration, inProgress, pending int) {
	fmt.Printf("\n")
	fmt.Printf("========================================\n")
	fmt.Printf("QUEUE WATCHDOG: STALL DETECTED\n")
	fmt.Printf("========================================\n")
	fmt.Printf("Queue: %s\n", q.Name())
	fmt.Printf("Stalled for: %v\n", stalledFor.Round(time.Second))
	fmt.Printf("State: %s\n", q.State())
	fmt.Printf("Mode: %s\n", q.GetMode())
	fmt.Printf("Round: %d\n", q.GetRound())
	fmt.Printf("Pending: %d\n", pending)
	fmt.Printf("InProgress: %d\n", inProgress)
	fmt.Printf("LastPullWasPartial: %v\n", q.GetLastPullWasPartial())
	fmt.Printf("Workers: %d\n", q.GetWorkerCount())

	round := q.GetRound()
	if stats := q.GetRoundStats(round); stats != nil {
		fmt.Printf("RoundStats[%d]: Expected=%d Completed=%d Failed=%d\n",
			round, stats.Expected, stats.Completed, stats.Failed)
	}

	fmt.Printf("\n--- IN-PROGRESS TASKS ---\n")
	q.mu.RLock()
	if len(q.inProgress) == 0 {
		fmt.Printf("  (none)\n")
	} else {
		for id, task := range q.inProgress {
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
	q.mu.RUnlock()

	fmt.Printf("\n--- PENDING TASKS (first 5) ---\n")
	q.mu.RLock()
	if len(q.pendingBuff) == 0 {
		fmt.Printf("  (none)\n")
	} else {
		count := len(q.pendingBuff)
		if count > 5 {
			count = 5
		}
		for i := 0; i < count; i++ {
			task := q.pendingBuff[i]
			fmt.Printf("  [%d] %s (round=%d)\n", i, task.LocationPath(), task.Round)
		}
		if len(q.pendingBuff) > 5 {
			fmt.Printf("  ... and %d more\n", len(q.pendingBuff)-5)
		}
	}
	q.mu.RUnlock()

	fmt.Printf("========================================\n\n")
}

// DumpCompletionStall prints diagnostics when the queue is idle but not advancing.
func (q *Queue) DumpCompletionStall(stalledFor time.Duration) {
	round := q.GetRound()
	info := q.RoundInfoReadOnly(round)
	fmt.Printf("\n")
	fmt.Printf("========================================\n")
	fmt.Printf("QUEUE WATCHDOG: COMPLETION STALL\n")
	fmt.Printf("========================================\n")
	fmt.Printf("Queue: %s\n", q.Name())
	fmt.Printf("Stalled for: %v\n", stalledFor.Round(time.Second))
	fmt.Printf("State: %s\n", q.State())
	fmt.Printf("Mode: %s\n", q.GetMode())
	fmt.Printf("Round: %d\n", round)
	fmt.Printf("Pending: 0 | InProgress: 0\n")
	fmt.Printf("Pulling: %v\n", q.IsPulling())
	fmt.Printf("LastPullWasPartial: %v\n", q.GetLastPullWasPartial())
	if info != nil {
		fmt.Printf("RoundInfo[%d]: PullCount=%d LastPartialPull=%v LastBatchYield=%d\n",
			round, info.PullCount, info.LastPartialPull, info.LastBatchYield)
	} else {
		fmt.Printf("RoundInfo[%d]: (none)\n", round)
	}
	fmt.Printf("HasCountedPull: %v\n", q.RoundHasCountedPull(round))
	fmt.Printf("========================================\n\n")
}
