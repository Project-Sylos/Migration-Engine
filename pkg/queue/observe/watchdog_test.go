// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package observe

import (
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

func TestQueueWatchdogBenignCompletionIdle(t *testing.T) {
	q := queue.NewQueue("delete", 3, 1, nil, nil)
	q.SetMode(queue.QueueModeDelete)
	q.SetState(queue.QueueStateRunning)
	q.SetRound(1)
	q.RecordPull(1, 0, true)
	q.SetLastPullWasPartial(true)

	wd := NewQueueWatchdog(q, 30*time.Second)
	wd.lastProgress.Store(time.Now().Add(-31 * time.Second).UnixNano())

	wd.checkForStall()

	if wd.PossibleStall() {
		t.Fatal("expected no possible stall when round advance gate is open")
	}
}

func TestQueueWatchdogCompletionIdleWithoutTerminalPull(t *testing.T) {
	q := queue.NewQueue("delete", 3, 1, nil, nil)
	q.SetMode(queue.QueueModeDelete)
	q.SetState(queue.QueueStateRunning)
	q.SetRound(1)
	q.RecordPull(1, 10, false)
	q.SetLastPullWasPartial(false)

	wd := NewQueueWatchdog(q, 30*time.Second)
	wd.lastProgress.Store(time.Now().Add(-31 * time.Second).UnixNano())

	wd.checkForStall()

	if wd.PossibleStall() {
		t.Fatal("completion idle without in-flight work should not surface PossibleStall")
	}
}

func TestQueueWatchdogFlagsStallWithPendingWork(t *testing.T) {
	q := queue.NewQueue("delete", 3, 1, nil, nil)
	q.SetMode(queue.QueueModeDelete)
	q.SetState(queue.QueueStateRunning)
	if !q.Add(&queue.TaskBase{ID: "task-1", Round: 1, Type: queue.TaskTypeDeleteFile}) {
		t.Fatal("failed to enqueue pending task")
	}

	wd := NewQueueWatchdog(q, 30*time.Second)
	wd.lastProgress.Store(time.Now().Add(-31 * time.Second).UnixNano())

	wd.checkForStall()

	if !wd.PossibleStall() {
		t.Fatal("expected possible stall when pending work exists but no progress")
	}
}

func TestQueueWatchdogBeatClearsInFlightLeaseStall(t *testing.T) {
	// Batch/copy leases hold tasks in-progress until ReportTaskResult; mid-flight
	// QueueWatchdog.Beat (e.g. beatQueueWatchdogWhile / chunk progress) must prevent false stall.
	q := queue.NewQueue("copy", 3, 1, nil, nil)
	q.SetMode(queue.QueueModeCopy)
	q.SetState(queue.QueueStateRunning)
	task := &queue.TaskBase{ID: "leased-batch", Round: 1, Type: queue.TaskTypeCopyFile}
	q.AddInProgress(task.ID, task)

	wd := NewQueueWatchdog(q, 30*time.Second)
	wd.lastProgress.Store(time.Now().Add(-31 * time.Second).UnixNano())

	wd.Beat()
	wd.checkForStall()
	if wd.PossibleStall() {
		t.Fatal("expected no stall when QueueWatchdog was Beaten while tasks are in-progress")
	}

	wd.lastProgress.Store(time.Now().Add(-31 * time.Second).UnixNano())
	wd.checkForStall()
	if !wd.PossibleStall() {
		t.Fatal("expected stall when in-progress work has had no Beat past stallTimeout")
	}
}
