// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"testing"
	"time"
)

func TestQueueWatchdogBenignCompletionIdle(t *testing.T) {
	q := NewQueue("delete", 3, 1, nil, nil)
	q.SetMode(QueueModeDelete)
	q.mu.Lock()
	q.round = 1
	q.lastPullWasPartial = true
	q.roundInfoMap[1] = &RoundInfo{Round: 1, PullCount: 1347, LastPartialPull: true, LastBatchYield: 0}
	q.mu.Unlock()

	wd := NewQueueWatchdog(q, 30*time.Second)
	wd.lastProgress.Store(time.Now().Add(-31 * time.Second).UnixNano())

	wd.checkForStall()

	if wd.PossibleStall() {
		t.Fatal("expected no possible stall when round advance gate is open")
	}
}

func TestQueueWatchdogCompletionIdleWithoutTerminalPull(t *testing.T) {
	q := NewQueue("delete", 3, 1, nil, nil)
	q.SetMode(QueueModeDelete)
	q.mu.Lock()
	q.round = 1
	q.lastPullWasPartial = false
	q.roundInfoMap[1] = &RoundInfo{Round: 1, PullCount: 10, LastPartialPull: false}
	q.mu.Unlock()

	wd := NewQueueWatchdog(q, 30*time.Second)
	wd.lastProgress.Store(time.Now().Add(-31 * time.Second).UnixNano())

	wd.checkForStall()

	if wd.PossibleStall() {
		t.Fatal("completion idle without in-flight work should not surface PossibleStall")
	}
}

func TestQueueWatchdogFlagsStallWithPendingWork(t *testing.T) {
	q := NewQueue("delete", 3, 1, nil, nil)
	q.SetMode(QueueModeDelete)
	q.mu.Lock()
	q.pendingBuff = append(q.pendingBuff, &TaskBase{ID: "task-1", Round: 1, Type: TaskTypeDeleteFile})
	q.mu.Unlock()

	wd := NewQueueWatchdog(q, 30*time.Second)
	wd.lastProgress.Store(time.Now().Add(-31 * time.Second).UnixNano())

	wd.checkForStall()

	if !wd.PossibleStall() {
		t.Fatal("expected possible stall when pending work exists but no progress")
	}
}
