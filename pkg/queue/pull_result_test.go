// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"testing"
)

func TestConfirmRoundAdvanceGate(t *testing.T) {
	q := NewQueue("src", 3, 1, nil, nil)
	q.mu.Lock()
	q.round = 2
	q.lastPullWasPartial = true
	q.roundInfoMap[2] = &RoundInfo{Round: 2, PullCount: 1, LastPartialPull: true}
	q.mu.Unlock()

	if !q.confirmRoundAdvanceGate(2) {
		t.Fatal("expected gate open with empty buffer and terminal pull")
	}
	if q.confirmRoundAdvanceGate(1) {
		t.Fatal("expected gate closed for stale round arg")
	}

	q.mu.Lock()
	q.pendingBuff = append(q.pendingBuff, &TaskBase{ID: "x", Round: 2})
	q.mu.Unlock()
	if q.confirmRoundAdvanceGate(2) {
		t.Fatal("expected gate closed with pending buffer")
	}
}

func TestRoundHasCountedPull(t *testing.T) {
	q := NewQueue("src", 3, 1, nil, nil)
	if q.roundHasCountedPull(0) {
		t.Fatal("expected no counted pull before any record")
	}
	q.recordPull(0, 0, true)
	if !q.roundHasCountedPull(0) {
		t.Fatal("expected counted pull after recordPull")
	}
}

func TestDequeuePendingDoesNotDropOldRoundTasks(t *testing.T) {
	q := NewQueue("src", 3, 1, nil, nil)
	q.mu.Lock()
	q.round = 3
	task := &TaskBase{ID: "folder-1", Round: 2, Type: TaskTypeSrcTraversal}
	q.pendingBuff = append(q.pendingBuff, task)
	q.mu.Unlock()

	leased := q.dequeuePending()
	if leased == nil {
		t.Fatal("expected to lease buffered task from prior round")
	}
	if leased.ID != "folder-1" || leased.Round != 2 {
		t.Fatalf("unexpected leased task: %+v", leased)
	}
}

func TestShouldDeferForcePull(t *testing.T) {
	q := NewQueue("src", 3, 1, nil, nil)
	q.mu.Lock()
	q.pulling = true
	q.mu.Unlock()
	if !q.shouldDeferForcePull() {
		t.Fatal("expected defer while pulling")
	}

	q.mu.Lock()
	q.pulling = false
	q.lastPullWasPartial = false
	q.pendingBuff = append(q.pendingBuff, &TaskBase{ID: "a", Round: 0})
	q.mu.Unlock()
	if !q.shouldDeferForcePull() {
		t.Fatal("expected defer while buffer has work and keyspace not exhausted")
	}

	q.mu.Lock()
	q.pendingBuff = nil
	q.lastPullWasPartial = true
	q.mu.Unlock()
	if q.shouldDeferForcePull() {
		t.Fatal("expected no defer when idle and partial")
	}
}

func TestPullResultOK(t *testing.T) {
	if !(PullResult{Status: PullOK}).OK() {
		t.Fatal("PullOK should be OK")
	}
	if (PullResult{Status: PullSkipped}).OK() {
		t.Fatal("PullSkipped should not be OK")
	}
}
