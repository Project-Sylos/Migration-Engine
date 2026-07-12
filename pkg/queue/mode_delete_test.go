// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"testing"
)

func newDeleteQueueForTest() *Queue {
	q := NewQueue("delete", 3, 1, nil, nil)
	q.SetMode(QueueModeDelete)
	q.SetState(QueueStateRunning)
	q.SetMaxKnownDepth(5)
	return q
}

func TestCheckDeleteCompletion_notAtDepth1(t *testing.T) {
	q := newDeleteQueueForTest()
	q.SetRound(3)
	q.SetCopyPass(1)

	if q.CheckDeleteCompletion(3) {
		t.Fatal("expected false before depth 1 sweep completes")
	}
	if q.GetCopyPass() != 1 {
		t.Fatalf("pass should remain 1, got %d", q.GetCopyPass())
	}
}

func TestCheckDeleteCompletion_passSwitchAtDepth1(t *testing.T) {
	q := newDeleteQueueForTest()
	q.SetRound(1)
	q.SetCopyPass(1)

	if q.CheckDeleteCompletion(1) {
		t.Fatal("pass switch should return false (phase not complete)")
	}
	if q.GetCopyPass() != 2 {
		t.Fatalf("expected pass 2 after file sweep, got %d", q.GetCopyPass())
	}
	if q.GetRound() != 5 {
		t.Fatalf("expected round reset to maxKnownDepth 5, got %d", q.GetRound())
	}
}

func TestCheckDeleteCompletion_marksCompletePass2(t *testing.T) {
	q := newDeleteQueueForTest()
	q.SetRound(1)
	q.SetCopyPass(2)

	if !q.CheckDeleteCompletion(1) {
		t.Fatal("expected phase complete at depth 1 pass 2")
	}
	if q.State() != QueueStateCompleted {
		t.Fatalf("expected queue completed, got %v", q.State())
	}
}

func TestCheckDeleteCompletion_blockedWithPending(t *testing.T) {
	q := newDeleteQueueForTest()
	q.SetRound(1)
	q.SetCopyPass(2)
	q.mu.Lock()
	q.pendingBuff = append(q.pendingBuff, &TaskBase{ID: "x", Round: 1})
	q.mu.Unlock()

	if q.CheckDeleteCompletion(1) {
		t.Fatal("expected false with pending buffer work")
	}
	if q.State() == QueueStateCompleted {
		t.Fatal("queue should not complete with pending work")
	}
}

func TestAdvanceDeleteRound_decrementsDepthSamePass(t *testing.T) {
	q := newDeleteQueueForTest()
	q.SetRound(3)
	q.SetCopyPass(1)

	q.AdvanceDeleteRound()

	if q.GetRound() != 2 {
		t.Fatalf("expected depth 2, got %d", q.GetRound())
	}
	if q.GetCopyPass() != 1 {
		t.Fatalf("expected pass 1 unchanged, got %d", q.GetCopyPass())
	}
}

func TestAdvanceDeleteRound_decrementsDepthPass2(t *testing.T) {
	q := newDeleteQueueForTest()
	q.SetRound(4)
	q.SetCopyPass(2)

	q.AdvanceDeleteRound()

	if q.GetRound() != 3 {
		t.Fatalf("expected depth 3, got %d", q.GetRound())
	}
	if q.GetCopyPass() != 2 {
		t.Fatalf("expected pass 2 unchanged, got %d", q.GetCopyPass())
	}
}

func TestAdvanceDeleteRound_atDepth1Pass2Completes(t *testing.T) {
	q := newDeleteQueueForTest()
	q.SetRound(1)
	q.SetCopyPass(2)

	q.AdvanceDeleteRound()

	if q.State() != QueueStateCompleted {
		t.Fatalf("expected completed at depth 1 pass 2 boundary, got %v", q.State())
	}
}
