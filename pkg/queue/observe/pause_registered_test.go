// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package observe

import (
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

func TestPauseRegisteredQueuesClearsPendingAndPauses(t *testing.T) {
	o := NewQueueObserver(nil, time.Hour)
	q := queue.NewQueue("src", 3, 1, nil, nil)
	q.SetState(queue.QueueStateRunning)
	if !q.Add(&queue.TaskBase{ID: "p1", Round: 1, Type: queue.TaskTypeSrcTraversal}) {
		t.Fatal("enqueue failed")
	}
	leased := &queue.TaskBase{ID: "leased", Round: 1, Type: queue.TaskTypeSrcTraversal}
	q.AddInProgress(leased.ID, leased)
	o.RegisterQueue("src", q)

	inFlight := o.PauseRegisteredQueues()
	if inFlight != 1 {
		t.Fatalf("inProgress = %d, want 1", inFlight)
	}
	if q.State() != queue.QueueStatePaused {
		t.Fatalf("state = %s, want paused", q.State())
	}
	if q.GetPendingCount() != 0 {
		t.Fatalf("pending = %d, want 0", q.GetPendingCount())
	}
}

func TestAbandonRegisteredQueuesDoesNotRequeue(t *testing.T) {
	o := NewQueueObserver(nil, time.Hour)
	q := queue.NewQueue("src", 3, 1, nil, nil)
	q.SetState(queue.QueueStateRunning)
	if !q.Add(&queue.TaskBase{ID: "p1", Round: 1, Type: queue.TaskTypeSrcTraversal}) {
		t.Fatal("enqueue failed")
	}
	leased := &queue.TaskBase{ID: "leased", Round: 1, Type: queue.TaskTypeSrcTraversal}
	q.AddInProgress(leased.ID, leased)
	o.RegisterQueue("src", q)

	o.AbandonRegisteredQueues()
	if q.InProgressCount() != 0 {
		t.Fatalf("inProgress = %d, want 0", q.InProgressCount())
	}
	if q.GetPendingCount() != 0 {
		t.Fatalf("pending after DB-only abandon = %d, want 0 (must not requeue)", q.GetPendingCount())
	}
	if !q.Spin.AbandonDBOnly.Load() {
		t.Fatal("expected AbandonDBOnly set for force abandon")
	}
}
