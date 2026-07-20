// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func fileTask(id string, size int64) *TaskBase {
	return &TaskBase{
		ID:   id,
		Type: "upload",
		File: types.File{
			ServiceID:    id,
			DisplayName:  id,
			LocationPath: "/" + id,
			Size:         size,
			Type:         types.NodeTypeFile,
			LastUpdated:  "t0",
		},
	}
}

func folderTask(id string) *TaskBase {
	return &TaskBase{
		ID:   id,
		Type: "create-folder",
		Folder: types.Folder{
			ServiceID:    id,
			DisplayName:  id,
			LocationPath: "/" + id,
			Type:         types.NodeTypeFolder,
		},
	}
}

func queueWithWorkers(n int) *Queue {
	q := NewQueue("copy", 3, n, nil, &QueueSizing{LeaseBatchSize: 100})
	q.SetState(QueueStateRunning)
	q.pool.mu.Lock()
	for i := 0; i < n; i++ {
		h := &managedWorker{}
		h.idle.Store(true)
		q.pool.handles = append(q.pool.handles, h)
	}
	q.pool.mu.Unlock()
	return q
}

func TestLeaseGroupBudgetLargeFileSoftOverflow(t *testing.T) {
	q := queueWithWorkers(2)
	// 1×100GB + 4×1GB — first lease should take only the large file (soft overflow then stop).
	q.pendingBuff = []*TaskBase{
		fileTask("big", 100<<30),
		fileTask("s1", 1<<30),
		fileTask("s2", 1<<30),
		fileTask("s3", 1<<30),
		fileTask("s4", 1<<30),
	}
	first := q.leaseBudgetOnce(LeaseBudgetOpts{MaxCount: 1000, UseByteBudget: true}, 1000)
	if len(first) != 1 || first[0].ID != "big" {
		t.Fatalf("first lease want [big], got %v", idsOf(first))
	}
	var small []string
	for len(small) < 4 {
		batch := q.leaseBudgetOnce(LeaseBudgetOpts{MaxCount: 1000, UseByteBudget: true}, 1000)
		if len(batch) == 0 {
			break
		}
		small = append(small, idsOf(batch)...)
	}
	if len(small) != 4 {
		t.Fatalf("remaining leases want 4 small files, got %v", small)
	}
}

func TestLeaseGroupBudgetCountCeilingFolders(t *testing.T) {
	q := queueWithWorkers(2)
	for i := 0; i < 10; i++ {
		q.pendingBuff = append(q.pendingBuff, folderTask(string(rune('a'+i))))
	}
	// countCeiling = ceil(10/2)=5
	batch := q.leaseBudgetOnce(LeaseBudgetOpts{MaxCount: 1000}, 1000)
	if len(batch) != 5 {
		t.Fatalf("want countCeiling 5, got %d", len(batch))
	}
}

func idsOf(tasks []*TaskBase) []string {
	out := make([]string, len(tasks))
	for i, t := range tasks {
		if t != nil {
			out[i] = t.ID
		}
	}
	return out
}

func TestProvisionalFreezeGatesSetTargetWorkerCount(t *testing.T) {
	q := queueWithWorkers(2)
	q.spin.freeze.Store(true)
	if err := q.SetTargetWorkerCount(4); err != nil {
		t.Fatal(err)
	}
	if n := q.liveActiveWorkers(); n != 2 {
		t.Fatalf("freeze should no-op scale-up, want 2 workers got %d", n)
	}
	q.spin.freeze.Store(false)
}

func TestForceCheckoutMarksAllBusyLeases(t *testing.T) {
	q := queueWithWorkers(2)
	q.SetActiveLeaseSize("w-big", 1000)
	q.SetActiveLeaseSize("w-small", 10)
	q.SetActiveLeaseSize("w-folder-batch", 0)
	q.forceCheckoutBusyWorkers()
	if !q.forceCheckoutWorker("w-small") || !q.forceCheckoutWorker("w-big") || !q.forceCheckoutWorker("w-folder-batch") {
		t.Fatal("expected all tracked FS workers marked for force checkout (including non-file size 0)")
	}
}

func TestCancelBusyWorkerContextsCancelsNonIdleOnly(t *testing.T) {
	q := NewQueue("copy", 3, 2, nil, &QueueSizing{LeaseBatchSize: 100})
	busyCtx, busyCancel := context.WithCancel(context.Background())
	idleCtx, idleCancel := context.WithCancel(context.Background())
	defer idleCancel()

	busy := &managedWorker{cancel: busyCancel}
	busy.idle.Store(false)
	idle := &managedWorker{cancel: idleCancel}
	idle.idle.Store(true)
	q.pool.mu.Lock()
	q.pool.handles = []*managedWorker{busy, idle}
	q.pool.mu.Unlock()

	q.CancelBusyWorkerContexts()
	select {
	case <-busyCtx.Done():
	default:
		t.Fatal("expected busy worker context canceled")
	}
	select {
	case <-idleCtx.Done():
		t.Fatal("idle worker context must not be canceled")
	default:
	}
}

func TestTransferCheckpointPersistAndResumePolicy(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/xfer.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	ctx := context.Background()
	_ = database.RunWrite(ctx, func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			return w.AppenderInsert("src_nodes", []*db.NodeState{{
				ID: "n1", ServiceID: "s1", Path: "/f", ParentPath: "/", Type: "file", Size: 100, MTime: "t0", Depth: 1,
			}})
		})
	})

	q := NewQueue("copy", 3, 1, nil, nil)
	q.setDatabase(database)
	task := fileTask("n1", 100)
	task.File.LastUpdated = "t0"

	if err := q.PersistTransferCheckpoint(ctx, task, 40, "dst1"); err != nil {
		t.Fatal(err)
	}
	ckpt, err := database.GetTransferCheckpoint(ctx, "n1")
	if err != nil || ckpt == nil || ckpt.Offset != 40 {
		t.Fatalf("checkpoint want offset 40, got %+v err=%v", ckpt, err)
	}

	// Non-resumable policy → clear and restart at 0.
	off, err := prepareFileTransferResume(ctx, q, types.DefaultTransferRestartPolicy{}, task)
	if err != nil {
		t.Fatal(err)
	}
	if off != 0 {
		t.Fatalf("non-resumable want offset 0, got %d", off)
	}
	ckpt, _ = database.GetTransferCheckpoint(ctx, "n1")
	if ckpt != nil {
		t.Fatalf("checkpoint should be cleared, got %+v", ckpt)
	}

	// Resumable + matching fingerprint.
	if err := q.PersistTransferCheckpoint(ctx, task, 40, "dst1"); err != nil {
		t.Fatal(err)
	}
	pol := mockResumablePolicy{}
	off, err = prepareFileTransferResume(ctx, q, pol, task)
	if err != nil {
		t.Fatal(err)
	}
	if off != 40 {
		t.Fatalf("resumable want offset 40, got %d", off)
	}
}

type mockResumablePolicy struct{}

func (mockResumablePolicy) SupportsResumableTransfer() bool   { return true }
func (mockResumablePolicy) RequiresDeleteBeforeRestart() bool { return false }

func TestAbandonRequeueVsDBOnly(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/abandon.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	_ = database.RunWrite(ctx, func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			return w.AppenderInsert("src_nodes", []*db.NodeState{{
				ID: "n2", ServiceID: "s2", Path: "/g", ParentPath: "/", Type: "file", Size: 50, MTime: "t0", Depth: 1,
			}})
		})
	})

	q := queueWithWorkers(1)
	q.setDatabase(database)
	task := fileTask("n2", 50)
	task.File.LastUpdated = "t0"
	q.inProgress[task.ID] = task

	if err := q.AbandonTransferCheckpoint(ctx, task, 25, "dst", TransferAbandonRequeue); err != nil {
		t.Fatal(err)
	}
	if q.GetPendingCount() != 1 {
		t.Fatalf("scale-down abandon should requeue, pending=%d", q.GetPendingCount())
	}
	ckpt, _ := database.GetTransferCheckpoint(ctx, "n2")
	if ckpt == nil || ckpt.Offset != 25 {
		t.Fatalf("want checkpoint 25, got %+v", ckpt)
	}

	// Drain pending and abandon DB-only.
	q.pendingBuff = nil
	task2 := fileTask("n2", 50)
	task2.File.LastUpdated = "t0"
	q.inProgress[task2.ID] = task2
	if err := q.AbandonTransferCheckpoint(ctx, task2, 30, "dst", TransferAbandonDBOnly); err != nil {
		t.Fatal(err)
	}
	if q.GetPendingCount() != 0 {
		t.Fatalf("stop abandon must not requeue, pending=%d", q.GetPendingCount())
	}
}

func TestEnterProvisionalFreezeLifts(t *testing.T) {
	q := queueWithWorkers(1)
	q.EnterProvisionalFreeze(20 * time.Millisecond)
	if !q.PoolSizeFrozen() {
		t.Fatal("expected freeze")
	}
	time.Sleep(50 * time.Millisecond)
	if q.PoolSizeFrozen() {
		t.Fatal("expected freeze lifted after grace")
	}
}
