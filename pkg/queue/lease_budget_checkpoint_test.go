// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/checkpoint"
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
	q.Spin.Freeze.Store(true)
	if err := q.SetTargetWorkerCount(4); err != nil {
		t.Fatal(err)
	}
	if n := q.liveActiveWorkers(); n != 2 {
		t.Fatalf("freeze should no-op scale-up, want 2 workers got %d", n)
	}
	q.Spin.Freeze.Store(false)
}

func TestForceCheckoutMarksAllBusyLeases(t *testing.T) {
	q := queueWithWorkers(2)
	q.SetActiveLeaseSize("w-big", 1000)
	q.SetActiveLeaseSize("w-small", 10)
	q.SetActiveLeaseSize("w-folder-batch", 0)
	q.forceCheckoutBusyWorkers()
	if !q.ForceCheckoutWorker("w-small") || !q.ForceCheckoutWorker("w-big") || !q.ForceCheckoutWorker("w-folder-batch") {
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
	if err := database.SeedDiscoveredNodes([]db.InsertOperation{{
		QueueType: "SRC", Level: 1, Status: db.StatusSuccessful,
		State: &db.NodeState{
			ID: "n1", ServiceID: "s1", Path: "/f", ParentPath: "/", Name: "f",
			Type: db.NodeTypeFile, Size: 100, MTime: "t0", Depth: 1,
			TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
		},
	}}); err != nil {
		t.Fatal(err)
	}

	q := NewQueue("copy", 3, 1, nil, nil)
	q.SetDatabase(database)
	task := fileTask("n1", 100)
	task.File.LastUpdated = "t0"

	if err := q.PersistTransferCheckpointToken(ctx, task, 40, "dst1", ""); err != nil {
		t.Fatal(err)
	}
	ckpt, err := checkpoint.GetTransferCheckpoint(database, ctx, "n1")
	if err != nil || ckpt == nil || ckpt.Offset != 40 {
		t.Fatalf("checkpoint want offset 40, got %+v err=%v", ckpt, err)
	}

	// Non-resumable policy → delete attempt + full clear, restart at 0.
	plan, err := PrepareFileTransferResume(ctx, q, types.DefaultTransferRestartPolicy{}, task)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ResumeOffset != 0 {
		t.Fatalf("non-resumable want offset 0, got %d", plan.ResumeOffset)
	}
	ckpt, _ = checkpoint.GetTransferCheckpoint(database, ctx, "n1")
	if ckpt != nil {
		t.Fatalf("checkpoint should be cleared after non-resumable restart, got %+v", ckpt)
	}

	// Resumable + matching fingerprint.
	if err := q.PersistTransferCheckpointToken(ctx, task, 40, "dst1", ""); err != nil {
		t.Fatal(err)
	}
	pol := mockResumablePolicy{}
	plan, err = PrepareFileTransferResume(ctx, q, pol, task)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ResumeOffset != 40 {
		t.Fatalf("resumable want offset 40, got %d", plan.ResumeOffset)
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
	if err := database.SeedDiscoveredNodes([]db.InsertOperation{{
		QueueType: "SRC", Level: 1, Status: db.StatusSuccessful,
		State: &db.NodeState{
			ID: "n2", ServiceID: "s2", Path: "/g", ParentPath: "/", Name: "g",
			Type: db.NodeTypeFile, Size: 50, MTime: "t0", Depth: 1,
			TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
		},
	}}); err != nil {
		t.Fatal(err)
	}

	q := queueWithWorkers(1)
	q.SetDatabase(database)
	task := fileTask("n2", 50)
	task.File.LastUpdated = "t0"
	q.AddInProgress(task.ID, task)

	if err := q.AbandonTransferCheckpointToken(ctx, task, 25, "dst", "", TransferAbandonRequeue); err != nil {
		t.Fatal(err)
	}
	if q.GetPendingCount() != 1 {
		t.Fatalf("scale-down abandon should requeue, pending=%d", q.GetPendingCount())
	}
	ckpt, _ := checkpoint.GetTransferCheckpoint(database, ctx, "n2")
	if ckpt == nil || ckpt.Offset != 25 {
		t.Fatalf("want checkpoint 25, got %+v", ckpt)
	}

	// Drain pending and abandon DB-only.
	q.pendingBuff = nil
	task2 := fileTask("n2", 50)
	task2.File.LastUpdated = "t0"
	q.AddInProgress(task2.ID, task2)
	if err := q.AbandonTransferCheckpointToken(ctx, task2, 30, "dst", "", TransferAbandonDBOnly); err != nil {
		t.Fatal(err)
	}
	if q.GetPendingCount() != 0 {
		t.Fatalf("stop abandon must not requeue, pending=%d", q.GetPendingCount())
	}
}

func TestEnterProvisionalFreezeLifts(t *testing.T) {
	q := queueWithWorkers(1)
	q.EnterProvisionalFreeze(20 * time.Millisecond)
	if !q.Spin.Freeze.Load() {
		t.Fatal("expected freeze")
	}
	time.Sleep(50 * time.Millisecond)
	if q.Spin.Freeze.Load() {
		t.Fatal("expected freeze lifted after grace")
	}
}

func TestScaleDownPrefersIdleThenSmallestLease(t *testing.T) {
	q := NewQueue("copy", 3, 3, nil, &QueueSizing{LeaseBatchSize: 100})
	q.SetState(QueueStateRunning)
	shutdown, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.SetShutdownContext(shutdown)

	makeHandle := func(id string, idle bool) *managedWorker {
		_, c := context.WithCancel(shutdown)
		h := &managedWorker{id: id, cancel: c}
		h.idle.Store(idle)
		return h
	}
	idle := makeHandle("copy-worker-0", true)
	big := makeHandle("copy-worker-1", false)
	small := makeHandle("copy-worker-2", false)
	q.pool.mu.Lock()
	q.pool.handles = []*managedWorker{idle, big, small}
	q.pool.nextID = 3
	q.workers = make([]Worker, 3)
	q.pool.mu.Unlock()
	q.SetActiveLeaseSize("copy-worker-1", 1000)
	q.SetActiveLeaseSize("copy-worker-2", 10)

	if err := q.SetTargetWorkerCount(1); err != nil {
		t.Fatal(err)
	}
	if n := q.liveActiveWorkers(); n != 1 {
		t.Fatalf("want 1 active worker, got %d", n)
	}
	q.pool.mu.Lock()
	remaining := q.pool.handles[0].id
	q.pool.mu.Unlock()
	if remaining != "copy-worker-1" {
		t.Fatalf("want largest busy kept, got %s", remaining)
	}
	if !idle.retire.Load() || !small.retire.Load() {
		t.Fatal("idle and smallest busy should be marked retire")
	}
	if !q.Spin.Freeze.Load() {
		t.Fatal("busy deferred retiree should enter provisional freeze")
	}
	q.Spin.graceMu.Lock()
	_, deferred := q.Spin.deferredRetire["copy-worker-2"]
	q.Spin.graceMu.Unlock()
	if !deferred {
		t.Fatal("smallest busy should be deferred retiree")
	}
}

func TestTryClaimIdleRetirementCancelsDeferred(t *testing.T) {
	q := queueWithWorkers(1)
	ctx, cancel := context.WithCancel(context.Background())
	h := &managedWorker{id: "w1", cancel: cancel}
	h.retire.Store(true)
	q.trackDeferredRetiree("w1", h)
	q.Spin.Freeze.Store(true)

	q.TryClaimIdleRetirement("w1")
	select {
	case <-ctx.Done():
	default:
		t.Fatal("expected deferred retiree cancelled on idle claim")
	}
	if q.Spin.Freeze.Load() {
		t.Fatal("freeze should lift when no deferred remain")
	}
}

func TestForceCheckoutDeferredOnly(t *testing.T) {
	q := queueWithWorkers(1)
	ctx, cancel := context.WithCancel(context.Background())
	h := &managedWorker{id: "defer-me", cancel: cancel}
	q.trackDeferredRetiree("defer-me", h)
	q.SetActiveLeaseSize("defer-me", 50)
	q.SetActiveLeaseSize("other-busy", 5)

	q.forceCheckoutDeferredRetirees()
	if !q.ForceCheckoutWorker("defer-me") {
		t.Fatal("deferred retiree should be force-checked out")
	}
	if q.ForceCheckoutWorker("other-busy") {
		t.Fatal("non-deferred busy worker must not be force-checked out")
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("deferred retiree context should be cancelled")
	}
}

func TestStaleLeaseEpochDropsCompletion(t *testing.T) {
	q := NewQueue("copy", 3, 1, nil, nil)
	q.SetRoundStatsCounts(1, 1, 0, 0)

	task1 := fileTask("n-epoch", 10)
	task1.Round = 1
	q.AddInProgress(task1.ID, task1)
	q.BindLeaseOwner(task1, "worker-a")
	epochA := task1.LeaseEpoch
	ownerA := task1.LeaseOwner

	// Second lease overwrites ownership (simulates the old bulk-requeue race).
	task2 := fileTask("n-epoch", 10)
	task2.Round = 1
	q.AddInProgress(task2.ID, task2)
	q.BindLeaseOwner(task2, "worker-b")

	stale := *task1
	stale.LeaseEpoch = epochA
	stale.LeaseOwner = ownerA
	if q.LeaseMatches(&stale) {
		t.Fatal("stale owner+epoch must not match current lease")
	}
	if !q.LeaseMatches(task2) {
		t.Fatal("current owner must match")
	}

	// finishTask drops stale results before any counter credit.
	q.finishTask(&stale, time.Millisecond, true)
	if stats := q.GetRoundStats(1); stats == nil || stats.Completed != 0 {
		t.Fatalf("stale owner must not credit Completed, got %+v", stats)
	}
	// Credit only when LeaseMatches (mode hooks may be nil in this package).
	if q.LeaseMatches(task2) {
		q.IncrementRoundStatsCompleted(1)
		q.RemoveInProgress(task2.ID)
	}
	if stats := q.GetRoundStats(1); stats == nil || stats.Completed != 1 {
		t.Fatalf("current owner should credit once, got %+v", stats)
	}
}

func TestAttemptMarkerPersistsAtOffsetZero(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/attempt.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	if err := database.SeedDiscoveredNodes([]db.InsertOperation{{
		QueueType: "SRC", Level: 1, Status: db.StatusSuccessful,
		State: &db.NodeState{
			ID: "n-attempt", ServiceID: "s", Path: "/a", ParentPath: "/", Name: "a",
			Type: db.NodeTypeFile, Size: 10, MTime: "t0", Depth: 1,
			TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
		},
	}}); err != nil {
		t.Fatal(err)
	}
	q := NewQueue("copy", 3, 1, nil, nil)
	q.SetDatabase(database)
	task := fileTask("n-attempt", 10)
	task.File.LastUpdated = "t0"
	if err := q.PersistTransferCheckpointToken(ctx, task, 0, "dst-ref", ""); err != nil {
		t.Fatal(err)
	}
	ckpt, err := checkpoint.GetTransferCheckpoint(database, ctx, "n-attempt")
	if err != nil || ckpt == nil || ckpt.DstRef != "dst-ref" {
		t.Fatalf("want attempt marker dst-ref, got %+v err=%v", ckpt, err)
	}
	if !HasCopyAttempt(task) {
		t.Fatal("task should report HasCopyAttempt")
	}
}

func TestCompletedNeverExceedsExpectedAfterStaleDrop(t *testing.T) {
	// Characterization: Completed must stay <= Expected when stale leases report success.
	q := NewQueue("copy", 3, 1, nil, nil)
	q.SetRoundStatsCounts(2, 2, 0, 0)

	for i := 0; i < 5; i++ {
		task := fileTask("n-bounds", 1)
		task.Round = 2
		q.AddInProgress(task.ID, task)
		q.BindLeaseOwner(task, "w")
		stale := *task
		q.AddInProgress(task.ID, task)
		q.BindLeaseOwner(task, "w2")
		if q.LeaseMatches(&stale) {
			q.IncrementRoundStatsCompleted(2)
		}
	}
	task := fileTask("n-bounds", 1)
	task.Round = 2
	q.AddInProgress(task.ID, task)
	q.BindLeaseOwner(task, "final")
	if q.LeaseMatches(task) {
		q.IncrementRoundStatsCompleted(2)
	}

	stats := q.GetRoundStats(2)
	if stats == nil {
		t.Fatal("missing round stats")
	}
	if stats.Completed > stats.Expected {
		t.Fatalf("Completed (%d) > Expected (%d)", stats.Completed, stats.Expected)
	}
	if stats.Completed != 1 {
		t.Fatalf("want exactly 1 completion credited, got %d", stats.Completed)
	}
}
