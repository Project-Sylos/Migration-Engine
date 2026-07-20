// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"testing"
	"time"

	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func TestEnsureLeaseBatchSizeAtLeast(t *testing.T) {
	q := NewQueue("copy", 3, 1, nil, nil)
	if q.EffectiveLeaseBatchSize() != defaultLeaseBatchSize {
		t.Fatalf("default=%d", q.EffectiveLeaseBatchSize())
	}
	q.EnsureLeaseBatchSizeAtLeast(types.DefaultCreateFolderBatchPullSize)
	if q.EffectiveLeaseBatchSize() != types.DefaultCreateFolderBatchPullSize {
		t.Fatalf("got %d", q.EffectiveLeaseBatchSize())
	}
	q.EnsureLeaseBatchSizeAtLeast(1000)
	if q.EffectiveLeaseBatchSize() != types.DefaultCreateFolderBatchPullSize {
		t.Fatalf("should not shrink, got %d", q.EffectiveLeaseBatchSize())
	}
}

func TestReportTaskOutcomeRateLimited(t *testing.T) {
	q := NewQueue("copy", 3, 1, nil, nil)
	q.SetMode(QueueModeCopy)
	q.SetState(QueueStateRunning)
	task := &TaskBase{ID: "t1", Type: TaskTypeCopyFolder, Round: 1, LeaseTime: time.Now()}
	q.addInProgress(task.ID, task)
	reportTaskOutcome(q, task, &rateLimitErr{})
	if task.WorkerResult != "rate_limited" {
		t.Fatalf("WorkerResult=%q", task.WorkerResult)
	}
	if q.InProgressCount() != 0 {
		t.Fatalf("inProgress=%d want 0 (yielded)", q.InProgressCount())
	}
	if q.GetPendingCount() != 1 {
		t.Fatalf("pending=%d want 1 after rate-limit yield", q.GetPendingCount())
	}
}

type rateLimitErr struct{}

func (e *rateLimitErr) Error() string { return "HTTP 429 too many requests" }

func TestCreateFolderBatchFromHelper(t *testing.T) {
	if _, ok := types.CreateFolderBatchFrom(nil); ok {
		t.Fatal("nil adapter should be false")
	}
	stub := &stubFolderBatch{max: 100}
	got, ok := types.CreateFolderBatchFrom(stub)
	if !ok || got.CreateFolderBatchMax() != 100 {
		t.Fatalf("ok=%v max=%d", ok, got.CreateFolderBatchMax())
	}
}

// stubFolderBatch is only used to satisfy CreateFolderBatchFrom type assert in tests.
type stubFolderBatch struct {
	max int
}

func (s *stubFolderBatch) CreateFolderBatchMax() int { return s.max }

func (s *stubFolderBatch) CreateFolderBatch(_ context.Context, _ []types.CreateFolderBatchItem) ([]types.CreateFolderBatchEntryResult, error) {
	return nil, nil
}
