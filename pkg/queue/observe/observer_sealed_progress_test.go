package observe

import (
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

func TestSealedProgressPercent(t *testing.T) {
	t.Parallel()
	if got := sealedProgressPercent(4, 10); got != 40 {
		t.Fatalf("got %v want 40", got)
	}
	if got := sealedProgressPercent(0, 0); got != 0 {
		t.Fatalf("empty total got %v want 0", got)
	}
	if got := sealedProgressPercent(12, 10); got != 100 {
		t.Fatalf("cap got %v want 100", got)
	}
}

func TestMemoryStatusTotals(t *testing.T) {
	q := queue.NewQueue("src", 3, 1, nil, nil)
	q.SetRoundStatsCounts(1, 10, 7, 1)
	q.SetRoundStatsCounts(2, 5, 2, 2)

	pending, failed := q.MemoryStatusTotals()
	if pending != 6 {
		t.Fatalf("pending=%d want 6", pending)
	}
	if failed != 3 {
		t.Fatalf("failed=%d want 3", failed)
	}
}

func TestPollQueueUsesMemoryWithoutDatabase(t *testing.T) {
	q := queue.NewQueue("src", 3, 1, nil, nil)
	q.SetRoundStatsCounts(3, 20, 8, 2)
	q.SetRound(3)
	o := NewQueueObserver(nil, time.Hour)
	defer o.updateTicker.Stop()

	metric := o.pollQueue("src", q)
	if metric == nil {
		t.Fatal("expected metric")
	}
	if metric.TotalPending != 12 || metric.TotalFailed != 2 {
		t.Fatalf("pending/failed=%d/%d want 12/2", metric.TotalPending, metric.TotalFailed)
	}
}

func TestEnrichCopyDeleteUsesSealedTotalsNotRemaining(t *testing.T) {
	t.Parallel()
	q := queue.NewQueue("copy", 3, 1, nil, nil)
	q.SetMode(queue.QueueModeCopy)
	q.SetWorkTotals(queue.WorkTotals{Folders: 2, Files: 8, Bytes: 1000})
	q.SetRoundStatsCounts(1, 10, 5, 1)
	metric := &ExternalQueueMetrics{
		Folders: 3, // successful creates so far
		Files:   1,
		Bytes:   400,
	}
	enrichCopyDeleteProgressFromMemory(metric, q, queue.QueueModeCopy)

	if metric.ItemsTotal != 10 {
		t.Fatalf("ItemsTotal=%d want 10 (grand sealed)", metric.ItemsTotal)
	}
	// normal mode: successful(4) + failed(1) = 5
	if metric.ItemsCompleted != 5 {
		t.Fatalf("ItemsCompleted=%d want 5", metric.ItemsCompleted)
	}
	if metric.FoldersExpected != 2 || metric.FilesExpected != 8 {
		t.Fatalf("expected folders/files=%d/%d", metric.FoldersExpected, metric.FilesExpected)
	}
	if metric.BytesTotal != 1000 {
		t.Fatalf("BytesTotal=%d want 1000", metric.BytesTotal)
	}
	if metric.BytesProgressPercent != 40 {
		t.Fatalf("bytes pct=%v want 40 (transferred only, no failures)", metric.BytesProgressPercent)
	}

	q.RecordFailedBytes(300)
	metric3 := &ExternalQueueMetrics{Folders: 3, Files: 1, Bytes: 400}
	enrichCopyDeleteProgressFromMemory(metric3, q, queue.QueueModeCopy)
	// touched bytes = 400 transferred + 300 failed = 700 → 70%
	if metric3.BytesProgressPercent != 70 {
		t.Fatalf("bytes pct with failures=%v want 70", metric3.BytesProgressPercent)
	}
	if metric3.BytesFailed != 300 || metric3.BytesFailedPercent != 30 {
		t.Fatalf("bytes failed=%d pct=%v want 300/30", metric3.BytesFailed, metric3.BytesFailedPercent)
	}
	if metric3.ItemsFailedPercent != 10 {
		t.Fatalf("items failed pct=%v want 10", metric3.ItemsFailedPercent)
	}

	// Retry mode: successful only vs same grand total (not 0 of remaining).
	metric2 := &ExternalQueueMetrics{Folders: 3, Files: 1, Bytes: 400}
	enrichCopyDeleteProgressFromMemory(metric2, q, queue.QueueModeCopyRetry)
	if metric2.ItemsTotal != 10 {
		t.Fatalf("retry ItemsTotal=%d want 10", metric2.ItemsTotal)
	}
	if metric2.ItemsCompleted != 4 {
		t.Fatalf("retry ItemsCompleted=%d want 4", metric2.ItemsCompleted)
	}
	if metric2.ItemsProgressPercent != 40 {
		t.Fatalf("retry pct=%v want 40", metric2.ItemsProgressPercent)
	}
	if metric2.BytesProgressPercent != 40 {
		t.Fatalf("retry bytes pct=%v want 40 (transferred only)", metric2.BytesProgressPercent)
	}
	if metric2.BytesFailedPercent != 0 || metric2.ItemsFailedPercent != 0 {
		t.Fatalf("retry failed percents should be 0, got bytes=%v items=%v", metric2.BytesFailedPercent, metric2.ItemsFailedPercent)
	}
}
