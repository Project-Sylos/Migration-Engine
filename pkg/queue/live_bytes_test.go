// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"testing"

	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func TestLiveBytesTransferredOverlay(t *testing.T) {
	q := NewQueue("copy", 3, 1, nil, nil)
	q.SetMode(QueueModeCopy)
	q.SeedCopyCounters(0, 0, 1000, 0)

	task := &TaskBase{
		ID:       "file-1",
		Round:    1,
		CopyPass: 2,
		Type:     TaskTypeCopyFile,
		File:     types.File{Size: 500, Type: types.NodeTypeFile, DisplayName: "a.txt"},
	}
	q.AddInProgress(task.ID, task)

	if got := q.GetLiveBytesTransferredTotal(); got != 1000 {
		t.Fatalf("live before progress = %d, want 1000", got)
	}

	q.ReportTaskBytesTransferred(task, 200)
	if got := q.GetLiveBytesTransferredTotal(); got != 1200 {
		t.Fatalf("live mid-flight = %d, want 1200", got)
	}
	if got := q.GetBytesTransferredTotal(); got != 1000 {
		t.Fatalf("completed total should stay 1000, got %d", got)
	}

	q.RecordTerminalProgress(TerminalProgressCopied, task.ID, false, true, task.BytesTransferred)

	if got := q.GetLiveBytesTransferredTotal(); got != 1200 {
		t.Fatalf("live after complete = %d, want 1200 (continuous)", got)
	}
	if got := q.GetBytesTransferredTotal(); got != 1200 {
		t.Fatalf("completed total after complete = %d, want 1200", got)
	}
}

func TestReportTaskBytesTransferredDoesNotNeedInProgressScan(t *testing.T) {
	q := NewQueue("copy", 3, 1, nil, nil)
	task := &TaskBase{ID: "file-x", Type: TaskTypeCopyFile}
	q.ReportTaskBytesTransferred(task, 64<<10)
	q.ReportTaskBytesTransferred(task, 128<<10)
	if got := q.GetLiveBytesTransferredTotal(); got != 128<<10 {
		t.Fatalf("live=%d want %d", got, 128<<10)
	}
	q.markLiveBytesDone(task)
	q.ReportTaskBytesTransferred(task, 256<<10)
	if got := q.GetLiveBytesTransferredTotal(); got != 128<<10 {
		t.Fatalf("after done, live=%d want %d (snapshot stays, further reports no-op)", got, 128<<10)
	}
}

func TestLiveBytesRemainderOnCompleteWithoutStream(t *testing.T) {
	q := NewQueue("copy", 3, 1, nil, nil)
	q.SetMode(QueueModeCopy)
	task := &TaskBase{
		ID:   "empty-or-delete",
		Type: TaskTypeCopyFile,
		File: types.File{Size: 500, Type: types.NodeTypeFile},
	}
	q.AddInProgress(task.ID, task)
	q.RecordTerminalProgress(TerminalProgressCopied, task.ID, false, true, 500)
	if got := q.GetLiveBytesTransferredTotal(); got != 500 {
		t.Fatalf("live remainder=%d want 500", got)
	}
	if got := q.GetBytesTransferredTotal(); got != 500 {
		t.Fatalf("completed=%d want 500", got)
	}
}
