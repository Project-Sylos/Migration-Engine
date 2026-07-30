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
	q.mu.Lock()
	q.inProgress[task.ID] = task
	q.mu.Unlock()

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

	// Simulate CompleteCopyTask credit + remove under one lock (no double-count).
	q.mu.Lock()
	q.bytesTransferredTotal += task.BytesTransferred
	q.filesCreatedTotal++
	delete(q.inProgress, task.ID)
	q.mu.Unlock()

	if got := q.GetLiveBytesTransferredTotal(); got != 1200 {
		t.Fatalf("live after complete = %d, want 1200 (continuous)", got)
	}
	if got := q.GetBytesTransferredTotal(); got != 1200 {
		t.Fatalf("completed total after complete = %d, want 1200", got)
	}
}
