// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package worker

import (
	"io"
	"strings"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func TestTransferProgressReaderReportsBytes(t *testing.T) {
	q := queue.NewQueue("copy", 3, 1, nil, nil)
	task := &queue.TaskBase{
		ID:   "file-2",
		Type: queue.TaskTypeCopyFile,
		File: types.File{Size: 11, Type: types.NodeTypeFile},
	}
	q.AddInProgress(task.ID, task)

	r := newTransferProgressReader(q, nil, task, io.NopCloser(strings.NewReader("hello world")), 0)
	buf := make([]byte, 5)
	n, err := r.Read(buf)
	if err != nil || n != 5 {
		t.Fatalf("first read: n=%d err=%v", n, err)
	}
	if got := q.GetLiveBytesTransferredTotal(); got != 5 {
		t.Fatalf("after first read live=%d want 5", got)
	}
	rest, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 6 {
		t.Fatalf("remaining bytes = %d, want 6", len(rest))
	}
	if got := q.GetLiveBytesTransferredTotal(); got != 11 {
		t.Fatalf("after drain live=%d want 11", got)
	}
	if task.BytesTransferred != 11 {
		t.Fatalf("task.BytesTransferred=%d want 11", task.BytesTransferred)
	}
}

func TestTransferProgressReaderResumeOffset(t *testing.T) {
	q := queue.NewQueue("copy", 3, 1, nil, nil)
	task := &queue.TaskBase{ID: "file-3", Type: queue.TaskTypeCopyFile}
	q.AddInProgress(task.ID, task)

	r := newTransferProgressReader(q, nil, task, io.NopCloser(strings.NewReader("xyz")), 100)
	if got := q.GetLiveBytesTransferredTotal(); got != 100 {
		t.Fatalf("resume start live=%d want 100", got)
	}
	_, _ = io.ReadAll(r)
	_ = r.Close()
	if got := q.GetLiveBytesTransferredTotal(); got != 103 {
		t.Fatalf("after resume read live=%d want 103", got)
	}
}
