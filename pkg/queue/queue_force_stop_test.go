// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"testing"
	"time"

	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func TestAbandonInProgressTasksCancelsLeaseAndClearsWorkingOn(t *testing.T) {
	q := NewQueue("copy", 3, 1, nil, nil)
	q.SetMode(QueueModeCopy)
	q.Spin.AbandonDBOnly.Store(true)

	cancelled := false
	task := &TaskBase{
		ID:          "f1",
		Type:        TaskTypeCopyFile,
		CopyPass:    2,
		DisplayPath: "/a.bin",
		File:        types.File{Size: 10, DisplayName: "a.bin", Type: types.NodeTypeFile},
	}
	q.AddInProgress("f1", task)
	q.BindLeaseCancel("f1", func() { cancelled = true })

	q.AbandonInProgressTasks()
	if !cancelled {
		t.Fatal("expected lease cancel before Working-on clear")
	}
	if q.HasInProgress("f1") {
		t.Fatal("Working-on must clear immediately on soft-stop abandon")
	}
	if q.GetPendingCount() != 0 {
		t.Fatal("DB-only abandon must not requeue")
	}
}

func TestDrainSoftStopGraceClearsWithoutWaitingForever(t *testing.T) {
	// Soft-stop drain lives in migration; here we only assert abandon is immediate.
	q := NewQueue("src", 3, 1, nil, nil)
	q.SetMode(QueueModeTraversal)
	q.Spin.AbandonDBOnly.Store(true)
	task := &TaskBase{
		ID:          "d1",
		Type:        TaskTypeSrcTraversal,
		DisplayPath: "/hung",
		Folder:      types.Folder{ServiceID: "s1", DisplayName: "hung", Type: types.NodeTypeFolder},
	}
	q.AddInProgress("d1", task)
	start := time.Now()
	q.CancelBusyWorkerContexts()
	q.AbandonInProgressTasks()
	if time.Since(start) > time.Second {
		t.Fatal("abandon must not block on FS")
	}
	if q.HasInProgress("d1") {
		t.Fatal("expected clear")
	}
}
