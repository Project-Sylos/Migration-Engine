// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package mode

import (
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func TestFailTraversalTask_nonRetryableSkipsRetries(t *testing.T) {
	q := queue.NewQueue("src", 3, 1, nil, nil)
	task := &queue.TaskBase{
		ID:        "n1",
		Round:     0,
		Attempts:  0,
		LastError: "failed to list children of /proc: fs: path blocked from migration: /proc",
		Folder:    types.Folder{ServiceID: "/proc", LocationPath: "/proc", Type: types.NodeTypeFolder},
	}
	q.AddInProgress(task.ID, task)

	q.FinishModeTask(queue.ModeTraversal, task, time.Millisecond, false)

	if task.Status != "failed" {
		t.Fatalf("status=%q want failed", task.Status)
	}
	if q.GetPendingCount() != 0 {
		t.Fatalf("pending=%d want 0 (no retry)", q.GetPendingCount())
	}
	if q.InProgressCount() != 0 {
		t.Fatalf("inProgress=%d want 0", q.InProgressCount())
	}
}
