// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
)

// AbandonInProgressTasks unleases all in-flight tasks. File copy tasks with progress
// persist a transfer checkpoint. When abandonModeForStop is set (stop/soft-suspend),
// tasks are not requeued (DB-only). Otherwise they return to pendingBuff (scale-down).
func (q *Queue) AbandonInProgressTasks() {
	if q == nil {
		return
	}
	dbOnly := q.Spin.AbandonDBOnly.Load()
	q.mu.Lock()
	tasks := make([]*TaskBase, 0, len(q.inProgress))
	for _, f := range q.inProgress {
		if f.task != nil {
			tasks = append(tasks, f.task)
		}
	}
	q.mu.Unlock()

	ctx := context.Background()
	for _, task := range tasks {
		if task == nil {
			continue
		}
		nodeID := task.ID
		offset := task.BytesTransferred
		if offset <= 0 && task.XferOffset > 0 {
			offset = task.XferOffset
		}
		dstRef := task.XferDstRef
		task.Locked = false
		q.RemoveInProgress(nodeID)
		if task.IsFile() && offset > 0 {
			_ = q.PersistTransferCheckpointToken(ctx, task, offset, dstRef, "")
		}
		if dbOnly {
			continue
		}
		if !q.Add(task) {
			if logservice.LS != nil {
				_ = logservice.LS.Log("error",
					fmt.Sprintf("force-stop re-enqueue rejected for %s (id=%s) - task lost", task.LocationPath(), nodeID),
					"queue", q.name, q.name)
			}
		}
	}
}
