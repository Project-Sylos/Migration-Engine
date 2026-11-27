// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
)

type abandonLease struct {
	task   *TaskBase
	cancel context.CancelFunc
}

// AbandonInProgressTasks unleases all in-flight tasks. Lease contexts are cancelled
// first so FS awaits can return; Working-on clears before checkpoint I/O.
// File copy tasks with progress persist a transfer checkpoint asynchronously so a
// busy seal/DB path cannot hold soft-stop progress on "draining".
// When AbandonDBOnly is set (stop/soft-suspend), tasks are not requeued.
// Otherwise they return to pendingBuff (scale-down).
func (q *Queue) AbandonInProgressTasks() {
	if q == nil {
		return
	}
	dbOnly := q.Spin.AbandonDBOnly.Load()
	q.mu.Lock()
	leases := make([]abandonLease, 0, len(q.inProgress))
	for _, f := range q.inProgress {
		if f.task != nil {
			leases = append(leases, abandonLease{task: f.task, cancel: f.cancel})
		}
	}
	q.mu.Unlock()

	for _, lease := range leases {
		if lease.cancel != nil {
			lease.cancel()
		}
	}

	ctx := context.Background()
	for _, lease := range leases {
		task := lease.task
		if task == nil {
			continue
		}
		nodeID := task.ID
		offset := task.BytesTransferred
		if offset <= 0 && task.XferOffset > 0 {
			offset = task.XferOffset
		}
		dstRef := task.XferDstRef
		token := task.XferResumeToken
		task.Locked = false
		q.RemoveInProgress(nodeID)

		if task.IsFile() && (offset > 0 || dstRef != "" || token != "") {
			t := task
			off, ref, tok := offset, dstRef, token
			go func() {
				_ = q.PersistTransferCheckpointToken(ctx, t, off, ref, tok)
			}()
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
