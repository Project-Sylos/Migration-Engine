// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
)

// AbandonInProgressTasks unleases all in-flight tasks and returns them to the pending buffer
// without incrementing retry attempts. Used on force shutdown when workers cannot finish promptly.
func (q *Queue) AbandonInProgressTasks() {
	if q == nil {
		return
	}
	q.mu.Lock()
	tasks := make([]*TaskBase, 0, len(q.inProgress))
	for _, task := range q.inProgress {
		tasks = append(tasks, task)
	}
	q.mu.Unlock()

	for _, task := range tasks {
		if task == nil {
			continue
		}
		nodeID := task.ID
		task.Locked = false
		q.removeInProgress(nodeID)
		if !q.Add(task) {
			if logservice.LS != nil {
				_ = logservice.LS.Log("error",
					fmt.Sprintf("force-stop re-enqueue rejected for %s (id=%s) - task lost", task.LocationPath(), nodeID),
					"queue", q.name, q.name)
			}
		}
	}
}
