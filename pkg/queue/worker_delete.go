// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// DeleteWorker removes SRC nodes via FS DeleteNode during the delete phase.
type DeleteWorker struct {
	id          string
	queue       *Queue
	srcAdapter  types.FSAdapter
	shutdownCtx context.Context
	workerCtx   context.Context
	idle        *atomic.Bool
	retire      *atomic.Bool
}

func NewDeleteWorker(
	id string,
	q *Queue,
	srcAdapter types.FSAdapter,
	shutdownCtx, workerCtx context.Context,
	idle, retire *atomic.Bool,
) *DeleteWorker {
	return &DeleteWorker{
		id:          id,
		queue:       q,
		srcAdapter:  srcAdapter,
		shutdownCtx: shutdownCtx,
		workerCtx:   workerCtx,
		idle:        idle,
		retire:      retire,
	}
}

func (w *DeleteWorker) setBusy() {
	if w.idle != nil {
		w.idle.Store(false)
	}
}

func (w *DeleteWorker) setIdle() {
	if w.idle != nil {
		w.idle.Store(true)
	}
}

func (w *DeleteWorker) shouldRetire() bool {
	return w.retire != nil && w.retire.Load()
}

func (w *DeleteWorker) Run() {
	for {
		select {
		case <-w.workerCtx.Done():
			return
		case <-w.shutdownCtx.Done():
			return
		default:
		}
		if w.shouldRetire() {
			return
		}
		task := w.queue.Lease()
		if task == nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		w.setBusy()
		err := w.execute(task)
		w.setIdle()
		if err != nil {
			task.LastError = err.Error()
			task.WorkerResult = "error"
			w.queue.ReportTaskResult(task, TaskExecutionResultFailed)
		} else {
			task.WorkerResult = "success"
			w.queue.ReportTaskResult(task, TaskExecutionResultSuccessful)
		}
		if w.shouldRetire() {
			return
		}
	}
}

func (w *DeleteWorker) execute(task *TaskBase) error {
	ctx := w.workerCtx
	serviceID := task.Identifier()
	nodeType := types.NodeTypeFile
	if task.IsFolder() {
		nodeType = types.NodeTypeFolder
	}
	if serviceID == "" {
		return fmt.Errorf("empty service id for delete task %s", task.LocationPath())
	}
	return w.srcAdapter.DeleteNode(ctx, serviceID, nodeType)
}
