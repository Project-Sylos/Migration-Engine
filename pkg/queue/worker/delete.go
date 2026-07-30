// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package worker

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"

	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// DeleteWorker removes SRC nodes via FS DeleteNode during the delete phase.
type DeleteWorker struct {
	id          string
	queue       *queue.Queue
	srcAdapter  types.FSAdapter
	shutdownCtx context.Context
	workerCtx   context.Context
	idle        *atomic.Bool
	retire      *atomic.Bool
}

func NewDeleteWorker(
	id string,
	q *queue.Queue,
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
		if w.useDeleteBatchLease() {
			w.runDeleteBatchTurn()
			if w.shouldRetire() {
				return
			}
			continue
		}

		task := w.queue.Lease()
		if task == nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		setWorkerBusy(w.idle)
		err := w.execute(task)
		setWorkerIdle(w.idle)
		if errors.Is(err, errTransferAbandoned) {
			if w.queue.HasInProgress(task.ID) {
				task.Locked = false
				w.queue.RemoveInProgress(task.ID)
				if !w.queue.Spin.AbandonDBOnly.Load() {
					_ = w.queue.Add(task)
				}
			}
			if w.shouldRetire() {
				return
			}
			continue
		}
		if err != nil {
			task.LastError = err.Error()
			task.WorkerResult = "error"
			w.queue.ReportTaskResult(task, queue.TaskExecutionResultFailed)
		} else {
			task.WorkerResult = "success"
			w.queue.ReportTaskResult(task, queue.TaskExecutionResultSuccessful)
		}
		if w.shouldRetire() {
			return
		}
	}
}

func (w *DeleteWorker) execute(task *queue.TaskBase) error {
	parent := fsOpContext(w.workerCtx, w.shutdownCtx)
	wd, ctx := observe.NewProgressWatchdog(parent, deleteStallTimeout, progressStallSuppress(w.queue))
	defer wd.Stop()

	w.queue.SetActiveLeaseSize(w.id, 0)
	defer w.queue.ClearActiveLeaseSize(w.id)
	serviceID := task.Identifier()
	nodeType := types.NodeTypeFile
	if task.IsFolder() {
		nodeType = types.NodeTypeFolder
	}
	if serviceID == "" {
		return fmt.Errorf("empty service id for delete task %s", task.LocationPath())
	}

	// DeleteNode often ignores cancel; run async so the progress watchdog can still abort.
	err := awaitErr(ctx, parent, deleteStallTimeout, "delete", task.LocationPath(), func() error {
		return w.srcAdapter.DeleteNode(ctx, serviceID, nodeType)
	})
	if isForceCheckoutAbort(err, w.shouldRetire(), w.queue.ForceCheckoutWorker(w.id)) {
		return errTransferAbandoned
	}
	if err == nil {
		wd.Beat()
	}
	return err
}
