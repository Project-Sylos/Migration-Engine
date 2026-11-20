// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package worker

import (
	"context"
	"fmt"
	"sync/atomic"

	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"

	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// DeleteWorker removes SRC nodes via FS DeleteNode during the delete phase.
type DeleteWorker struct {
	workerBase
	srcAdapter types.FSAdapter
}

func NewDeleteWorker(
	id string,
	q *queue.Queue,
	srcAdapter types.FSAdapter,
	shutdownCtx, workerCtx context.Context,
	idle, retire *atomic.Bool,
) *DeleteWorker {
	w := &DeleteWorker{
		workerBase: newWorkerBase(id, q, shutdownCtx, workerCtx, idle, retire),
		srcAdapter: srcAdapter,
	}
	w.loop = loopSpec{
		label:     "Delete worker",
		queueName: "delete",
		execute:   w.execute,
		batchTurn: w.deleteBatchTurn,
	}
	return w
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
		return fmt.Errorf("empty service id for delete task %s", task.DisplayPath)
	}
	if logservice.LS != nil {
		_ = logservice.LS.LogStructured("trace",
			fmt.Sprintf("delete start: path=%s", task.DisplayPath),
			"worker", w.id,
			logservice.LogFields{Action: "delete", NodeID: task.ID, Queue: "delete"})
	}

	// DeleteNode often ignores cancel; run async so the progress watchdog can still abort.
	err := awaitErr(w.queue, ctx, parent, deleteStallTimeout, "delete", task.DisplayPath, func() error {
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
