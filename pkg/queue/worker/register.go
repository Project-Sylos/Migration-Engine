// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package worker

import (
	"context"
	"sync/atomic"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func init() {
	queue.RegisterWorkerHooks(queue.WorkerHooks{
		NewCopyWorker: func(
			id string,
			q *queue.Queue,
			srcAdapter types.FSAdapter,
			dstAdapter types.FSAdapter,
			shutdownCtx context.Context,
			workerCtx context.Context,
			idle *atomic.Bool,
			retire *atomic.Bool,
		) queue.Worker {
			return NewCopyWorker(id, q, srcAdapter, dstAdapter, shutdownCtx, workerCtx, idle, retire)
		},
		NewDeleteWorker: func(
			id string,
			q *queue.Queue,
			srcAdapter types.FSAdapter,
			shutdownCtx context.Context,
			workerCtx context.Context,
			idle *atomic.Bool,
			retire *atomic.Bool,
		) queue.Worker {
			return NewDeleteWorker(id, q, srcAdapter, shutdownCtx, workerCtx, idle, retire)
		},
		NewTraversalWorker: func(
			id string,
			q *queue.Queue,
			adapter types.FSAdapter,
			queueName string,
			shutdownCtx context.Context,
			workerCtx context.Context,
			idle *atomic.Bool,
			retire *atomic.Bool,
		) queue.Worker {
			return NewTraversalWorker(id, q, adapter, queueName, shutdownCtx, workerCtx, idle, retire)
		},
	})
}
