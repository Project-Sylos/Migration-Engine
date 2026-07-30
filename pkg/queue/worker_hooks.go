// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"sync/atomic"

	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// WorkerHooks wires pkg/queue/worker factories into Queue without an import cycle
// (worker imports queue; queue does not import worker).
type WorkerHooks struct {
	NewCopyWorker func(
		id string,
		q *Queue,
		srcAdapter types.FSAdapter,
		dstAdapter types.FSAdapter,
		shutdownCtx context.Context,
		workerCtx context.Context,
		idle *atomic.Bool,
		retire *atomic.Bool,
	) Worker
	NewDeleteWorker func(
		id string,
		q *Queue,
		srcAdapter types.FSAdapter,
		shutdownCtx context.Context,
		workerCtx context.Context,
		idle *atomic.Bool,
		retire *atomic.Bool,
	) Worker
	NewTraversalWorker func(
		id string,
		q *Queue,
		adapter types.FSAdapter,
		queueName string,
		shutdownCtx context.Context,
		workerCtx context.Context,
		idle *atomic.Bool,
		retire *atomic.Bool,
	) Worker
}

var workerHooks WorkerHooks

// RegisterWorkerHooks installs worker package factories. Called from queue/worker init.
func RegisterWorkerHooks(h WorkerHooks) {
	workerHooks = h
}

// BeatWatchdog records progress on the queue-level stall watchdog, if any.
func (q *Queue) BeatWatchdog() {
	if q == nil {
		return
	}
	q.mu.RLock()
	wd := q.watchdog
	q.mu.RUnlock()
	if wd != nil {
		wd.Beat()
	}
}

// HasWatchdog reports whether a queue-level stall watchdog is attached.
func (q *Queue) HasWatchdog() bool {
	if q == nil {
		return false
	}
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.watchdog != nil
}
