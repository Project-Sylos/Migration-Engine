// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package worker

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

// workerBase is the state every phase worker shares.
type workerBase struct {
	id          string
	queue       *queue.Queue
	shutdownCtx context.Context
	workerCtx   context.Context
	idle        *atomic.Bool
	retire      *atomic.Bool
	// loop is the per-worker policy Run executes; constructors set it.
	loop loopSpec
}

func newWorkerBase(id string, q *queue.Queue, shutdownCtx, workerCtx context.Context, idle, retire *atomic.Bool) workerBase {
	if workerCtx == nil {
		workerCtx = shutdownCtx
	}
	return workerBase{id: id, queue: q, shutdownCtx: shutdownCtx, workerCtx: workerCtx, idle: idle, retire: retire}
}

func (b *workerBase) shouldRetire() bool {
	return b.retire != nil && b.retire.Load()
}

// loopSpec is the per-worker policy for Run.
type loopSpec struct {
	// label prefixes lifecycle logs ("Copy worker started").
	label     string
	queueName string
	execute   func(*queue.TaskBase) error
	// batchTurn runs one batch lease turn when the adapter and pass allow it and reports
	// whether it ran. Nil for workers without batch leases.
	batchTurn func() bool
	// logFailure logs a non-throttle execution failure after it was reported. Optional.
	logFailure func(task *queue.TaskBase, err error)
}

func (b *workerBase) logWorker(queueName, level, msg string) {
	if logservice.LS == nil {
		return
	}
	if err := logservice.LS.Log(level, msg, "worker", b.id, queueName); err != nil {
		fmt.Println("error logging", err)
	}
}

// Run is the shared worker loop: lifecycle gates, optional batch turn, then lease,
// execute, and report one task at a time. When no work is available or the queue is
// paused it briefly sleeps before polling again; when the queue is exhausted the worker exits.
func (b *workerBase) Run() {
	spec := b.loop
	defer b.queue.NotifyWorkerExit(b.id)
	logf := func(msg string) { b.logWorker(spec.queueName, "info", spec.label+" "+msg) }
	logf("started")

	for {
		// Check for shutdown first (force exit)
		if b.shutdownCtx != nil {
			select {
			case <-b.shutdownCtx.Done():
				logf("exiting - shutdown requested")
				return
			default:
			}
		}
		if b.workerCtx != nil {
			select {
			case <-b.workerCtx.Done():
				logf("exiting - scale down")
				return
			default:
			}
		}
		if b.shouldRetire() {
			logf("exiting - scale down retire")
			return
		}

		if b.queue.IsPaused() {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if b.queue.IsExhausted() {
			logf("exiting - queue exhausted")
			return
		}

		if wait := b.queue.RateLimitedWaitDuration(); wait > 0 {
			waitCtx := b.workerCtx
			if waitCtx == nil {
				waitCtx = b.shutdownCtx
			}
			if err := b.queue.WaitRateLimited(waitCtx, wait); err != nil {
				return
			}
			continue
		}

		if spec.batchTurn != nil && spec.batchTurn() {
			if b.shouldRetire() {
				logf("exiting - scale down retire")
				return
			}
			continue
		}

		task := b.queue.Lease()
		if task == nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		b.queue.BindLeaseOwner(task, b.id)

		setWorkerBusy(b.idle)
		err := spec.execute(task)
		markWorkerIdle(b.queue, b.id, b.idle)
		if errors.Is(err, errTransferAbandoned) {
			// Scale-down abandon may already have checkpointed/removed the task; yield without failure.
			abandonLeasedGroup(b.queue, []*queue.TaskBase{task})
			if b.shouldRetire() {
				logf("exiting - scale down retire after abandon")
				return
			}
			continue
		}
		// Worker result is set before ReportTaskResult (for stall diagnostics).
		reportTaskOutcome(b.queue, task, err)
		if err != nil && spec.logFailure != nil && !queue.IsThrottleError(err) {
			spec.logFailure(task, err)
		}
		if b.shouldRetire() {
			logf("exiting - scale down retire")
			return
		}
	}
}
