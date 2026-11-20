// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"sync/atomic"
	"time"

	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// ModeKind selects which mode-package hook to invoke on Queue.
type ModeKind int

const (
	ModeTraversal ModeKind = iota
	ModeRetry
	ModeGPL
	ModeCopy
	ModeDelete
)

// ModeHooks wires pkg/queue/mode implementations into Queue without an import cycle
// (mode imports queue; queue does not import mode).
type ModeHooks struct {
	PullTraversal     func(*Queue, bool) PullResult
	PullRetry         func(*Queue, bool) PullResult
	PullGPL           func(*Queue, bool) PullResult
	PullCopy          func(*Queue, bool) PullResult
	PullDelete        func(*Queue, bool) PullResult
	CompleteTraversal func(*Queue, *TaskBase, time.Duration)
	CompleteGPL       func(*Queue, *TaskBase, time.Duration)
	CompleteCopy      func(*Queue, *TaskBase, time.Duration)
	CompleteDelete    func(*Queue, *TaskBase, time.Duration)
	FailTraversal     func(*Queue, *TaskBase, time.Duration)
	FailGPL           func(*Queue, *TaskBase, time.Duration)
	FailCopy          func(*Queue, *TaskBase, time.Duration)
	FailDelete        func(*Queue, *TaskBase, time.Duration)
	CheckTraversal    func(*Queue, int) bool
	CheckCopy         func(*Queue, int) bool
	CheckDelete       func(*Queue, int) bool
	AdvanceTraversal  func(*Queue)
	AdvanceCopy       func(*Queue)
	AdvanceDelete     func(*Queue)
}

var modeHooks ModeHooks

// RegisterModeHooks installs mode package handlers. Called from queue/mode init.
func RegisterModeHooks(h ModeHooks) {
	modeHooks = h
}

// PullTasks refills work for the given mode via registered hooks.
func (q *Queue) PullTasks(mode ModeKind, force bool) PullResult {
	var fn func(*Queue, bool) PullResult
	switch mode {
	case ModeTraversal:
		fn = modeHooks.PullTraversal
	case ModeRetry:
		fn = modeHooks.PullRetry
	case ModeGPL:
		fn = modeHooks.PullGPL
	case ModeCopy:
		fn = modeHooks.PullCopy
	case ModeDelete:
		fn = modeHooks.PullDelete
	}
	if fn == nil {
		return PullResult{Status: PullAborted}
	}
	return fn(q, force)
}

// FinishModeTask records completion or failure for the given mode.
func (q *Queue) FinishModeTask(mode ModeKind, task *TaskBase, d time.Duration, success bool) {
	var fn func(*Queue, *TaskBase, time.Duration)
	switch mode {
	case ModeTraversal, ModeRetry:
		if success {
			fn = modeHooks.CompleteTraversal
		} else {
			fn = modeHooks.FailTraversal
		}
	case ModeGPL:
		if success {
			fn = modeHooks.CompleteGPL
		} else {
			fn = modeHooks.FailGPL
		}
	case ModeCopy:
		if success {
			fn = modeHooks.CompleteCopy
		} else {
			fn = modeHooks.FailCopy
		}
	case ModeDelete:
		if success {
			fn = modeHooks.CompleteDelete
		} else {
			fn = modeHooks.FailDelete
		}
	}
	if fn != nil {
		fn(q, task, d)
	}
}

// CheckModeCompletion returns whether the phase should complete for the given mode.
func (q *Queue) CheckModeCompletion(mode ModeKind, round int) bool {
	var fn func(*Queue, int) bool
	switch mode {
	case ModeTraversal, ModeRetry, ModeGPL:
		fn = modeHooks.CheckTraversal
	case ModeCopy:
		fn = modeHooks.CheckCopy
	case ModeDelete:
		fn = modeHooks.CheckDelete
	}
	if fn == nil {
		return false
	}
	return fn(q, round)
}

// AdvanceModeRound advances the mode-specific round/pass state.
func (q *Queue) AdvanceModeRound(mode ModeKind) {
	var fn func(*Queue)
	switch mode {
	case ModeTraversal, ModeRetry, ModeGPL:
		fn = modeHooks.AdvanceTraversal
	case ModeCopy:
		fn = modeHooks.AdvanceCopy
	case ModeDelete:
		fn = modeHooks.AdvanceDelete
	}
	if fn != nil {
		fn(q)
	}
}

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

// StallWatchdog is the queue-level stall monitor. Implemented by pkg/queue/observe.QueueWatchdog.
type StallWatchdog interface {
	Beat()
	Start()
	Stop()
	PossibleStall() bool
}

// QueueRegistrar is implemented by pkg/queue/observe.QueueObserver.
type QueueRegistrar interface {
	RegisterQueue(queueName string, q *Queue)
}

// ObserveHooks wires pkg/queue/observe into Queue without an import cycle.
type ObserveHooks struct {
	NewQueueWatchdog func(q *Queue, stallTimeout time.Duration) StallWatchdog
}

var observeHooks ObserveHooks

// DefaultQueueStallTimeout is the default queue-level stall window (matches observe).
const DefaultQueueStallTimeout = 30 * time.Second

// RegisterObserveHooks installs observe package factories. Called from queue/observe init.
func RegisterObserveHooks(h ObserveHooks) {
	observeHooks = h
}

func (q *Queue) attachQueueWatchdog() {
	if q == nil {
		return
	}
	if observeHooks.NewQueueWatchdog == nil {
		return
	}
	wd := observeHooks.NewQueueWatchdog(q, DefaultQueueStallTimeout)
	q.mu.Lock()
	q.watchdog = wd
	q.mu.Unlock()
	if wd != nil {
		wd.Start()
	}
}
