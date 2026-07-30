// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import "time"

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
