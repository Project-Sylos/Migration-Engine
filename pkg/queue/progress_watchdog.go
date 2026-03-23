// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"sync/atomic"
	"time"
)

const (
	copyStallTimeout      = 2 * time.Minute
	traversalStallTimeout = 60 * time.Second
)

// ProgressWatchdog detects stalled tasks by requiring progress (Beat) within a timeout.
// If no progress occurs, it cancels the context so adapter operations abort.
type ProgressWatchdog struct {
	ctx            context.Context
	cancel         context.CancelFunc
	timeout        time.Duration
	stallSuppress  func() bool // when non-nil and true, stall timer is frozen (e.g. seal flush / backpressure)
	lastBeat       atomic.Int64 // UnixNano (time.Now().UnixNano())
}

// NewProgressWatchdog creates a watchdog and a child context. When no Beat() occurs
// within timeout, the context is cancelled. Call Stop() when the task ends to release resources.
// If stallSuppress is non-nil and returns true, the elapsed stall window does not advance (workers blocked on seal I/O are not adapter stalls).
func NewProgressWatchdog(parent context.Context, timeout time.Duration, stallSuppress func() bool) (*ProgressWatchdog, context.Context) {
	ctx, cancel := context.WithCancel(parent)
	wd := &ProgressWatchdog{
		ctx:           ctx,
		cancel:        cancel,
		timeout:       timeout,
		stallSuppress: stallSuppress,
	}
	wd.lastBeat.Store(time.Now().UnixNano())
	go wd.monitor()
	return wd, ctx
}

// Beat records progress. Call whenever meaningful work completes.
func (w *ProgressWatchdog) Beat() {
	w.lastBeat.Store(time.Now().UnixNano())
}

// Stop cancels the context and stops the monitor goroutine.
func (w *ProgressWatchdog) Stop() {
	w.cancel()
}

func (w *ProgressWatchdog) monitor() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			if w.stallSuppress != nil && w.stallSuppress() {
				w.Beat()
				continue
			}
			lastNano := w.lastBeat.Load()
			last := time.Unix(lastNano/1e9, lastNano%1e9)
			if time.Since(last) > w.timeout {
				w.cancel()
				return
			}
		}
	}
}
