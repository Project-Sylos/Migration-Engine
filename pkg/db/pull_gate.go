// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"time"
)

// PullGate serializes frontier pulls across queues so SRC/DST (and copy/delete)
// share one Badger pull lane. Waiters may supply a beat callback for stall watchdogs.
type PullGate struct {
	ticket chan struct{}
}

func newPullGate() *PullGate {
	g := &PullGate{ticket: make(chan struct{}, 1)}
	g.ticket <- struct{}{}
	return g
}

// Acquire waits for the shared pull ticket. onWaitBeat is called about once per
// second while blocked so queue watchdogs do not treat the wait as a stall.
// When blocked, dbWork and pullGate overlap clocks (if non-nil) Enter for the wait.
func (g *PullGate) Acquire(onWaitBeat func(), dbWork, pullGate *OverlapClock) {
	if g == nil {
		return
	}
	select {
	case <-g.ticket:
		return
	default:
	}
	if dbWork != nil {
		dbWork.Enter()
	}
	if pullGate != nil {
		pullGate.Enter()
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-g.ticket:
			if pullGate != nil {
				pullGate.Leave()
			}
			if dbWork != nil {
				dbWork.Leave()
			}
			return
		case <-ticker.C:
			if onWaitBeat != nil {
				onWaitBeat()
			}
		}
	}
}

// Release returns the pull ticket. Extra releases are ignored.
func (g *PullGate) Release() {
	if g == nil {
		return
	}
	select {
	case g.ticket <- struct{}{}:
	default:
	}
}
