// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package worker

import (
	"sync/atomic"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

func setWorkerBusy(idle *atomic.Bool) {
	if idle != nil {
		idle.Store(false)
	}
}

func setWorkerIdle(idle *atomic.Bool) {
	if idle != nil {
		idle.Store(true)
	}
}

// markWorkerIdle sets the idle flag and claims a deferred scale-down retirement if owed.
func markWorkerIdle(q *queue.Queue, workerID string, idle *atomic.Bool) {
	setWorkerIdle(idle)
	if q != nil {
		q.TryClaimIdleRetirement(workerID)
	}
}
