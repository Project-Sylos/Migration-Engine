// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package worker

import "sync/atomic"

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
