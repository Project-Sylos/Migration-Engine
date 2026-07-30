// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package observe

import (
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

func init() {
	queue.RegisterObserveHooks(queue.ObserveHooks{
		NewQueueWatchdog: func(q *queue.Queue, stallTimeout time.Duration) queue.StallWatchdog {
			return NewQueueWatchdog(q, stallTimeout)
		},
	})
}
