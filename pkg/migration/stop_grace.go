// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"time"
)

// DefaultStopGracePeriod is how long a soft-suspend drain may run before the run context is canceled.
const DefaultStopGracePeriod = 30 * time.Second

const softSuspendMaxWait = 4 * time.Minute

// softSuspendWaitContext bounds soft-suspend I/O; it is canceled when ShutdownContext is canceled.
func softSuspendWaitContext(shutdown context.Context) (context.Context, context.CancelFunc) {
	parent := shutdown
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, softSuspendMaxWait)
}

func (m *Migration) armStopGraceTimer(period time.Duration) {
	if period <= 0 {
		period = DefaultStopGracePeriod
	}
	m.stopGraceMu.Lock()
	defer m.stopGraceMu.Unlock()
	if m.stopGraceTimer != nil {
		m.stopGraceTimer.Stop()
	}
	m.stopGraceTimer = time.AfterFunc(period, func() {
		_, _ = m.ForceStop()
	})
}

func (m *Migration) disarmStopGraceTimer() {
	m.stopGraceMu.Lock()
	defer m.stopGraceMu.Unlock()
	if m.stopGraceTimer != nil {
		m.stopGraceTimer.Stop()
		m.stopGraceTimer = nil
	}
}
