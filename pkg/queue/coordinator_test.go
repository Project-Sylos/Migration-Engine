// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"testing"
)

func TestCoordinator_CanDstStartRound(t *testing.T) {
	c := NewQueueCoordinator()
	// DST can start round N when SRC >= N+2 (or SRC done).
	if c.CanDstStartRound(0) {
		t.Error("DST should not start round 0 when SRC at 0 (need SRC >= 2)")
	}
	c.UpdateRound("src", 2)
	if !c.CanDstStartRound(0) {
		t.Error("DST should start round 0 when SRC at 2")
	}
	c.UpdateRound("src", 5)
	if !c.CanDstStartRound(3) {
		t.Error("DST should start round 3 when SRC at 5")
	}
	if c.CanDstStartRound(4) {
		t.Error("DST should not start round 4 when SRC at 5 (need SRC >= 6)")
	}
	c.MarkCompleted("src")
	if !c.CanDstStartRound(100) {
		t.Error("DST can run any round after SRC is completed")
	}
}
