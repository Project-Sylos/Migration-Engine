// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"testing"
)

func TestCoordinator_CanSrcStartRound(t *testing.T) {
	c := NewQueueCoordinator()
	// Default: maxSrcAhead = 3. SRC may run when srcRound <= dstRound + 3.
	c.UpdateRound("dst", 0)
	if !c.CanSrcStartRound(0) {
		t.Error("SRC round 0 should be allowed when DST at 0")
	}
	if !c.CanSrcStartRound(3) {
		t.Error("SRC round 3 should be allowed when DST at 0 (3 <= 0+3)")
	}
	if c.CanSrcStartRound(4) {
		t.Error("SRC round 4 should not be allowed when DST at 0 (4 > 0+3)")
	}
	c.UpdateRound("dst", 2)
	if !c.CanSrcStartRound(5) {
		t.Error("SRC round 5 should be allowed when DST at 2 (5 <= 2+3)")
	}
	if c.CanSrcStartRound(6) {
		t.Error("SRC round 6 should not be allowed when DST at 2")
	}
	c.MarkCompleted("src")
	if !c.CanSrcStartRound(100) {
		t.Error("SRC can run any round after SRC is completed")
	}
}

func TestCoordinator_SetMaxSrcAhead(t *testing.T) {
	c := NewQueueCoordinator()
	c.UpdateRound("dst", 0)
	c.SetMaxSrcAhead(1)
	if c.CanSrcStartRound(2) {
		t.Error("SRC round 2 should not be allowed when maxAhead=1 and DST at 0")
	}
	if !c.CanSrcStartRound(1) {
		t.Error("SRC round 1 should be allowed when maxAhead=1 and DST at 0")
	}
	c.SetMaxSrcAhead(0)
	if c.CanSrcStartRound(1) {
		t.Error("SRC round 1 should not be allowed when maxAhead=0 and DST at 0")
	}
}

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
