// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"sync"

	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
)

// QueueCoordinator manages round advancement gates for dual-BFS traversal.
// It enforces the invariant: "DST cannot advance to round N until SRC has completed rounds N and N+1."
// This is a simple gate - queues manage themselves, coordinator only controls when DST can advance.
type QueueCoordinator struct {
	mu       sync.RWMutex
	srcRound int  // Current SRC round
	srcDone  bool // SRC has completed traversal
	dstRound int  // Current DST round
	dstDone  bool // DST has completed traversal
}

// NewQueueCoordinator creates a new coordinator.
func NewQueueCoordinator() *QueueCoordinator {
	return &QueueCoordinator{
		srcRound: 0,
		srcDone:  false,
		dstRound: 0,
		dstDone:  false,
	}
}

// UpdateSrcRound updates SRC's current round.
func (c *QueueCoordinator) UpdateSrcRound(round int) {
	c.mu.Lock()
	c.srcRound = round
	c.mu.Unlock()
}

// UpdateDstRound updates DST's current round.
func (c *QueueCoordinator) UpdateDstRound(round int) {
	c.mu.Lock()
	c.dstRound = round
	c.mu.Unlock()
}

// GetSrcRound returns SRC's current round.
func (c *QueueCoordinator) GetSrcRound() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.srcRound
}

// GetDstRound returns DST's current round.
func (c *QueueCoordinator) GetDstRound() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.dstRound
}

// MarkSrcCompleted marks SRC as completed.
func (c *QueueCoordinator) MarkSrcCompleted() {
	c.mu.Lock()
	c.srcDone = true
	c.mu.Unlock()
	if logservice.LS != nil {
		_ = logservice.LS.Log("debug",
			"Coordinator: SRC marked as completed",
			"coordinator", "mark", "coordinator")
	}
}

// MarkDstCompleted marks DST as completed.
func (c *QueueCoordinator) MarkDstCompleted() {
	c.mu.Lock()
	c.dstDone = true
	c.mu.Unlock()
	if logservice.LS != nil {
		_ = logservice.LS.Log("debug",
			"Coordinator: DST marked as completed",
			"coordinator", "mark", "coordinator")
	}
}

// IsCompleted returns true if the specified queue ("src", "dst", or "both") has completed traversal.
func (c *QueueCoordinator) IsCompleted(queueType string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	switch queueType {
	case "src":
		return c.srcDone
	case "dst":
		return c.dstDone
	case "both":
		return c.srcDone && c.dstDone
	default:
		return false
	}
}

// CanDstStartRound returns true if DST can start processing the specified round.
// DST can start round N if:
//   - SRC has completed traversal entirely (DST can proceed freely), OR
//   - SRC has completed rounds N and N+1 (SRC is at round N+2 or higher)
//
// Note: Since DST can now freely advance to a round and then pause, the check is N+2
// (if DST wants to start round 4, SRC needs to have completed rounds 4 and 5, so SRC >= 6).
// Once SRC is completed, DST can proceed at full speed with no restrictions.
func (c *QueueCoordinator) CanDstStartRound(targetRound int) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.srcDone {
		return !c.dstDone
	}

	requiredSrcRound := targetRound + 2
	return c.srcRound >= requiredSrcRound && !c.dstDone
}
