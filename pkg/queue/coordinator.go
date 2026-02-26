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

// UpdateRound updates the current round for SRC or DST.
func (c *QueueCoordinator) UpdateRound(which string, round int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch which {
	case "src":
		c.srcRound = round
	case "dst":
		c.dstRound = round
	}
}

// GetRound returns the current round for SRC or DST.
func (c *QueueCoordinator) GetRound(which string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	switch which {
	case "src":
		return c.srcRound
	case "dst":
		return c.dstRound
	default:
		return -1
	}
}

// MarkCompleted marks SRC or DST as completed based on the argument ("src" or "dst").
func (c *QueueCoordinator) MarkCompleted(which string) {
	c.mu.Lock()
	switch which {
	case "src":
		c.srcDone = true
	case "dst":
		c.dstDone = true
	}
	c.mu.Unlock()
	if logservice.LS != nil {
		var whichMsg string
		switch which {
		case "src":
			whichMsg = "SRC"
		case "dst":
			whichMsg = "DST"
		default:
			whichMsg = which
		}
		_ = logservice.LS.Log("debug",
			"Coordinator: "+whichMsg+" marked as completed",
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
