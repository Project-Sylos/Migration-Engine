// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"sync"

	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
)

// YAMLUpdateCallback is a function that updates the YAML config file when rounds advance.
// It's called from a background goroutine to avoid blocking.
type YAMLUpdateCallback func(srcRound, dstRound int)

// QueueCoordinator manages round advancement gates for dual-BFS traversal.
// It enforces the invariant: "DST cannot advance to round N until SRC has completed rounds N and N+1."
// This is a simple gate - queues manage themselves, coordinator only controls when DST can advance.
type QueueCoordinator struct {
	mu           sync.RWMutex
	srcRound     int                // Current SRC round
	srcDone      bool               // SRC has completed traversal
	dstRound     int                // Current DST round
	dstDone      bool               // DST has completed traversal
	yamlUpdateCB YAMLUpdateCallback // Optional callback for YAML updates on round advance
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

// SetYAMLUpdateCallback sets a callback function that will be invoked (in a background goroutine)
// whenever a round advances. This allows automatic YAML config updates.
func (c *QueueCoordinator) SetYAMLUpdateCallback(cb YAMLUpdateCallback) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.yamlUpdateCB = cb
}

// UpdateSrcRound updates SRC's current round.
func (c *QueueCoordinator) UpdateSrcRound(round int) {
	c.mu.Lock()
	oldRound := c.srcRound
	c.srcRound = round
	cb := c.yamlUpdateCB
	c.mu.Unlock()

	// If round changed and we have a callback, update YAML in background
	if oldRound != round && cb != nil {
		go func() {
			c.mu.RLock()
			srcR := c.srcRound
			dstR := c.dstRound
			c.mu.RUnlock()
			cb(srcR, dstR)
		}()
	}
}

// UpdateDstRound updates DST's current round.
func (c *QueueCoordinator) UpdateDstRound(round int) {
	c.mu.Lock()
	oldRound := c.dstRound
	c.dstRound = round
	cb := c.yamlUpdateCB
	c.mu.Unlock()

	// If round changed and we have a callback, update YAML in background
	if oldRound != round && cb != nil {
		go func() {
			c.mu.RLock()
			srcR := c.srcRound
			dstR := c.dstRound
			c.mu.RUnlock()
			cb(srcR, dstR)
		}()
	}
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
	// Log outside of lock to avoid deadlock
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
	// Log outside of lock to avoid deadlock
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

	// If SRC is done, DST can always proceed at full speed (unless DST is also done)
	// This allows DST to catch up and finish as quickly as possible once SRC completes
	if c.srcDone {
		return !c.dstDone
	}

	// DST can start round N if SRC has completed rounds N and N+1
	// SRC completes round N when it advances to N+1, completes round N+1 when it advances to N+2
	// So DST can start round N if SRC is at round N+2 or higher
	requiredSrcRound := targetRound + 2
	return c.srcRound >= requiredSrcRound && !c.dstDone
}
