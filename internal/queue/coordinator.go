// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"sync"
)

// QueueCoordinator manages explicit inter-queue round coordination between src and dst queues.
// It ensures bounded concurrency by limiting how far ahead src can be relative to dst.
type QueueCoordinator struct {
	mu           sync.RWMutex
	srcRound     int
	srcCompleted bool
	dstRound     int
	dstCompleted bool
	maxLead      int // Maximum rounds src can be ahead of dst
}

// NewQueueCoordinator creates a new coordinator with the specified maximum lead.
// maxLead determines how many rounds ahead src can be before it must wait for dst.
func NewQueueCoordinator(maxLead int) *QueueCoordinator {
	return &QueueCoordinator{
		srcRound:     0,
		srcCompleted: false,
		dstRound:     0,
		dstCompleted: false,
		maxLead:      maxLead,
	}
}

// CanSrcAdvance returns true if src can advance to the next round.
// Src can advance if it's not more than maxLead rounds ahead of dst.
// If dst has completed, src should not advance (dst finished early, migration should fail).
func (c *QueueCoordinator) CanSrcAdvance() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.dstCompleted {
		return false // DST completed early, SRC should stop
	}
	return (c.srcRound - c.dstRound) < c.maxLead
}

// CanDstAdvance returns true if dst can advance to the next round.
// Dst on round N can only advance to round N+1 if src is at least on round N+3.
// This ensures that when dst completes round N and needs to query src's successful tasks
// from round N+1, those tasks are guaranteed to exist because src round N+1 completed
// when src advanced to round N+2, and src is now at least on round N+3.
func (c *QueueCoordinator) CanDstAdvance() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return (c.dstRound+3 <= c.srcRound || c.srcCompleted) && !c.dstCompleted
}

// UpdateSrcRound atomically updates the src round counter.
func (c *QueueCoordinator) UpdateSrcRound(round int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.srcRound = round
}

// UpdateDstRound atomically updates the dst round counter.
func (c *QueueCoordinator) UpdateDstRound(round int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dstRound = round
}

// GetSrcRound returns the current src round.
func (c *QueueCoordinator) GetSrcRound() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.srcRound
}

// GetDstRound returns the current dst round.
func (c *QueueCoordinator) GetDstRound() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.dstRound
}

// UpdateSrcCompleted atomically updates the src completed flag.
func (c *QueueCoordinator) UpdateSrcCompleted() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.srcCompleted = true
}

// UpdateDstCompleted atomically updates the dst completed flag.
func (c *QueueCoordinator) UpdateDstCompleted() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dstCompleted = true
}

// IsDstCompleted returns true if dst has completed.
func (c *QueueCoordinator) IsDstCompleted() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.dstCompleted
}
