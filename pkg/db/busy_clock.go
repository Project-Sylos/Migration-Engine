// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"sync"
	"time"
)

// OverlapClock accumulates wall-clock time while at least one Enter is unmatched
// by Leave. Parallel waiters overlapping the same interval are counted once.
type OverlapClock struct {
	mu     sync.Mutex
	active int
	since  time.Time
	total  int64 // nanoseconds
}

// Enter marks the start of one overlapping busy period.
func (c *OverlapClock) Enter() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active == 0 {
		c.since = time.Now()
	}
	c.active++
}

// Leave ends one Enter. When the last waiter leaves, elapsed wall time is added.
func (c *OverlapClock) Leave() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active <= 0 {
		return
	}
	c.active--
	if c.active == 0 {
		c.total += time.Since(c.since).Nanoseconds()
	}
}

// TakeNs returns and clears accumulated wall nanoseconds. If still busy, credits
// time since the current interval started and restarts the interval at now.
func (c *OverlapClock) TakeNs() int64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if c.active > 0 {
		c.total += now.Sub(c.since).Nanoseconds()
		c.since = now
	}
	v := c.total
	c.total = 0
	return v
}
