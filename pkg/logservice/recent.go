// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package logservice

import (
	"sync"
	"time"
)

const recentLogCap = 1000

// RecentLog is one in-memory log line for live UI polling.
type RecentLog struct {
	ID      string
	At      time.Time
	Level   string
	Message string
}

type recentRing struct {
	mu      sync.Mutex
	entries []RecentLog
	next    int
	size    int
}

func newRecentRing(capacity int) *recentRing {
	if capacity <= 0 {
		capacity = recentLogCap
	}
	return &recentRing{entries: make([]RecentLog, capacity)}
}

func (r *recentRing) add(entry RecentLog) {
	if r == nil || len(r.entries) == 0 {
		return
	}
	r.mu.Lock()
	r.entries[r.next] = entry
	r.next = (r.next + 1) % len(r.entries)
	if r.size < len(r.entries) {
		r.size++
	}
	r.mu.Unlock()
}

func (r *recentRing) recent(limit int) []RecentLog {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if limit <= 0 || limit > r.size {
		limit = r.size
	}
	out := make([]RecentLog, 0, limit)
	for i := 0; i < limit; i++ {
		idx := (r.next - 1 - i + len(r.entries)) % len(r.entries)
		out = append(out, r.entries[idx])
	}
	return out
}
