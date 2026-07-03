// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"sort"
	"sync"
)

const (
	listFillMaxSamples = 256
	// listFillMinSamples avoids actuating page size on too few list observations.
	listFillMinSamples = 10
)

type listFillTracker struct {
	mu      sync.Mutex
	samples []int
}

func (t *listFillTracker) record(itemCount int) {
	if t == nil {
		return
	}
	if itemCount < 0 {
		itemCount = 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.samples) >= listFillMaxSamples {
		t.samples = append(t.samples[:0], t.samples[1:]...)
	}
	t.samples = append(t.samples, itemCount)
}

func (t *listFillTracker) p95() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.samples) < listFillMinSamples {
		return 0
	}
	cp := append([]int(nil), t.samples...)
	sort.Ints(cp)
	idx := p95Index(len(cp))
	return cp[idx]
}

func p95Index(n int) int {
	if n <= 0 {
		return 0
	}
	idx := (n*95+99)/100 - 1
	if idx < 0 {
		return 0
	}
	if idx >= n {
		return n - 1
	}
	return idx
}

// RecordListFill records how many children one ListChildren call returned.
func (q *Queue) RecordListFill(itemCount int) {
	if q == nil {
		return
	}
	q.listFill.record(itemCount)
}

// ListItemsP95 returns the p95 child count from recent list operations, or 0 if
// there are too few samples for a stable estimate.
func (q *Queue) ListItemsP95() int {
	if q == nil {
		return 0
	}
	return q.listFill.p95()
}
