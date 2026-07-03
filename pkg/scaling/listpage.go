// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

// ApplyQueueListPagination sets initial list page size from a provider profile.
func ApplyQueueListPagination(q QueueActuator, profile FSPerformanceProfile) {
	if q == nil {
		return
	}
	size := profile.DefaultListPageSize
	if size <= 0 {
		size = 100
	}
	if min := profile.MinListPageSize; min > 0 && size < min {
		size = min
	}
	if max := profile.MaxListPageSize; max > 0 && size > max {
		size = max
	}
	q.SetListPageSize(size)
}

// p95ListItems must come from recent ListChildren result sizes; zero means insufficient data.
func ListPageIncreaseAllowed(curPageSize, p95ListItems int) bool {
	if curPageSize <= 0 || p95ListItems <= 0 {
		return false
	}
	return p95ListItems > curPageSize
}

// IncreaseListPageSize grows pagination under FS throttle (fewer list API calls per folder).
func IncreaseListPageSize(cur, min, max, step int) (int, bool) {
	if cur <= 0 {
		cur = min
	}
	if cur >= max {
		return cur, false
	}
	if step <= 0 {
		step = 20
	}
	next := cur * 2
	if next > max {
		next = max
	}
	if next <= cur {
		next = cur + step
		if next > max {
			next = max
		}
	}
	next = ClampInt(next, min, max)
	return next, next > cur
}

// DecreaseListPageSize reduces pagination toward default during calm scale-up.
func DecreaseListPageSize(cur, min, defaultSize int) (int, bool) {
	if defaultSize <= 0 {
		defaultSize = min
	}
	if cur <= defaultSize {
		return cur, false
	}
	next := cur / 2
	if next < defaultSize {
		next = defaultSize
	}
	next = ClampInt(next, min, next)
	return next, next < cur
}
