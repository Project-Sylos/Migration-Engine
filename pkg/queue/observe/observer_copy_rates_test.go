// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package observe

import (
	"testing"
	"time"
)

func TestCalculateCopyPhaseRatesSharedSnapshot(t *testing.T) {
	o := NewQueueObserver(nil, time.Second)
	now := time.Now()

	items, bytes := o.calculateCopyPhaseRates("copy", 10, 20, 1_000_000, now)
	if items != 0 || bytes != 0 {
		t.Fatalf("first poll want 0,0 got items=%v bytes=%v", items, bytes)
	}

	// Same-poll sequential updates used to zero the second rate; combined call must not.
	later := now.Add(time.Second)
	items, bytes = o.calculateCopyPhaseRates("copy", 10, 30, 1_000_000+200_000_000, later)
	if items <= 0 {
		t.Fatalf("items/sec want >0 after +10 files in 1s, got %v", items)
	}
	if bytes <= 0 {
		t.Fatalf("bytes/sec want >0 after +200MB in 1s, got %v", bytes)
	}
	// Sliding window: +10 items / 1s = 10; +200MB / 1s = 2e8
	if items < 9.5 || items > 10.5 {
		t.Fatalf("items rate out of expected range: %v", items)
	}
	if bytes < 190_000_000 || bytes > 210_000_000 {
		t.Fatalf("bytes rate out of expected range: %v", bytes)
	}

	// Zero elapsed should keep the same window (append same timestamp → dt from oldest).
	items2, bytes2 := o.calculateCopyPhaseRates("copy", 10, 30, 1_000_000+200_000_000, later)
	if items2 != items || bytes2 != bytes {
		t.Fatalf("zero timeDelta should keep rate: got items=%v/%v bytes=%v/%v", items2, items, bytes2, bytes)
	}
}

func TestCalculateCopyPhaseRatesHoldsBurstOverWindow(t *testing.T) {
	o := NewQueueObserver(nil, 200*time.Millisecond)
	now := time.Now()

	_, _ = o.calculateCopyPhaseRates("copy", 0, 0, 0, now)
	// Batch completes: +100 files instantly, then idle ticks (Dropbox finish_batch pattern).
	burst := now.Add(200 * time.Millisecond)
	items, _ := o.calculateCopyPhaseRates("copy", 0, 100, 0, burst)
	if items < 400 {
		// 100 / 0.2s = 500/s
		t.Fatalf("burst rate too low: %v", items)
	}
	idle := burst.Add(2 * time.Second)
	itemsIdle, _ := o.calculateCopyPhaseRates("copy", 0, 100, 0, idle)
	// Still inside 5s window: 100 / ~2.2s ≈ 45/s — must not EMA-decay to ~0.
	if itemsIdle < 20 {
		t.Fatalf("idle after burst should still show windowed rate, got %v", itemsIdle)
	}
	afterWindow := burst.Add(rateWindow + time.Second)
	itemsGone, _ := o.calculateCopyPhaseRates("copy", 0, 100, 0, afterWindow)
	if itemsGone > 1 {
		t.Fatalf("after window rate should fall near 0, got %v", itemsGone)
	}
}

func TestCalculateTaskCompletionRateSlidingWindow(t *testing.T) {
	o := NewQueueObserver(nil, 200*time.Millisecond)
	now := time.Now()
	if got := o.calculateTaskCompletionRate("copy", 10, now); got != 0 {
		t.Fatalf("first sample want 0, got %v", got)
	}
	later := now.Add(2 * time.Second)
	got := o.calculateTaskCompletionRate("copy", 30, later)
	// +20 tasks / 2s = 10/s
	if got < 9.5 || got > 10.5 {
		t.Fatalf("task rate=%v want ~10", got)
	}
}
