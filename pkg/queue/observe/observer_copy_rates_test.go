// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package observe

import (
	"testing"
	"time"
)

func TestCalculateCopyItemsRateSlidingWindow(t *testing.T) {
	o := NewQueueObserver(nil, time.Second)
	now := time.Now()

	if items := o.calculateCopyItemsRate("copy", 10, 20, now); items != 0 {
		t.Fatalf("first poll want 0 got %v", items)
	}

	later := now.Add(time.Second)
	items := o.calculateCopyItemsRate("copy", 10, 30, later)
	if items < 9.5 || items > 10.5 {
		t.Fatalf("items rate out of expected range: %v (want ~10)", items)
	}

	items2 := o.calculateCopyItemsRate("copy", 10, 30, later)
	if items2 != items {
		t.Fatalf("zero timeDelta should keep rate: got %v want %v", items2, items)
	}
}

func TestCalculateCopyItemsRateHoldsBurstOverWindow(t *testing.T) {
	o := NewQueueObserver(nil, 200*time.Millisecond)
	now := time.Now()

	_ = o.calculateCopyItemsRate("copy", 0, 0, now)
	burst := now.Add(200 * time.Millisecond)
	items := o.calculateCopyItemsRate("copy", 0, 100, burst)
	if items < 400 {
		t.Fatalf("burst rate too low: %v", items)
	}
	idle := burst.Add(2 * time.Second)
	itemsIdle := o.calculateCopyItemsRate("copy", 0, 100, idle)
	if itemsIdle < 20 {
		t.Fatalf("idle after burst should still show windowed rate, got %v", itemsIdle)
	}
	afterWindow := burst.Add(rateWindow + time.Second)
	itemsGone := o.calculateCopyItemsRate("copy", 0, 100, afterWindow)
	if itemsGone > 1 {
		t.Fatalf("after window rate should fall near 0, got %v", itemsGone)
	}
}

func TestCalculateBytesEMA(t *testing.T) {
	o := NewQueueObserver(nil, time.Second)
	now := time.Now()

	if got := o.calculateBytesEMA("copy", 1_000_000, now); got != 0 {
		t.Fatalf("first poll want 0 got %v", got)
	}

	later := now.Add(time.Second)
	got := o.calculateBytesEMA("copy", 1_000_000+200_000_000, later)
	// EMA α=0.2: 0.2 * 2e8/s + 0.8 * 0 = 4e7
	if got < 35_000_000 || got > 45_000_000 {
		t.Fatalf("bytes EMA=%v want ~4e7", got)
	}

	got2 := o.calculateBytesEMA("copy", 1_000_000+200_000_000, later)
	if got2 != got {
		t.Fatalf("zero timeDelta should keep EMA: got %v want %v", got2, got)
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
	if got < 9.5 || got > 10.5 {
		t.Fatalf("task rate=%v want ~10", got)
	}
}

func TestCalculateDiscoveryRateAdvances(t *testing.T) {
	o := NewQueueObserver(nil, 200*time.Millisecond)
	now := time.Now()
	if got := o.calculateDiscoveryRate("src", 0, 0, now); got != 0 {
		t.Fatalf("first poll want 0, got %v", got)
	}
	later := now.Add(time.Second)
	got := o.calculateDiscoveryRate("src", 8, 2, later)
	if got < 1.5 || got > 2.5 {
		t.Fatalf("discovery EMA=%v want ~2", got)
	}
}
