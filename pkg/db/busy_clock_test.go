// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"testing"
	"time"
)

func TestOverlapClock_parallelCountsOnce(t *testing.T) {
	var c OverlapClock
	c.Enter()
	time.Sleep(40 * time.Millisecond)
	c.Enter() // second waiter overlaps
	time.Sleep(40 * time.Millisecond)
	c.Leave()
	time.Sleep(40 * time.Millisecond)
	c.Leave()

	ns := c.TakeNs()
	// ~120ms wall, not ~160ms worker-sum
	if ns < int64(100*time.Millisecond) || ns > int64(200*time.Millisecond) {
		t.Fatalf("wall ns=%d want ~120ms", ns)
	}
	if c.TakeNs() != 0 {
		t.Fatal("second Take should be empty")
	}
}

func TestOverlapClock_takeWhileBusyCreditsPartial(t *testing.T) {
	var c OverlapClock
	c.Enter()
	time.Sleep(30 * time.Millisecond)
	ns := c.TakeNs()
	if ns < int64(20*time.Millisecond) {
		t.Fatalf("partial credit too small: %d", ns)
	}
	time.Sleep(30 * time.Millisecond)
	c.Leave()
	ns2 := c.TakeNs()
	if ns2 < int64(20*time.Millisecond) {
		t.Fatalf("remainder too small: %d", ns2)
	}
}
