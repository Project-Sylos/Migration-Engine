// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package observe

import (
	"context"
	"errors"
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

func TestIsNonRetryableTraversalError(t *testing.T) {
	fatal := []string{
		"failed to list children of /x: fs: path blocked from migration: /proc",
		"permission denied",
		"traversal stalled: list children of /foo stalled after 15s",
		"localfs: not a directory: /x",
		"no such file or directory",
	}
	for _, msg := range fatal {
		if !queue.IsNonRetryableTraversalError(msg) {
			t.Errorf("expected non-retryable: %q", msg)
		}
	}
	if queue.IsNonRetryableTraversalError("temporary network glitch") {
		t.Fatal("transient should retry")
	}
}

func TestProgressWatchdog_cancelsOnStall(t *testing.T) {
	parent := context.Background()
	stalled := false
	wd, ctx := NewProgressWatchdog(parent, 50*time.Millisecond, nil, func() { stalled = true })
	defer wd.Stop()
	wd.Open()

	select {
	case <-ctx.Done():
		// ok
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog did not cancel")
	}
	if parent.Err() != nil {
		t.Fatal("parent should still be live on stall cancel")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && !stalled {
		time.Sleep(5 * time.Millisecond)
	}
	if !stalled {
		t.Fatal("expected onStall callback")
	}
}

func TestProgressWatchdog_zeroTimeoutNeverStalls(t *testing.T) {
	parent := context.Background()
	stalled := false
	wd, ctx := NewProgressWatchdog(parent, 0, nil, func() { stalled = true })
	defer wd.Stop()
	wd.Open()
	time.Sleep(80 * time.Millisecond)
	if ctx.Err() != nil || stalled {
		t.Fatal("timeout 0 must not stall-abort")
	}
	wd.Close()
}

func TestProgressWatchdog_disarmedOutsideOpenClose(t *testing.T) {
	parent := context.Background()
	wd, ctx := NewProgressWatchdog(parent, 40*time.Millisecond, nil, nil)
	defer wd.Stop()

	time.Sleep(120 * time.Millisecond)
	if ctx.Err() != nil {
		t.Fatal("must not stall before Open")
	}
	wd.Open()
	time.Sleep(20 * time.Millisecond)
	wd.Close()
	time.Sleep(120 * time.Millisecond)
	if ctx.Err() != nil {
		t.Fatal("must not stall after Close")
	}
}

func TestProgressWatchdog_rateLimitPreservesRemainingBudget(t *testing.T) {
	parent := context.Background()
	active := true
	// 80ms active budget. Spend ~40ms, suppress well past the wall clock, then
	// clear suppress: must keep ~40ms remaining (not cancel immediately, not reset to 80ms).
	wd, ctx := NewProgressWatchdog(parent, 80*time.Millisecond, func() bool { return active }, nil)
	defer wd.Stop()
	wd.Open()

	time.Sleep(40 * time.Millisecond)
	if ctx.Err() != nil {
		t.Fatal("should not stall during first active slice")
	}
	time.Sleep(200 * time.Millisecond) // wall >> timeout while suppressed
	if ctx.Err() != nil {
		t.Fatal("wall clock during rate-limit must not consume stall budget")
	}
	active = false
	cleared := time.Now()
	select {
	case <-ctx.Done():
		elapsed := time.Since(cleared)
		if elapsed < 20*time.Millisecond {
			t.Fatalf("cancelled too soon after rate-limit cleared (%s); remaining budget should apply", elapsed)
		}
		if elapsed > 200*time.Millisecond {
			t.Fatalf("cancelled too late after rate-limit cleared (%s); budget should not fully reset", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog did not cancel after remaining active budget")
	}
}

func TestErrTraversalStalled_isNonRetryable(t *testing.T) {
	err := errors.Join(errors.New("traversal stalled"), errors.New("list children of /x stalled after 15s"))
	if !queue.IsNonRetryableTraversalError(err.Error()) {
		t.Fatalf("want non-retryable: %v", err)
	}
}
