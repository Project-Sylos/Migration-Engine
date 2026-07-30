// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package observe

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"context"
	"errors"
	"testing"
	"time"
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
	wd, ctx := NewProgressWatchdog(parent, 50*time.Millisecond, nil)
	defer wd.Stop()

	select {
	case <-ctx.Done():
		// ok
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog did not cancel")
	}
	if parent.Err() != nil {
		t.Fatal("parent should still be live on stall cancel")
	}
}

func TestProgressWatchdog_rateLimitSuppressesStall(t *testing.T) {
	parent := context.Background()
	active := true
	wd, ctx := NewProgressWatchdog(parent, 40*time.Millisecond, func() bool { return active })
	defer wd.Stop()

	time.Sleep(120 * time.Millisecond)
	if ctx.Err() != nil {
		t.Fatal("stall should be suppressed while rate-limit active")
	}
	active = false
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog did not cancel after suppress cleared")
	}
}

func TestErrTraversalStalled_isNonRetryable(t *testing.T) {
	err := errors.Join(errors.New("traversal stalled"), errors.New("list children of /x stalled after 15s"))
	if !queue.IsNonRetryableTraversalError(err.Error()) {
		t.Fatalf("want non-retryable: %v", err)
	}
}
