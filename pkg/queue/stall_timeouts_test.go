// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"testing"
	"time"
)

func TestStallTimeoutFromSeconds(t *testing.T) {
	if got := StallTimeoutFromSeconds(-1, 60); got != 60*time.Second {
		t.Fatalf("default: got %v", got)
	}
	if got := StallTimeoutFromSeconds(0, 60); got != 0 {
		t.Fatalf("unlimited: got %v", got)
	}
	if got := StallTimeoutFromSeconds(90, 60); got != 90*time.Second {
		t.Fatalf("custom: got %v", got)
	}
}

func TestQueueFSStallTimeoutOverrides(t *testing.T) {
	q := NewQueue("src", 3, 1, nil, nil)
	if q.TraversalStallTimeout() != DefaultTraversalStallTimeout {
		t.Fatal("expected package default before Set")
	}
	q.SetFSStallTimeouts(FSStallTimeouts{
		Traversal: 0,
		Delete:    30 * time.Second,
		Copy:      120 * time.Second,
	})
	if q.TraversalStallTimeout() != 0 {
		t.Fatal("expected unlimited traversal")
	}
	if q.DeleteStallTimeout() != 30*time.Second {
		t.Fatal("expected 30s delete")
	}
	if q.CopyStallTimeout() != 120*time.Second {
		t.Fatal("expected 120s copy")
	}
}
