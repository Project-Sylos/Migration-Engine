// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"testing"
	"time"

	"codeberg.org/Sylos/Sylos-FS/pkg/credentials"
)

type stubRateLimitTelemetry struct {
	until time.Time
}

func (s stubRateLimitTelemetry) TakeRecentHits() int64 { return 0 }
func (s stubRateLimitTelemetry) RateLimitedUntil() time.Time {
	return s.until
}

func TestIsNonRetryableCopyError(t *testing.T) {
	if !IsNonRetryableCopyError("failed to commit upload: dropbox: path/no_write_permission/ (HTTP 409)") {
		t.Fatal("expected fatal copy error")
	}
	if IsNonRetryableCopyError("timeout waiting for response") {
		t.Fatal("transient error should retry")
	}
}

func TestIsThrottleError(t *testing.T) {
	if IsThrottleError(nil) {
		t.Fatal("nil error should not be throttle")
	}
	if !IsThrottleError(credentials.RateLimited(2 * time.Second)) {
		t.Fatal("RateLimitedError should be throttle")
	}
	if !IsThrottleError(errString("429 Too Many Requests")) {
		t.Fatal("429 message should be throttle")
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func TestRateLimitedUntilSides(t *testing.T) {
	srcUntil := time.Now().Add(10 * time.Second)
	dstUntil := time.Now().Add(20 * time.Second)

	srcQ := NewQueue("src", 3, 1, nil, nil)
	srcQ.SetRateLimitTelemetry(stubRateLimitTelemetry{until: srcUntil})
	gotSrc, gotDst := srcQ.RateLimitedUntilSides()
	if !gotSrc.Equal(srcUntil) || !gotDst.IsZero() {
		t.Fatalf("src queue: src=%v dst=%v", gotSrc, gotDst)
	}

	dstQ := NewQueue("dst", 3, 1, nil, nil)
	dstQ.SetRateLimitTelemetry(stubRateLimitTelemetry{until: dstUntil})
	gotSrc, gotDst = dstQ.RateLimitedUntilSides()
	if !gotSrc.IsZero() || !gotDst.Equal(dstUntil) {
		t.Fatalf("dst queue: src=%v dst=%v", gotSrc, gotDst)
	}

	copyQ := NewQueue("copy", 3, 1, nil, nil)
	copyQ.SetRateLimitTelemetry(
		stubRateLimitTelemetry{until: srcUntil},
		stubRateLimitTelemetry{until: dstUntil},
	)
	gotSrc, gotDst = copyQ.RateLimitedUntilSides()
	if !gotSrc.Equal(srcUntil) || !gotDst.Equal(dstUntil) {
		t.Fatalf("copy queue: src=%v dst=%v", gotSrc, gotDst)
	}

	delQ := NewQueue("delete", 3, 1, nil, nil)
	delQ.SetRateLimitTelemetry(stubRateLimitTelemetry{until: srcUntil})
	gotSrc, gotDst = delQ.RateLimitedUntilSides()
	if !gotSrc.Equal(srcUntil) || !gotDst.IsZero() {
		t.Fatalf("delete queue: src=%v dst=%v", gotSrc, gotDst)
	}
}


func TestYieldTaskOnRateLimitDoesNotIncrementAttempts(t *testing.T) {
	q := NewQueue("src", 3, 1, nil, nil)
	task := &TaskBase{ID: "abc", Round: 0, Attempts: 0}
	q.mu.Lock()
	q.inProgress[task.ID] = task
	task.Locked = true
	q.mu.Unlock()

	q.yieldTaskOnRateLimit(task, time.Millisecond)

	if task.Attempts != 0 {
		t.Fatalf("attempts=%d want 0", task.Attempts)
	}
	if q.InProgressCount() != 0 {
		t.Fatalf("inProgress=%d want 0", q.InProgressCount())
	}
	if q.GetPendingCount() != 1 {
		t.Fatalf("pending=%d want 1", q.GetPendingCount())
	}
}

func TestRateLimitReleaseJitter(t *testing.T) {
	seen := map[time.Duration]bool{}
	for i := 0; i < 40; i++ {
		j := rateLimitReleaseJitter(4 * time.Second)
		if j < 0 || j > rateLimitReleaseJitterCap {
			t.Fatalf("jitter=%v out of range", j)
		}
		seen[j] = true
	}
	if len(seen) < 2 {
		t.Fatal("expected varied jitter samples")
	}
}
