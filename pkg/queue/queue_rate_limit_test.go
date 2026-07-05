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

func TestRateLimitedWaitDuration(t *testing.T) {
	q := NewQueue("src", 3, 1, nil, nil)
	until := time.Now().Add(500 * time.Millisecond)
	q.SetRateLimitTelemetry(stubRateLimitTelemetry{until: until})
	if d := q.RateLimitedWaitDuration(); d <= 0 {
		t.Fatalf("expected positive wait, got %v", d)
	}
	q.SetRateLimitTelemetry(stubRateLimitTelemetry{until: time.Now().Add(-time.Second)})
	if d := q.RateLimitedWaitDuration(); d != 0 {
		t.Fatalf("expected zero wait after window, got %v", d)
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
