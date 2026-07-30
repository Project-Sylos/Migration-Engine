// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package aimd

import (
	"testing"
	"time"
)

func TestFSThrottleBackoffBlocksRepeatDecrease(t *testing.T) {
	st := &State{}
	now := time.Now()
	retryUntil := now.Add(10 * time.Second)

	if FSThrottleActuationBlocked(st, now, retryUntil) {
		t.Fatal("first decrease should not be blocked")
	}
	st.NoteFSBackoff(now, retryUntil, 6*time.Second)

	if !FSThrottleActuationBlocked(st, now.Add(time.Second), retryUntil) {
		t.Fatal("expected block while retry-after active")
	}
	if !FSThrottleActuationBlocked(st, now.Add(5*time.Second), retryUntil) {
		t.Fatal("expected block before probe cooldown minimum")
	}
	if FSThrottleActuationBlocked(st, now.Add(11*time.Second), now) {
		t.Fatal("expected unblock after backoff window")
	}
}

func TestFSThrottleBackoffExtendsWithAdapterRetryAfter(t *testing.T) {
	st := &State{}
	now := time.Now()
	st.NoteFSBackoff(now, now.Add(100*time.Millisecond), 6*time.Second)

	extended := now.Add(15 * time.Second)
	if !FSThrottleActuationBlocked(st, now.Add(10*time.Second), extended) {
		t.Fatal("expected adapter retry-after to extend backoff")
	}
}

func TestClearFSBackoffOnCalm(t *testing.T) {
	st := &State{}
	now := time.Now()
	st.NoteFSBackoff(now, now.Add(time.Second), time.Second)
	st.ClearFSBackoff()
	if !st.FSBackoffUntil.IsZero() {
		t.Fatal("expected backoff cleared")
	}
}

func TestInterOpDecreaseBlockedDuringFSBackoff(t *testing.T) {
	st := &State{}
	now := time.Now()
	retryUntil := now.Add(10 * time.Second)
	st.NoteFSBackoff(now, retryUntil, 6*time.Second)
	if InterOpDecreaseAllowed(st, now.Add(time.Second), retryUntil, 6*time.Second) {
		t.Fatal("expected inter-op recovery blocked during FS backoff")
	}
}

func TestInterOpIncreaseBlockedDuringFSBackoff(t *testing.T) {
	st := &State{}
	now := time.Now()
	st.NoteFSBackoff(now, now.Add(10*time.Second), 6*time.Second)
	if InterOpIncreaseAllowed(st, now.Add(time.Second), now.Add(10*time.Second)) {
		t.Fatal("expected inter-op increase blocked during FS backoff")
	}
}
