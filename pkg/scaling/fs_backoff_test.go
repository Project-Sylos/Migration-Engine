// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import (
	"testing"
	"time"
)

func TestFSThrottleBackoffBlocksRepeatDecrease(t *testing.T) {
	st := &queueAIMDState{}
	now := time.Now()
	retryUntil := now.Add(10 * time.Second)

	if fsThrottleActuationBlocked(st, now, retryUntil) {
		t.Fatal("first decrease should not be blocked")
	}
	st.noteFSBackoff(now, retryUntil, 6*time.Second)

	if !fsThrottleActuationBlocked(st, now.Add(time.Second), retryUntil) {
		t.Fatal("expected block while retry-after active")
	}
	if !fsThrottleActuationBlocked(st, now.Add(5*time.Second), retryUntil) {
		t.Fatal("expected block before probe cooldown minimum")
	}
	if fsThrottleActuationBlocked(st, now.Add(11*time.Second), now) {
		t.Fatal("expected unblock after backoff window")
	}
}

func TestFSThrottleBackoffExtendsWithAdapterRetryAfter(t *testing.T) {
	st := &queueAIMDState{}
	now := time.Now()
	st.noteFSBackoff(now, now.Add(100*time.Millisecond), 6*time.Second)

	extended := now.Add(15 * time.Second)
	if !fsThrottleActuationBlocked(st, now.Add(10*time.Second), extended) {
		t.Fatal("expected adapter retry-after to extend backoff")
	}
}

func TestClearFSBackoffOnCalm(t *testing.T) {
	st := &queueAIMDState{}
	now := time.Now()
	st.noteFSBackoff(now, now.Add(time.Second), time.Second)
	st.clearFSBackoff()
	if !st.fsBackoffUntil.IsZero() {
		t.Fatal("expected backoff cleared")
	}
}

func TestInterOpDecreaseBlockedDuringFSBackoff(t *testing.T) {
	st := &queueAIMDState{}
	now := time.Now()
	retryUntil := now.Add(10 * time.Second)
	st.noteFSBackoff(now, retryUntil, 6*time.Second)
	if interOpDecreaseAllowed(st, now.Add(time.Second), retryUntil, 6*time.Second) {
		t.Fatal("expected inter-op recovery blocked during FS backoff")
	}
}

func TestInterOpIncreaseBlockedDuringFSBackoff(t *testing.T) {
	st := &queueAIMDState{}
	now := time.Now()
	st.noteFSBackoff(now, now.Add(10*time.Second), 6*time.Second)
	if interOpIncreaseAllowed(st, now.Add(time.Second), now.Add(10*time.Second)) {
		t.Fatal("expected inter-op increase blocked during FS backoff")
	}
}
