// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package worker

import (
	"context"
	"testing"
	"time"
)

func TestAwaitCancellable_completes(t *testing.T) {
	ctx := context.Background()
	v, ok := awaitCancellable(ctx, func() int { return 7 })
	if !ok || v != 7 {
		t.Fatalf("got %v ok=%v", v, ok)
	}
}

func TestAwaitCancellable_ctxWinsOverHang(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	go func() {
		<-started
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	v, ok := awaitCancellable(ctx, func() int {
		close(started)
		time.Sleep(time.Hour)
		return 1
	})
	if ok || v != 0 {
		t.Fatalf("expected cancel win, got v=%v ok=%v", v, ok)
	}
}

func TestAwaitCancellableBeating_beatsUntilDone(t *testing.T) {
	ctx := context.Background()
	var beats int
	v, ok := awaitCancellableBeating(ctx, func() { beats++ }, 20*time.Millisecond, func() int {
		time.Sleep(55 * time.Millisecond)
		return 9
	})
	if !ok || v != 9 {
		t.Fatalf("got %v ok=%v", v, ok)
	}
	if beats < 1 {
		t.Fatalf("expected mid-op beats, got %d", beats)
	}
}

func TestAwaitCancellableBeating_ctxWins(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(25 * time.Millisecond)
		cancel()
	}()
	v, ok := awaitCancellableBeating(ctx, func() {}, 5*time.Millisecond, func() int {
		time.Sleep(time.Hour)
		return 1
	})
	if ok || v != 0 {
		t.Fatalf("expected cancel win, got v=%v ok=%v", v, ok)
	}
}
