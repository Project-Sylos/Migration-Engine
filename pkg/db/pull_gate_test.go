// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestPullGateSerializesAndBeatsWhileWaiting(t *testing.T) {
	g := newPullGate()
	var beats atomic.Int64

	g.Acquire(nil)
	done := make(chan struct{})
	go func() {
		g.Acquire(func() { beats.Add(1) })
		close(done)
	}()

	time.Sleep(1100 * time.Millisecond)
	if beats.Load() < 1 {
		t.Fatal("expected wait beat while second Acquire blocked")
	}
	g.Release()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("second Acquire did not proceed after Release")
	}
	g.Release()
}

func TestPullGateExtraReleaseIgnored(t *testing.T) {
	g := newPullGate()
	g.Release()
	g.Release()
	acquired := make(chan struct{}, 2)
	go func() {
		g.Acquire(nil)
		acquired <- struct{}{}
	}()
	go func() {
		g.Acquire(nil)
		acquired <- struct{}{}
	}()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("first Acquire should succeed from initial ticket")
	}
	select {
	case <-acquired:
		t.Fatal("second Acquire must wait; extra Release must not mint tickets")
	case <-time.After(200 * time.Millisecond):
	}
	g.Release()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("second Acquire should proceed after one Release")
	}
	g.Release()
}
