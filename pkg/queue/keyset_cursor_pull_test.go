// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestKeysetCursorNeverRewinds(t *testing.T) {
	q := NewQueue("dst", 3, 1, nil, nil)

	q.SetKeysetCursor("bbb")
	if got := q.GetKeysetCursor(); got != "bbb" {
		t.Fatalf("cursor=%q want bbb", got)
	}
	q.SetKeysetCursor("aaa") // rewind attempt
	if got := q.GetKeysetCursor(); got != "bbb" {
		t.Fatalf("cursor rewound to %q; want bbb", got)
	}
	q.SetKeysetCursor("ccc")
	if got := q.GetKeysetCursor(); got != "ccc" {
		t.Fatalf("cursor=%q want ccc", got)
	}

	q.resetThisQueueKeysetCursor()
	if got := q.GetKeysetCursor(); got != "" {
		t.Fatalf("reset cursor=%q want empty", got)
	}

	src := NewQueue("src", 3, 1, nil, nil)
	src.SetKeysetCursor("m")
	src.SetKeysetCursor("a")
	if got := src.GetKeysetCursor(); got != "m" {
		t.Fatalf("src cursor rewound to %q", got)
	}

	cp := NewQueue("copy", 3, 1, nil, nil)
	cp.SetKeysetCursor("m")
	cp.SetKeysetCursor("a")
	if got := cp.GetKeysetCursor(); got != "m" {
		t.Fatalf("copy cursor rewound to %q", got)
	}
}

func TestTryBeginPullingExclusive(t *testing.T) {
	q := NewQueue("dst", 3, 1, nil, nil)
	if !q.TryBeginPulling() {
		t.Fatal("first acquire should succeed")
	}
	if q.TryBeginPulling() {
		t.Fatal("second acquire should fail while pulling")
	}
	q.SetPulling(false)
	if !q.TryBeginPulling() {
		t.Fatal("acquire after release should succeed")
	}
	q.SetPulling(false)

	var acquired int64
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if q.TryBeginPulling() {
				atomic.AddInt64(&acquired, 1)
			}
		}()
	}
	wg.Wait()
	if acquired != 1 {
		t.Fatalf("concurrent acquires=%d want 1", acquired)
	}
	q.SetPulling(false)
}
