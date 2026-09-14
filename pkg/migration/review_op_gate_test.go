// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReviewPathsOverlap(t *testing.T) {
	t.Parallel()
	cases := []struct {
		a, b string
		want bool
	}{
		{"/a", "/a", true},
		{"/a", "/a/b", true},
		{"/a/b", "/a", true},
		{"/a", "/ab", false},
		{"/a/b", "/a/c", false},
		{"/", "/x", true},
		{"/x", "/", true},
	}
	for _, tc := range cases {
		got := reviewPathsOverlap(normalizeReviewOpPath(tc.a), normalizeReviewOpPath(tc.b))
		if got != tc.want {
			t.Fatalf("overlap(%q,%q)=%v want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestReviewOpGatePathAndBulk(t *testing.T) {
	t.Parallel()
	g := newReviewOpGate()
	if err := g.TryBeginPathMutation("/a"); err != nil {
		t.Fatal(err)
	}
	if err := g.TryBeginPathMutation("/a/b"); !errors.Is(err, ErrReviewOpBusy) {
		t.Fatalf("child while parent: %v", err)
	}
	if err := g.TryBeginPathMutation("/a"); !errors.Is(err, ErrReviewOpBusy) {
		t.Fatalf("same path: %v", err)
	}
	if err := g.TryBeginBulkMutation(); !errors.Is(err, ErrReviewOpBusy) {
		t.Fatalf("bulk while path: %v", err)
	}
	if err := g.TryBeginPathMutation("/z"); err != nil {
		t.Fatal(err)
	}
	g.EndPathMutation("/z")
	g.EndPathMutation("/a")
	if err := g.TryBeginPathMutation("/a/b"); err != nil {
		t.Fatal(err)
	}
	g.EndPathMutation("/a/b")

	if err := g.TryBeginBulkMutation(); err != nil {
		t.Fatal(err)
	}
	if !g.BulkMutationInProgress() {
		t.Fatal("bulk not in progress")
	}
	if err := g.TryBeginBulkMutation(); !errors.Is(err, ErrReviewOpBusy) {
		t.Fatalf("second bulk: %v", err)
	}
	if err := g.TryBeginPathMutation("/x"); !errors.Is(err, ErrReviewOpBusy) {
		t.Fatalf("path while bulk: %v", err)
	}
	g.EndBulkMutation()
	if g.BulkMutationInProgress() {
		t.Fatal("bulk still held")
	}
	if err := g.TryBeginPathMutation("/x"); err != nil {
		t.Fatal(err)
	}
	g.EndPathMutation("/x")
}

func TestReviewOpGateInteractiveReadCancelsPrior(t *testing.T) {
	t.Parallel()
	g := newReviewOpGate()
	ctx1, cancel1 := g.beginInteractiveRead(context.Background())
	defer cancel1()
	ctx2, cancel2 := g.beginInteractiveRead(context.Background())
	defer cancel2()
	select {
	case <-ctx1.Done():
	case <-time.After(time.Second):
		t.Fatal("first read not cancelled")
	}
	if err := ctx2.Err(); err != nil {
		t.Fatalf("second read already done: %v", err)
	}
}
