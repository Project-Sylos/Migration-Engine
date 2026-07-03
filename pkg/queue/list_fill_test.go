// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import "testing"

func TestListFillP95(t *testing.T) {
	var tr listFillTracker
	for i := 0; i < listFillMinSamples-1; i++ {
		tr.record(50)
	}
	if tr.p95() != 0 {
		t.Fatal("expected 0 p95 before min samples")
	}
	tr.record(50)
	for i := 0; i < 9; i++ {
		tr.record(500)
	}
	if got := tr.p95(); got != 500 {
		t.Fatalf("p95=%d want 500", got)
	}
}

func TestP95Index(t *testing.T) {
	if p95Index(10) != 9 {
		t.Fatalf("p95Index(10)=%d want 9", p95Index(10))
	}
	if p95Index(20) != 18 {
		t.Fatalf("p95Index(20)=%d want 18", p95Index(20))
	}
}
