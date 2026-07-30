// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package profile

import "testing"

func TestListPageIncreaseAllowed(t *testing.T) {
	if ListPageIncreaseAllowed(100, 0) {
		t.Fatal("zero p95 should not allow increase")
	}
	if ListPageIncreaseAllowed(100, 80) {
		t.Fatal("p95 below page size should not allow increase")
	}
	if !ListPageIncreaseAllowed(100, 150) {
		t.Fatal("p95 above page size should allow increase")
	}
	if ListPageIncreaseAllowed(100, 100) {
		t.Fatal("p95 equal to page size should not allow increase")
	}
}

func TestIncreaseListPageSize(t *testing.T) {
	next, ok := IncreaseListPageSize(100, 20, 10000, 20)
	if !ok || next != 200 {
		t.Fatalf("got (%d,%v) want (200,true)", next, ok)
	}
	next, ok = IncreaseListPageSize(8000, 20, 10000, 20)
	if !ok || next != 10000 {
		t.Fatalf("cap: got (%d,%v)", next, ok)
	}
}

func TestDecreaseListPageSize(t *testing.T) {
	next, ok := DecreaseListPageSize(400, 20, 100)
	if !ok || next != 200 {
		t.Fatalf("halve: got (%d,%v) want (200,true)", next, ok)
	}
	next, ok = DecreaseListPageSize(200, 20, 100)
	if !ok || next != 100 {
		t.Fatalf("to default: got (%d,%v) want (100,true)", next, ok)
	}
}
