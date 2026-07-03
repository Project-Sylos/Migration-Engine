// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import "testing"

func TestIncreaseTowardMax(t *testing.T) {
	next, ok := increaseTowardMax(500, 100, 1000, 100)
	if !ok || next != 1000 {
		t.Fatalf("got (%d,%v) want (1000,true)", next, ok)
	}
	next, ok = increaseTowardMax(1000, 100, 1000, 100)
	if ok {
		t.Fatalf("at max should not increase")
	}
}

func TestSystemUsedFraction(t *testing.T) {
	s := MemorySample{MemTotalKB: 1000, MemAvailableKB: 100}
	if got := SystemUsedFraction(s); got < 0.89 || got > 0.91 {
		t.Fatalf("fraction=%v want ~0.9", got)
	}
}
