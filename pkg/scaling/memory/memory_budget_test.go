// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package memory

import "testing"

func TestSystemUsedFraction(t *testing.T) {
	s := MemorySample{MemTotalKB: 1000, MemAvailableKB: 100}
	if got := SystemUsedFraction(s); got < 0.89 || got > 0.91 {
		t.Fatalf("fraction=%v want ~0.9", got)
	}
}
