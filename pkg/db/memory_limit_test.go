// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import "testing"

func TestClampAndResolveMemoryLimitGB(t *testing.T) {
	if got := ClampMemoryLimitGB(1); got != MinMemoryLimitGB {
		t.Fatalf("Clamp(1)=%d want %d", got, MinMemoryLimitGB)
	}
	if got := ClampMemoryLimitGB(100); got != MaxMemoryLimitGB {
		t.Fatalf("Clamp(100)=%d want %d", got, MaxMemoryLimitGB)
	}
	if got := ClampMemoryLimitGB(16); got != 16 {
		t.Fatalf("Clamp(16)=%d want 16", got)
	}
	auto := DefaultMemoryLimitGB()
	if auto < MinMemoryLimitGB || auto > AutoMaxMemoryLimitGB {
		t.Fatalf("DefaultMemoryLimitGB=%d out of [%d,%d]", auto, MinMemoryLimitGB, AutoMaxMemoryLimitGB)
	}
	if got := ResolveMemoryLimitGB(0); got != auto {
		t.Fatalf("Resolve(0)=%d want auto %d", got, auto)
	}
	if got := ResolveMemoryLimitGB(24); got != 24 {
		t.Fatalf("Resolve(24)=%d want 24", got)
	}
}
