// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import "testing"

type fakeMemorySampler struct {
	sample MemorySample
}

func (f fakeMemorySampler) Sample() MemorySample { return f.sample }

func TestLevelFromSample(t *testing.T) {
	tests := []struct {
		name string
		s    MemorySample
		want MemoryLevel
	}{
		{
			"green host 50pct",
			MemorySample{MemTotalKB: 32 * 1024 * 1024, MemAvailableKB: 16 * 1024 * 1024, RSSKB: 5 * 1024 * 1024},
			MemoryGreen,
		},
		{
			"yellow host 85pct",
			MemorySample{MemTotalKB: 32 * 1024 * 1024, MemAvailableKB: 4800 * 1024, RSSKB: 5 * 1024 * 1024},
			MemoryYellow,
		},
		{
			"red host 92pct",
			MemorySample{MemTotalKB: 32 * 1024 * 1024, MemAvailableKB: 2500 * 1024, RSSKB: 5 * 1024 * 1024},
			MemoryRed,
		},
		{
			"high rss plenty host stays green",
			MemorySample{MemTotalKB: 32 * 1024 * 1024, MemAvailableKB: 16 * 1024 * 1024, RSSKB: 8 * 1024 * 1024},
			MemoryGreen,
		},
		{
			"fallback red low avail",
			MemorySample{MemAvailableKB: 400 * 1024},
			MemoryRed,
		},
		{"unknown proc", MemorySample{}, MemoryGreen},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := LevelFromSample(tc.s); got != tc.want {
				t.Fatalf("LevelFromSample()=%v want %v", got, tc.want)
			}
		})
	}
}

func TestScaleUpAllowed(t *testing.T) {
	if !ScaleUpAllowed(MemoryGreen) {
		t.Fatal("green should allow scale up")
	}
	if ScaleUpAllowed(MemoryYellow) || ScaleUpAllowed(MemoryRed) {
		t.Fatal("yellow/red should block scale up")
	}
}

func TestMemoryBudgetAllowsIncrease(t *testing.T) {
	sample := MemorySample{MemTotalKB: 32 * 1024 * 1024, MemAvailableKB: 16 * 1024 * 1024}
	if !MemoryBudgetAllowsIncrease(sample, 0) {
		t.Fatal("50% host use should allow increase")
	}
	// +12GiB projected: 16+12=28 used of 32 = 87.5% still ok; +14GiB -> 93.75% blocked
	if !MemoryBudgetAllowsIncrease(sample, 12*1024*1024) {
		t.Fatal("moderate increment should still be allowed at 50% host use")
	}
	if MemoryBudgetAllowsIncrease(sample, 14*1024*1024) {
		t.Fatal("large increment should be blocked before 90% ceiling")
	}
}
