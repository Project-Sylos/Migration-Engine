// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package memory

import (
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/scaling/profile"
)

// memoryPressureMaxUsedFraction is the system RAM use ceiling (used/total) before
// classifying memory pressure and blocking buffer scale-up.
const memoryPressureMaxUsedFraction = 0.90

// memoryYellowUsedFraction warns as the host approaches the ceiling.
const memoryYellowUsedFraction = 0.80

// memoryPressureMinAvailableKB forces red when reclaimable RAM is critically low
// even if used% is still under the fraction ceiling (matches "almost out" UIs).
const memoryPressureMinAvailableKB = 512 * 1024 // 512 MiB

// Rough per-unit memory costs for projecting the next batch/seal increment (bytes).
const (
	estBytesPerLeaseSlot     = 16 * 1024
	estBytesPerRefillSlot    = 8 * 1024
	estBytesPerSealRow       = 512
	estBytesPerExpectedChild = 512
)

// YellowThresholdPercent is the used% where LevelFromSample turns yellow (80).
func YellowThresholdPercent() float64 {
	return memoryYellowUsedFraction * 100
}

// RedThresholdPercent is the used% where LevelFromSample turns red (90).
func RedThresholdPercent() float64 {
	return memoryPressureMaxUsedFraction * 100
}

// DisplayUsedPercent is max(avail-based, monitor-style) used fraction as 0..100.
// Matches the OR trigger in LevelFromSample so the operator graph colors when
// either signal crosses yellow/red.
func DisplayUsedPercent(s MemorySample) float64 {
	a := SystemUsedFraction(s) * 100
	m := SystemMonitorUsedFraction(s) * 100
	if m > a {
		return m
	}
	return a
}

// SystemUsedFraction returns (MemTotal - MemAvailable) / MemTotal, or 0 if unknown.
// This is the kernel's reclaimability-aware used fraction.
func SystemUsedFraction(s MemorySample) float64 {
	if s.MemTotalKB <= 0 {
		return 0
	}
	used := s.MemTotalKB - s.MemAvailableKB
	if used < 0 {
		used = 0
	}
	return float64(used) / float64(s.MemTotalKB)
}

// SystemMonitorUsedFraction returns (MemTotal - MemFree - Buffers - Cached) / MemTotal.
// Desktop system monitors often treat cache/buffers as "used"; this can read higher
// than SystemUsedFraction and is used as a second red/yellow trigger so we do not
// stay green while the UI already shows ~90%+.
// Returns 0 when free/buffers/cached were not sampled (tests / incomplete /proc).
func SystemMonitorUsedFraction(s MemorySample) float64 {
	if s.MemTotalKB <= 0 {
		return 0
	}
	if s.MemFreeKB == 0 && s.BuffersKB == 0 && s.CachedKB == 0 {
		return 0
	}
	used := s.MemTotalKB - s.MemFreeKB - s.BuffersKB - s.CachedKB
	if used < 0 {
		used = 0
	}
	return float64(used) / float64(s.MemTotalKB)
}

// ProjectedUsedFraction estimates system use after allocating extraKB in-process.
func ProjectedUsedFraction(s MemorySample, extraKB int64) float64 {
	if s.MemTotalKB <= 0 {
		return 0
	}
	used := s.MemTotalKB - s.MemAvailableKB + extraKB
	if used < 0 {
		used = 0
	}
	return float64(used) / float64(s.MemTotalKB)
}

// MemoryBudgetAllowsIncrease reports whether extraKB of buffer growth keeps host use below 90%.
func MemoryBudgetAllowsIncrease(s MemorySample, extraKB int64) bool {
	if s.MemTotalKB > 0 {
		return ProjectedUsedFraction(s, extraKB) < memoryPressureMaxUsedFraction
	}
	// Unknown host total: fall back to absolute MemAvailable heuristics only.
	return LevelFromSample(s) == MemoryGreen
}

type BatchIncrementKind int

const (
	BatchIncrementLease BatchIncrementKind = iota
	BatchIncrementRefill
	BatchIncrementSealRow
)

// EstimateBatchIncrementKB is the rough RAM delta for growing a batch knob.
func EstimateBatchIncrementKB(kind BatchIncrementKind, cur, next int) int64 {
	if next <= cur {
		return 0
	}
	var unit int64
	switch kind {
	case BatchIncrementLease:
		unit = estBytesPerLeaseSlot
	case BatchIncrementRefill:
		unit = estBytesPerRefillSlot
	case BatchIncrementSealRow:
		unit = estBytesPerSealRow
	}
	return int64(next-cur) * unit / 1024
}

// EstimateDstChildQuotaKB is the rough RAM for hydrated expected SRC children in a DST pull batch.
func EstimateDstChildQuotaKB(childQuota int) int64 {
	if childQuota <= 0 {
		return 0
	}
	return int64(childQuota) * estBytesPerExpectedChild / 1024
}

// FormatMemoryBudgetLine summarizes a sample for debug logs.
func FormatMemoryBudgetLine(s MemorySample) string {
	if s.MemTotalKB > 0 {
		return fmt.Sprintf(
			"host_used=%.1f%% monitor_used=%.1f%% (avail=%.1fGiB free=%.1fGiB total=%.1fGiB) proc_rss=%.1fGiB ceiling=%.0f%%",
			SystemUsedFraction(s)*100,
			SystemMonitorUsedFraction(s)*100,
			float64(s.MemAvailableKB)/1024/1024,
			float64(s.MemFreeKB)/1024/1024,
			float64(s.MemTotalKB)/1024/1024,
			float64(s.RSSKB)/1024/1024,
			memoryPressureMaxUsedFraction*100,
		)
	}
	return fmt.Sprintf("host_used=unknown avail=%.1fGiB proc_rss=%.1fGiB", float64(s.MemAvailableKB)/1024/1024, float64(s.RSSKB)/1024/1024)
}

// IncreaseTowardMaxQuarter grows cur by 25% of its current value toward max
// (memory-knob calm recovery; always slower than doubling).
func IncreaseTowardMaxQuarter(cur, min, max int) (int, bool) {
	if cur <= 0 {
		cur = min
	}
	if cur >= max {
		return cur, false
	}
	step := cur / 4
	if step < 1 {
		step = 1
	}
	next := profile.ClampInt(cur+step, min, max)
	return next, next > cur
}
