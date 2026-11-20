// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package memory

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// MemoryLevel indicates host memory headroom for batch/seal scale-up gating.
type MemoryLevel int

const (
	MemoryGreen MemoryLevel = iota
	MemoryYellow
	MemoryRed
)

// MemorySample is a point-in-time memory snapshot (autoscaler tick only).
type MemorySample struct {
	MemTotalKB     int64
	MemAvailableKB int64
	// MemFreeKB / BuffersKB / CachedKB come from /proc/meminfo for a
	// "cache counted as used" fraction that often matches desktop system monitors
	// more closely than MemAvailable alone.
	MemFreeKB int64
	BuffersKB int64
	CachedKB  int64
	RSSKB     int64
}

// MemorySampler reads host and process memory. Injectable for tests.
type MemorySampler interface {
	Sample() MemorySample
}

// DefaultMemorySampler reads Linux /proc when present.
var DefaultMemorySampler MemorySampler = procMemorySampler{}

type procMemorySampler struct{}

func (procMemorySampler) Sample() MemorySample {
	total, avail, free, buffers, cached := readMeminfoKB()
	return MemorySample{
		MemTotalKB:     total,
		MemAvailableKB: avail,
		MemFreeKB:      free,
		BuffersKB:      buffers,
		CachedKB:       cached,
		RSSKB:          readProcessRSSKB(),
	}
}

// LevelFromSample maps host memory to green/yellow/red.
// Red when either MemAvailable-based use is at/above 90%, or the
// free+buffers+cached "monitor-style" use is at/above 90%, or absolute
// MemAvailable is critically low. Process RSS alone does not trigger red.
func LevelFromSample(s MemorySample) MemoryLevel {
	if s.MemTotalKB > 0 {
		availUsed := SystemUsedFraction(s)
		monitorUsed := SystemMonitorUsedFraction(s)
		if availUsed >= memoryPressureMaxUsedFraction || monitorUsed >= memoryPressureMaxUsedFraction {
			return MemoryRed
		}
		// Absolute floor on large hosts: under 90% used can still mean almost no reclaimable RAM.
		// Skip on tiny totals (unit tests / constrained VMs) where 512MiB would always fire.
		if s.MemAvailableKB > 0 && s.MemAvailableKB < memoryPressureMinAvailableKB &&
			s.MemTotalKB >= 4*1024*1024 {
			return MemoryRed
		}
		if availUsed >= memoryYellowUsedFraction || monitorUsed >= memoryYellowUsedFraction {
			return MemoryYellow
		}
		return MemoryGreen
	}
	// Fallback when MemTotal is unavailable (non-Linux / restricted /proc).
	if s.MemAvailableKB > 0 {
		if s.MemAvailableKB < memoryPressureMinAvailableKB {
			return MemoryRed
		}
		if s.MemAvailableKB < 2*1024*1024 {
			return MemoryYellow
		}
	}
	return MemoryGreen
}

func readMeminfoKB() (totalKB, availKB, freeKB, buffersKB, cachedKB int64) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, 0, 0, 0, 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		v, _ := strconv.ParseInt(fields[1], 10, 64)
		switch {
		case strings.HasPrefix(line, "MemTotal:"):
			totalKB = v
		case strings.HasPrefix(line, "MemAvailable:"):
			availKB = v
		case strings.HasPrefix(line, "MemFree:"):
			freeKB = v
		case strings.HasPrefix(line, "Buffers:"):
			buffersKB = v
		case strings.HasPrefix(line, "Cached:"):
			cachedKB = v
		}
	}
	if err := sc.Err(); err != nil {
		return 0, 0, 0, 0, 0
	}
	return totalKB, availKB, freeKB, buffersKB, cachedKB
}

func readProcessRSSKB() int64 {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				v, _ := strconv.ParseInt(fields[1], 10, 64)
				return v
			}
		}
	}
	if err := sc.Err(); err != nil {
		return 0
	}
	return 0
}

// ScaleUpAllowed returns true when memory headroom permits increasing batch/seal knobs.
// Worker scaling does not use this gate.
func ScaleUpAllowed(level MemoryLevel) bool {
	return level == MemoryGreen
}

// String returns a stable operator-facing name for the level.
func (l MemoryLevel) String() string {
	switch l {
	case MemoryYellow:
		return "yellow"
	case MemoryRed:
		return "red"
	default:
		return "green"
	}
}
