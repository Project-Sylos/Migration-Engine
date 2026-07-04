// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package scaling

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// MemoryLevel indicates host memory headroom for scale-up gating.
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
	RSSKB          int64
}

// MemorySampler reads host and process memory. Injectable for tests.
type MemorySampler interface {
	Sample() MemorySample
}

// DefaultMemorySampler reads Linux /proc when present.
var DefaultMemorySampler MemorySampler = procMemorySampler{}

type procMemorySampler struct{}

func (procMemorySampler) Sample() MemorySample {
	total, avail := readMemTotalAvailableKB()
	return MemorySample{
		MemTotalKB:     total,
		MemAvailableKB: avail,
		RSSKB:          readProcessRSSKB(),
	}
}

// LevelFromSample maps host + process memory to green/yellow/red.
// Red primarily means host RAM use is at/above 90%; process RSS alone does not trigger red
// when the host still has headroom.
func LevelFromSample(s MemorySample) MemoryLevel {
	if s.MemTotalKB > 0 {
		used := SystemUsedFraction(s)
		if used >= memoryPressureMaxUsedFraction {
			return MemoryRed
		}
		if used >= memoryYellowUsedFraction {
			return MemoryYellow
		}
		return MemoryGreen
	}
	// Fallback when MemTotal is unavailable (non-Linux / restricted /proc).
	if s.MemAvailableKB > 0 {
		if s.MemAvailableKB < 512*1024 {
			return MemoryRed
		}
		if s.MemAvailableKB < 2*1024*1024 {
			return MemoryYellow
		}
	}
	return MemoryGreen
}

// SampleMemoryLevel reads process RSS and system memory when /proc is present.
func SampleMemoryLevel() MemoryLevel {
	return LevelFromSample(DefaultMemorySampler.Sample())
}

func readMemTotalAvailableKB() (totalKB, availKB int64) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "MemTotal:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				totalKB, _ = strconv.ParseInt(fields[1], 10, 64)
			}
		}
		if strings.HasPrefix(line, "MemAvailable:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				availKB, _ = strconv.ParseInt(fields[1], 10, 64)
			}
		}
	}
	if err := sc.Err(); err != nil {
		return 0, 0
	}
	return totalKB, availKB
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

// ScaleUpAllowed returns true when memory headroom permits increasing throughput knobs.
func ScaleUpAllowed(level MemoryLevel) bool {
	return level == MemoryGreen
}
