// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

const (
	// MinMemoryLimitGB is the floor for auto and explicit DuckDB memory_limit.
	MinMemoryLimitGB = 4
	// AutoMaxMemoryLimitGB caps host-derived defaults (UI may raise further).
	AutoMaxMemoryLimitGB = 12
	// MaxMemoryLimitGB is the hard ceiling for PRAGMA memory_limit.
	MaxMemoryLimitGB = 64
)

// DefaultMemoryLimitGB picks a DuckDB memory_limit from host free RAM.
// Uses half of MemAvailable (Linux), clamped to [MinMemoryLimitGB, AutoMaxMemoryLimitGB].
// Falls back to MinMemoryLimitGB when free memory cannot be read.
func DefaultMemoryLimitGB() int {
	avail := memAvailableGB()
	if avail <= 0 {
		return MinMemoryLimitGB
	}
	half := avail / 2
	if half < MinMemoryLimitGB {
		return MinMemoryLimitGB
	}
	if half > AutoMaxMemoryLimitGB {
		return AutoMaxMemoryLimitGB
	}
	return half
}

// ClampMemoryLimitGB normalizes an explicit limit. Values below MinMemoryLimitGB
// are raised to the minimum; values above MaxMemoryLimitGB are capped.
func ClampMemoryLimitGB(gb int) int {
	if gb < MinMemoryLimitGB {
		return MinMemoryLimitGB
	}
	if gb > MaxMemoryLimitGB {
		return MaxMemoryLimitGB
	}
	return gb
}

// ResolveMemoryLimitGB returns ClampMemoryLimitGB(gb) when gb > 0, else DefaultMemoryLimitGB().
func ResolveMemoryLimitGB(gb int) int {
	if gb <= 0 {
		return DefaultMemoryLimitGB()
	}
	return ClampMemoryLimitGB(gb)
}

func memAvailableGB() int {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "MemAvailable:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil || kb <= 0 {
			return 0
		}
		return int(kb / (1024 * 1024))
	}
	if err := sc.Err(); err != nil {
		return 0
	}
	return 0
}
