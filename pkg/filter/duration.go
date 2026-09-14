// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package filter

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ParseRelativeDuration parses values like "5y", "6m", "90d", "12h".
// Months are approximated as 30 days; years as 365 days.
func ParseRelativeDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}
	i := 0
	for i < len(s) && (s[i] >= '0' && s[i] <= '9') {
		i++
	}
	if i == 0 || i == len(s) {
		return 0, fmt.Errorf("invalid duration %q", s)
	}
	n, err := strconv.ParseInt(s[:i], 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid duration amount %q", s)
	}
	unit := s[i:]
	switch unit {
	case "y", "yr", "year", "years":
		return time.Duration(n) * 365 * 24 * time.Hour, nil
	case "m", "mo", "month", "months":
		return time.Duration(n) * 30 * 24 * time.Hour, nil
	case "w", "week", "weeks":
		return time.Duration(n) * 7 * 24 * time.Hour, nil
	case "d", "day", "days":
		return time.Duration(n) * 24 * time.Hour, nil
	case "h", "hr", "hour", "hours":
		return time.Duration(n) * time.Hour, nil
	default:
		return 0, fmt.Errorf("unknown duration unit %q", unit)
	}
}

// ParseNodeMTime parses common provider / RFC3339 timestamps.
func ParseNodeMTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty mtime")
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	var last error
	for _, layout := range layouts {
		t, err := time.Parse(layout, s)
		if err == nil {
			return t, nil
		}
		last = err
	}
	return time.Time{}, fmt.Errorf("parse mtime %q: %w", s, last)
}
