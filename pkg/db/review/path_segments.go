// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"strings"
)

// NormalizePathSegments trims empties; order preserved.
func NormalizePathSegments(segs []string) []string {
	if len(segs) == 0 {
		return nil
	}
	out := make([]string, 0, len(segs))
	for _, s := range segs {
		s = strings.TrimSpace(s)
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

// pathSegmentsForFilter returns PathSegments, or a single segment from Query when
// the caller expressed a path search as QueryField "path" instead.
func pathSegmentsForFilter(f ReviewFilter) []string {
	segs := NormalizePathSegments(f.PathSegments)
	if len(segs) > 0 {
		return segs
	}
	if strings.EqualFold(strings.TrimSpace(f.QueryField), "path") {
		q := strings.TrimSpace(f.Query)
		if q != "" {
			return []string{q}
		}
	}
	return nil
}
