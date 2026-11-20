// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import "strings"

// Overwrite policy values for DST file compare / start options.
const (
	OverwriteNewestMTime = "newest_mtime"
	OverwriteDecide      = "decide"
	OverwriteSourceWins  = "source_wins"
)

// NormalizeOverwritePolicy returns a known policy or newest_mtime when empty/unknown.
func NormalizeOverwritePolicy(policy string) string {
	switch strings.ToLower(strings.TrimSpace(policy)) {
	case OverwriteDecide:
		return OverwriteDecide
	case OverwriteSourceWins:
		return OverwriteSourceWins
	default:
		return OverwriteNewestMTime
	}
}
