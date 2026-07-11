// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import "testing"

func TestQueueStatsKeyForAPI(t *testing.T) {
	tests := map[string]string{
		"src":    "src-traversal",
		"dst":    "dst-traversal",
		"copy":   "copy",
		"delete": "delete",
	}
	for in, want := range tests {
		if got := queueStatsKeyForAPI(in); got != want {
			t.Fatalf("queueStatsKeyForAPI(%q) = %q, want %q", in, got, want)
		}
	}
}
