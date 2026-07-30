// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package observe

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

func TestPhaseFamilyForMode(t *testing.T) {
	tests := []struct {
		mode queue.QueueMode
		want string
	}{
		{queue.QueueModeTraversal, "traversal"},
		{queue.QueueModeRetry, "traversal"},
		{queue.QueueModeCopy, "copy"},
		{queue.QueueModeCopyRetry, "copy"},
	}
	for _, tc := range tests {
		if got := PhaseFamilyForMode(tc.mode); got != tc.want {
			t.Fatalf("PhaseFamilyForMode(%q) = %q, want %q", tc.mode, got, tc.want)
		}
	}
}

func TestRehydrateCountersFromMetricsJSON(t *testing.T) {
	src := queue.NewQueue("src", 3, 1, nil, nil)
	payload := []byte(`{"files_discovered_total":120,"folders_discovered_total":30}`)
	if err := RehydrateCountersFromMetricsJSON(src, payload); err != nil {
		t.Fatal(err)
	}
	if got := src.GetFilesDiscoveredTotal(); got != 120 {
		t.Fatalf("files discovered = %d, want 120", got)
	}
	if got := src.GetFoldersDiscoveredTotal(); got != 30 {
		t.Fatalf("folders discovered = %d, want 30", got)
	}

	copyQ := queue.NewQueue("copy", 3, 1, nil, nil)
	copyPayload := []byte(`{"folders":4,"files":9,"bytes":1024}`)
	if err := RehydrateCountersFromMetricsJSON(copyQ, copyPayload); err != nil {
		t.Fatal(err)
	}
	if got := copyQ.GetFoldersCreatedTotal(); got != 4 {
		t.Fatalf("folders created = %d, want 4", got)
	}
	if got := copyQ.GetFilesCreatedTotal(); got != 9 {
		t.Fatalf("files created = %d, want 9", got)
	}
	if got := copyQ.GetBytesTransferredTotal(); got != 1024 {
		t.Fatalf("bytes = %d, want 1024", got)
	}
}
