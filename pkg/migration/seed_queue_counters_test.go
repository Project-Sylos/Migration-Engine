// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

func TestSeedQueueCountersFromDB(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/seed-counters.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	metrics := `{"files_discovered_total":20000,"folders_discovered_total":500}`
	if err := database.AppendQueueStats("src-traversal", db.QueueStatsPhaseTraversal, metrics); err != nil {
		t.Fatal(err)
	}

	q := queue.NewQueue("src", 3, 1, nil, nil)
	seedQueueCountersFromDB(database, q, "src-traversal", db.QueueStatsPhaseTraversal)

	if got := q.GetFilesDiscoveredTotal(); got != 20000 {
		t.Fatalf("seeded files = %d, want 20000", got)
	}
	if got := q.GetFoldersDiscoveredTotal(); got != 500 {
		t.Fatalf("seeded folders = %d, want 500", got)
	}
}
