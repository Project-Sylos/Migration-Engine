// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package stats

import (
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func TestQueueStatsAppendAndLatest(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/queue-stats.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	if err := database.AppendQueueStats("src-traversal", db.QueueStatsPhaseTraversal, `{"files_discovered_total":10}`); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	if err := database.AppendQueueStats("src-traversal", db.QueueStatsPhaseTraversal, `{"files_discovered_total":20}`); err != nil {
		t.Fatal(err)
	}

	latest, err := GetLatestQueueStats(database, "src-traversal", db.QueueStatsPhaseTraversal)
	if err != nil {
		t.Fatal(err)
	}
	if string(latest) != `{"files_discovered_total":20}` {
		t.Fatalf("latest metrics = %q, want 20", string(latest))
	}

	all, err := GetAllQueueStats(database)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(all["src-traversal"]); got != `{"files_discovered_total":20}` {
		t.Fatalf("GetAllQueueStats src-traversal = %q", got)
	}
}

func TestQueueStatsPhaseFamilies(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/queue-stats-phases.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	if err := database.AppendQueueStats("copy", db.QueueStatsPhaseCopy, `{"files":5}`); err != nil {
		t.Fatal(err)
	}
	if err := database.AppendQueueStats("delete", db.QueueStatsPhaseDelete, `{"files":2,"folders":1}`); err != nil {
		t.Fatal(err)
	}
	if err := database.AppendQueueStats("src-traversal", db.QueueStatsPhaseTraversal, `{"files_discovered_total":3}`); err != nil {
		t.Fatal(err)
	}

	all, err := GetAllQueueStats(database)
	if err != nil {
		t.Fatal(err)
	}
	if string(all["copy"]) != `{"files":5}` {
		t.Fatalf("copy metrics = %q", string(all["copy"]))
	}
	if string(all["delete"]) != `{"files":2,"folders":1}` {
		t.Fatalf("delete metrics = %q", string(all["delete"]))
	}
	if string(all["src-traversal"]) != `{"files_discovered_total":3}` {
		t.Fatalf("src-traversal metrics = %q", string(all["src-traversal"]))
	}
}
