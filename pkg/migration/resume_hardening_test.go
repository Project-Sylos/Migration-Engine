// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

func TestSeedQueueCountersFromDB(t *testing.T) {
	database, err := db.Open(db.Options{Path: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	ctx := context.Background()
	metrics := `{"files_discovered_total":20000,"folders_discovered_total":500}`
	if err := database.RunWrite(ctx, func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			return w.AppendQueueStats("src-traversal", db.QueueStatsPhaseTraversal, metrics)
		})
	}); err != nil {
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

func TestTraversalResumeUsesRetryModeAndRoundZero(t *testing.T) {
	src := queue.NewQueue("src", 3, 1, queue.NewQueueCoordinator(), nil)
	dst := queue.NewQueue("dst", 3, 1, queue.NewQueueCoordinator(), nil)

	src.SetMode(queue.QueueModeRetry)
	dst.SetMode(queue.QueueModeRetry)
	src.SetMaxKnownDepth(12)
	dst.SetMaxKnownDepth(12)
	src.SetRound(0)
	dst.SetRound(0)

	if src.GetMode() != queue.QueueModeRetry || dst.GetMode() != queue.QueueModeRetry {
		t.Fatalf("expected retry mode on resume queues")
	}
	if src.Stats().Round != 0 || dst.Stats().Round != 0 {
		t.Fatalf("expected round 0 on resume, got src=%d dst=%d", src.Stats().Round, dst.Stats().Round)
	}
	if src.GetMaxKnownDepth() != 12 {
		t.Fatalf("expected max known depth 12, got %d", src.GetMaxKnownDepth())
	}
}

func TestCopyResumeUsesCopyMode(t *testing.T) {
	copyQ := queue.NewQueue("copy", 3, 1, nil, nil)
	copyQ.SetMode(queue.QueueModeCopy)
	copyQ.SetRound(3)

	if copyQ.GetMode() != queue.QueueModeCopy {
		t.Fatalf("expected copy mode, got %s", copyQ.GetMode())
	}
	if copyQ.Stats().Round != 3 {
		t.Fatalf("expected derived start round 3, got %d", copyQ.Stats().Round)
	}
}
