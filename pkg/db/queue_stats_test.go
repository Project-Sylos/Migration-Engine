// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"testing"
	"time"
)

func TestQueueStatsAppendAndLatest(t *testing.T) {
	database, err := Open(Options{Path: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	ctx := context.Background()
	if err := database.RunWrite(ctx, func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			return w.AppendQueueStats("src-traversal", QueueStatsPhaseTraversal, `{"files_discovered_total":10}`)
		})
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	if err := database.RunWrite(ctx, func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			if err := w.AppendQueueStats("src-traversal", QueueStatsPhaseTraversal, `{"files_discovered_total":20}`); err != nil {
				return err
			}
			return w.PruneQueueStats()
		})
	}); err != nil {
		t.Fatal(err)
	}

	latest, err := database.GetLatestQueueStats("src-traversal", QueueStatsPhaseTraversal)
	if err != nil {
		t.Fatal(err)
	}
	if string(latest) != `{"files_discovered_total":20}` {
		t.Fatalf("latest metrics = %q, want 20", string(latest))
	}

	all, err := database.GetAllQueueStats()
	if err != nil {
		t.Fatal(err)
	}
	if got := string(all["src-traversal"]); got != `{"files_discovered_total":20}` {
		t.Fatalf("GetAllQueueStats src-traversal = %q", got)
	}
}

func TestQueueStatsPhaseFamilies(t *testing.T) {
	database, err := Open(Options{Path: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	ctx := context.Background()
	if err := database.RunWrite(ctx, func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			if err := w.AppendQueueStats("copy", QueueStatsPhaseCopy, `{"files":5}`); err != nil {
				return err
			}
			if err := w.AppendQueueStats("delete", QueueStatsPhaseDelete, `{"files":2,"folders":1}`); err != nil {
				return err
			}
			if err := w.AppendQueueStats("src-traversal", QueueStatsPhaseTraversal, `{"files_discovered_total":3}`); err != nil {
				return err
			}
			return w.PruneQueueStats()
		})
	}); err != nil {
		t.Fatal(err)
	}

	all, err := database.GetAllQueueStats()
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
