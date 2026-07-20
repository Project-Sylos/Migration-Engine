// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"testing"
	"time"
)

func TestListMergedReviewDiffsExcludeDestinationOnly(t *testing.T) {
	database, err := Open(Options{Path: t.TempDir() + "/dst-only.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	eventTime := time.Now().UnixNano()
	srcBoth := &NodeState{
		ID:   DeterministicNodeID("SRC", NodeTypeFile, "/both.txt"),
		Path: "/both.txt", ParentPath: "/", Name: "both.txt",
		Type: NodeTypeFile, Depth: 1, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusSuccessful,
	}
	srcOnly := &NodeState{
		ID:   DeterministicNodeID("SRC", NodeTypeFile, "/src-only.txt"),
		Path: "/src-only.txt", ParentPath: "/", Name: "src-only.txt",
		Type: NodeTypeFile, Depth: 1, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusPending,
	}
	dstBoth := &NodeState{
		ID:   DeterministicNodeID("DST", NodeTypeFile, "/both.txt"),
		Path: "/both.txt", ParentPath: "/", Name: "both.txt",
		Type: NodeTypeFile, Depth: 1, TraversalStatus: StatusSuccessful,
	}
	dstOnly := &NodeState{
		ID:   DeterministicNodeID("DST", NodeTypeFile, "/dst-only.txt"),
		Path: "/dst-only.txt", ParentPath: "/", Name: "dst-only.txt",
		Type: NodeTypeFile, Depth: 1, TraversalStatus: StatusNotOnSrc,
	}

	err = database.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			if err := w.AppenderInsert(tableSrcNodes, []*NodeState{srcBoth, srcOnly}); err != nil {
				return err
			}
			if err := w.AppenderInsert(tableDstNodes, []*NodeState{dstBoth, dstOnly}); err != nil {
				return err
			}
			srcEvents := []StatusEvent{
				{ID: srcBoth.ID, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusSuccessful, EventTime: eventTime, Depth: 1},
				{ID: srcOnly.ID, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusPending, EventTime: eventTime, Depth: 1},
			}
			dstEvents := []StatusEvent{
				{ID: dstBoth.ID, TraversalStatus: StatusSuccessful, EventTime: eventTime, Depth: 1},
				{ID: dstOnly.ID, TraversalStatus: StatusNotOnSrc, EventTime: eventTime, Depth: 1},
			}
			if err := w.BatchInsertSrcStatusEvents(srcEvents); err != nil {
				return err
			}
			return w.BatchInsertDstStatusEvents(dstEvents)
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	all, totalAll, err := ListMergedReviewDiffs(database, ReviewFilter{ParentPath: "/"}, "path ASC", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if totalAll != 3 {
		t.Fatalf("include all: total=%d want 3 (got rows=%d)", totalAll, len(all))
	}

	filtered, totalFiltered, err := ListMergedReviewDiffs(database, ReviewFilter{
		ParentPath:             "/",
		ExcludeDestinationOnly: true,
	}, "path ASC", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if totalFiltered != 2 {
		t.Fatalf("exclude dst-only: total=%d want 2", totalFiltered)
	}
	if len(filtered) != 2 {
		t.Fatalf("exclude dst-only: rows=%d want 2", len(filtered))
	}
	for _, row := range filtered {
		if row.SrcNodeID == "" {
			t.Fatalf("unexpected destination-only row in filtered results: %+v", row)
		}
		if row.Path == "/dst-only.txt" {
			t.Fatalf("destination-only path still present: %s", row.Path)
		}
	}

	stats, err := GetMergedReviewStats(database, ReviewFilter{
		ParentPath:             "/",
		ExcludeDestinationOnly: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Total != 2 {
		t.Fatalf("stats.Total=%d want 2", stats.Total)
	}
}
