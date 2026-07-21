// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"testing"
	"time"
)

func TestListMergedReviewDiffsPathIssueFilter(t *testing.T) {
	database, err := Open(Options{Path: t.TempDir() + "/path-issue-filter.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	eventTime := time.Now().UnixNano()
	clean := &NodeState{
		ID: DeterministicNodeID("SRC", NodeTypeFile, "/clean.txt"),
		Path: "/clean.txt", ParentPath: "/", Name: "clean.txt",
		Type: NodeTypeFile, Depth: 1, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusPending,
	}
	issue := &NodeState{
		ID: DeterministicNodeID("SRC", NodeTypeFile, "/bad:name.txt"),
		Path: "/bad:name.txt", ParentPath: "/", Name: "bad:name.txt",
		Type: NodeTypeFile, Depth: 1, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusPending,
	}
	accepted := &NodeState{
		ID: DeterministicNodeID("SRC", NodeTypeFile, "/accepted.txt"),
		Path: "/accepted.txt", ParentPath: "/", Name: "accepted.txt",
		Type: NodeTypeFile, Depth: 1, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusPending,
	}
	rejected := &NodeState{
		ID: DeterministicNodeID("SRC", NodeTypeFile, "/ignored.txt"),
		Path: "/ignored.txt", ParentPath: "/", Name: "ignored.txt",
		Type: NodeTypeFile, Depth: 1, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusPending,
	}
	manual := &NodeState{
		ID: DeterministicNodeID("SRC", NodeTypeFile, "/*"),
		Path: "/*", ParentPath: "/", Name: "*",
		Type: NodeTypeFile, Depth: 1, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusPending,
	}

	err = database.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			if err := w.AppenderInsert(tableSrcNodes, []*NodeState{clean, issue, accepted, rejected, manual}); err != nil {
				return err
			}
			events := []StatusEvent{
				{ID: clean.ID, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusPending, EventTime: eventTime, Depth: 1},
				{ID: issue.ID, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusPending, EventTime: eventTime, Depth: 1},
				{ID: accepted.ID, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusPending, EventTime: eventTime, Depth: 1},
				{ID: rejected.ID, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusPending, GPLStatus: GPLStatusIgnored, EventTime: eventTime, Depth: 1},
				{ID: manual.ID, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusPending, EventTime: eventTime, Depth: 1},
			}
			if err := w.BatchInsertSrcStatusEvents(events); err != nil {
				return err
			}
			return w.BatchInsertPathEvents([]PathEvent{
				{ID: issue.ID, EventTime: eventTime, Category: PathEventCategoryGPLClean, ProposedPath: "bad_name.txt", Status: PathEventStatusPending},
				{ID: accepted.ID, EventTime: eventTime, Category: PathEventCategoryGPLClean, ProposedPath: "accepted.txt", Status: PathEventStatusAccepted},
				{ID: rejected.ID, EventTime: eventTime, Category: PathEventCategoryGPLClean, ProposedPath: "ignored.txt", Status: PathEventStatusPending},
				{ID: manual.ID, EventTime: eventTime, Category: PathEventCategoryGPLClean, ProposedPath: "", Status: PathEventStatusManualReview},
			})
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	mustNames := func(filter string) []string {
		t.Helper()
		rows, total, err := ListMergedReviewDiffs(database, ReviewFilter{ParentPath: "/", PathIssueFilter: filter}, "path ASC", 100, 0)
		if err != nil {
			t.Fatalf("filter %q: %v", filter, err)
		}
		names := make([]string, 0, len(rows))
		for _, r := range rows {
			names = append(names, r.Name)
		}
		if total != len(rows) {
			t.Fatalf("filter %q: total %d != rows %d", filter, total, len(rows))
		}
		return names
	}

	assertOnly := func(filter string, want ...string) {
		t.Helper()
		got := mustNames(filter)
		if len(got) != len(want) {
			t.Fatalf("filter %q: got %v want %v", filter, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("filter %q: got %v want %v", filter, got, want)
			}
		}
	}

	assertOnly("issues", "bad:name.txt")
	assertOnly("manual", "*")
	assertOnly("accepted", "accepted.txt")
	assertOnly("rejected", "ignored.txt")
	assertOnly("none", "clean.txt")

	rows, _, err := ListMergedReviewDiffs(database, ReviewFilter{ParentPath: "/"}, "path ASC", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]MergedReviewRow{}
	for _, r := range rows {
		byName[r.Name] = r
	}
	if got := byName["accepted.txt"].ResolvedDstName; got != "accepted.txt" {
		t.Fatalf("accepted ResolvedDstName=%q want accepted.txt", got)
	}
	if got := byName["bad:name.txt"].ResolvedDstName; got != "" {
		t.Fatalf("pending issue ResolvedDstName=%q want empty", got)
	}
	if got := byName["clean.txt"].ResolvedDstName; got != "" {
		t.Fatalf("clean ResolvedDstName=%q want empty", got)
	}
}
