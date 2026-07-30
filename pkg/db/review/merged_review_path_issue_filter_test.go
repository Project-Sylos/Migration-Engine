// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	_ "codeberg.org/Sylos/Migration-Engine/pkg/db/seal"
	"context"
	"testing"
	"time"
)

func TestListMergedReviewDiffsPathIssueFilter(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/path-issue-filter.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	eventTime := time.Now().UnixNano()
	clean := &db.NodeState{
		ID:   db.DeterministicNodeID("SRC", db.NodeTypeFile, "/clean.txt"),
		Path: "/clean.txt", ParentPath: "/", Name: "clean.txt",
		Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}
	issue := &db.NodeState{
		ID:   db.DeterministicNodeID("SRC", db.NodeTypeFile, "/bad:name.txt"),
		Path: "/bad:name.txt", ParentPath: "/", Name: "bad:name.txt",
		Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}
	accepted := &db.NodeState{
		ID:   db.DeterministicNodeID("SRC", db.NodeTypeFile, "/accepted.txt"),
		Path: "/accepted.txt", ParentPath: "/", Name: "accepted.txt",
		Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}
	rejected := &db.NodeState{
		ID:   db.DeterministicNodeID("SRC", db.NodeTypeFile, "/ignored.txt"),
		Path: "/ignored.txt", ParentPath: "/", Name: "ignored.txt",
		Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}
	manual := &db.NodeState{
		ID:   db.DeterministicNodeID("SRC", db.NodeTypeFile, "/*"),
		Path: "/*", ParentPath: "/", Name: "*",
		Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}

	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert(db.TableSrcNodes, []*db.NodeState{clean, issue, accepted, rejected, manual}); err != nil {
				return err
			}
			events := []db.StatusEvent{
				{ID: clean.ID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, EventTime: eventTime, Depth: 1},
				{ID: issue.ID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, EventTime: eventTime, Depth: 1},
				{ID: accepted.ID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, EventTime: eventTime, Depth: 1},
				{ID: rejected.ID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, GPLStatus: db.GPLStatusIgnored, EventTime: eventTime, Depth: 1},
				{ID: manual.ID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, EventTime: eventTime, Depth: 1},
			}
			if err := w.BatchInsertSrcStatusEvents(events); err != nil {
				return err
			}
			return w.BatchInsertPathEvents([]db.PathEvent{
				{ID: issue.ID, EventTime: eventTime, Category: db.PathEventCategoryGPLClean, ProposedPath: "bad_name.txt", Status: db.PathEventStatusPending},
				{ID: accepted.ID, EventTime: eventTime, Category: db.PathEventCategoryGPLClean, ProposedPath: "accepted.txt", Status: db.PathEventStatusAccepted},
				{ID: rejected.ID, EventTime: eventTime, Category: db.PathEventCategoryGPLClean, ProposedPath: "ignored.txt", Status: db.PathEventStatusPending},
				{ID: manual.ID, EventTime: eventTime, Category: db.PathEventCategoryGPLClean, ProposedPath: "", Status: db.PathEventStatusManualReview},
			})
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RebuildAllCurrent(); err != nil {
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

	assertOnly("issues", "*", "bad:name.txt")
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
