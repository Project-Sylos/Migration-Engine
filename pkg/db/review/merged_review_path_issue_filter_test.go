// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
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
		GPLStatus: db.GPLStatusIgnored,
	}
	manual := &db.NodeState{
		ID:   db.DeterministicNodeID("SRC", db.NodeTypeFile, "/*"),
		Path: "/*", ParentPath: "/", Name: "*",
		Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}
	seedReviewTree(t, database, []*db.NodeState{clean, issue, accepted, rejected, manual}, nil, nil)

	if err := database.Ops().BatchPutGPL([]opsdb.GPLRecord{
		{SrcID: issue.ID, UpdatedAt: eventTime, ProposedName: "bad_name.txt", Status: db.GPLIssueStatusPending},
		{SrcID: accepted.ID, UpdatedAt: eventTime, ProposedName: "accepted.txt", Status: db.GPLIssueStatusAccepted},
		{SrcID: rejected.ID, UpdatedAt: eventTime, ProposedName: "ignored.txt", Status: db.GPLIssueStatusPending},
		{SrcID: manual.ID, UpdatedAt: eventTime, ProposedName: "", Status: db.GPLIssueStatusManualReview},
	}); err != nil {
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
