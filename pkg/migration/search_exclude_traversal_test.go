// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func TestSearchTargetsTraversalAck(t *testing.T) {
	t.Parallel()
	if !searchTargetsTraversalAck(SearchRequest{TraversalStatus: db.StatusFailed}) {
		t.Fatal("top-level TraversalStatus=failed")
	}
	if !searchTargetsTraversalAck(SearchRequest{
		Conditions: []PathReviewSearchCondition{{Field: "traversalStatus", Value: db.StatusExcluded}},
	}) {
		t.Fatal("condition traversalStatus=excluded")
	}
	if searchTargetsTraversalAck(SearchRequest{CopyStatus: db.CopyStatusFailed}) {
		t.Fatal("copy failed alone must not target traversal ack")
	}
}

func TestApplySearchExclusionAcksTraversalFailed(t *testing.T) {
	t.Parallel()
	database, err := db.Open(db.Options{Path: t.TempDir() + "/search-excl-trav.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	seedFailedSearchFixture(t, database, 3)

	m := &Migration{
		DB:        database,
		store:     newMigrationStore(database, nil),
		phase:     PhaseTraversalReview,
		reviewOps: newReviewOpGate(),
	}

	req := SearchRequest{
		StatusSearchType: "traversal",
		Conditions: []PathReviewSearchCondition{
			{Field: "traversalStatus", Value: db.StatusFailed},
		},
	}
	res, err := m.ApplySearchExclusion(req, true, nil, "app-trav-ack")
	if err != nil {
		t.Fatal(err)
	}
	if res.AffectedCount < 1 {
		t.Fatalf("AffectedCount=%d want >=1", res.AffectedCount)
	}

	srcRoot := db.MintNodeID("SRC", "", db.NodeTypeFolder, "/")
	for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
		id := db.MintNodeID("SRC", srcRoot, db.NodeTypeFile, name)
		n, err := pull.GetNodeByID(database, "SRC", id)
		if err != nil {
			t.Fatal(err)
		}
		if n == nil {
			t.Fatalf("missing node %s", name)
		}
		if n.TraversalStatus != db.StatusExcluded {
			t.Fatalf("%s TraversalStatus=%q want excluded", name, n.TraversalStatus)
		}
		if !opsdb.IsTraversalAckFromFailed(n.ExclusionSource) {
			t.Fatalf("%s ExclusionSource=%q want traversal-ack-from-failed", name, n.ExclusionSource)
		}
		if n.CopyStatus != db.CopyStatusPending {
			t.Fatalf("%s CopyStatus=%q want pending (must not copy-exclude)", name, n.CopyStatus)
		}
		if n.Excluded {
			t.Fatalf("%s Excluded=true; traversal ack must not set copy Excluded", name)
		}
	}
}
