// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

// seedReviewTree writes SRC/DST nodes and optional id_map rows into Badger.
func seedReviewTree(t *testing.T, database *db.DB, src, dst []*db.NodeState, maps []db.IDMapEvent) {
	t.Helper()
	ops := make([]db.InsertOperation, 0, len(src)+len(dst))
	for _, n := range src {
		if n == nil {
			continue
		}
		ops = append(ops, db.InsertOperation{QueueType: "SRC", Level: n.Depth, Status: n.TraversalStatus, State: n})
	}
	for _, n := range dst {
		if n == nil {
			continue
		}
		ops = append(ops, db.InsertOperation{QueueType: "DST", Level: n.Depth, Status: n.TraversalStatus, State: n})
	}
	if err := database.SeedDiscoveredNodes(ops); err != nil {
		t.Fatal(err)
	}
	if len(maps) > 0 {
		if err := database.SeedIDMapEvents(maps); err != nil {
			t.Fatal(err)
		}
	}
}
