// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package stats

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func seedSrcDepthStats(t *testing.T, database *db.DB, nodes []*db.NodeState) {
	t.Helper()
	deltas := make([]db.DepthStatsDelta, 0, len(nodes)*4)
	for _, n := range nodes {
		trav := n.TraversalStatus
		if trav == "" {
			trav = db.StatusPending
		}
		deltas = append(deltas, db.DepthStatsDelta{
			Table: "SRC", Depth: n.Depth, Key: db.StatsKey(db.StatsKindTraversal, trav), Delta: 1,
		})
		copySt := n.CopyStatus
		if copySt == "" {
			copySt = db.CopyStatusPending
		}
		nt := db.NormalizeQueueNodeType(n.Type)
		deltas = append(deltas, db.DepthStatsDelta{
			Table: "SRC", Depth: n.Depth, Key: db.StatsKeyTyped(db.StatsKindCopy, copySt, nt), Delta: 1,
		})
		if nt == db.NodeTypeFile && n.Size > 0 {
			deltas = append(deltas, db.DepthStatsDelta{
				Table: "SRC", Depth: n.Depth, Key: db.StatsKeyCopyFileBytes(copySt), Delta: n.Size,
			})
		}
		if n.DeleteStatus != "" && (copySt == db.CopyStatusSuccessful || copySt == db.CopyStatusAlreadyExisted) {
			deltas = append(deltas, db.DepthStatsDelta{
				Table: "SRC", Depth: n.Depth, Key: db.StatsKeyTyped(db.StatsKindDelete, n.DeleteStatus, nt), Delta: 1,
			})
			if nt == db.NodeTypeFile && n.Size > 0 {
				deltas = append(deltas, db.DepthStatsDelta{
					Table: "SRC", Depth: n.Depth, Key: db.StatsKeyDeleteFileBytes(n.DeleteStatus), Delta: n.Size,
				})
			}
		}
	}
	if err := database.ApplyDepthStatsDeltas(deltas); err != nil {
		t.Fatal(err)
	}
}
