// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

// SeedRootNode writes a depth-0 root to Badger (no Duck catalog).
func (db *DB) SeedRootNode(table string, state *NodeState) error {
	if db == nil || db.Ops() == nil || state == nil {
		return fmt.Errorf("ops store required")
	}
	side := opsdb.SideSRC
	if table == "DST" {
		side = opsdb.SideDST
	}
	trav := state.TraversalStatus
	if trav == "" {
		trav = state.Status
	}
	copyStatus := state.CopyStatus
	if table == "SRC" {
		if copyStatus == "" {
			copyStatus = CopyStatusAlreadyExisted
		}
		state.CopyStatus = copyStatus
	}
	state.TraversalStatus = trav
	state.Status = trav

	ops := db.Ops()
	if err := ops.PutNode(side, nodeToOps(state)); err != nil {
		return err
	}
	st := statusFromNode(state)
	if err := ops.PutStatus(side, state.ID, st); err != nil {
		return err
	}
	if trav == StatusPending {
		_ = ops.AddPending(side, opsdb.PhaseTrav, 0, NormalizeQueueNodeType(state.Type), state.ID)
	}
	return db.applyRootSeedStats(table, trav, copyStatus, state)
}

func (db *DB) applyRootSeedStats(table, trav, copyStatus string, state *NodeState) error {
	var deltas []ReviewStatsDelta
	if table == "SRC" {
		if key := ReviewKeyForStatus("traversal", trav); key != "" {
			deltas = append(deltas, ReviewStatsDelta{Key: key, Delta: 1})
		}
		if key := ReviewKeyForStatus("copy", copyStatus); key != "" {
			deltas = append(deltas, ReviewStatsDelta{Key: key, Delta: 1})
		}
		switch NormalizeQueueNodeType(state.Type) {
		case NodeTypeFolder:
			deltas = append(deltas, ReviewStatsDelta{Key: ReviewKeyFolders, Delta: 1})
		case NodeTypeFile:
			deltas = append(deltas, ReviewStatsDelta{Key: ReviewKeyFiles, Delta: 1})
			if state.Size > 0 {
				deltas = append(deltas, ReviewStatsDelta{Key: ReviewKeySizeSrc, Delta: state.Size})
			}
		}
	} else if NormalizeQueueNodeType(state.Type) == NodeTypeFile && state.Size > 0 {
		deltas = append(deltas, ReviewStatsDelta{Key: ReviewKeySizeDst, Delta: state.Size})
	}
	depth := []DepthStatsDelta{
		{Table: table, Depth: 0, Key: StatsKey(StatsKindTraversal, trav), Delta: 1},
	}
	if table == "SRC" && copyStatus != "" {
		depth = append(depth, DepthStatsDelta{
			Table: table, Depth: 0, Key: StatsKeyTyped(StatsKindCopy, copyStatus, state.Type), Delta: 1,
		})
		if NormalizeQueueNodeType(state.Type) == NodeTypeFile && state.Size > 0 {
			depth = append(depth, DepthStatsDelta{
				Table: table, Depth: 0, Key: StatsKeyCopyFileBytes(copyStatus), Delta: state.Size,
			})
		}
	}
	return db.applyOpsHotpathStats(deltas, depth)
}

// ApplyDepthStatsDeltas applies per-depth counters (Badger path).
func (db *DB) ApplyDepthStatsDeltas(deltas []DepthStatsDelta) error {
	if db == nil || len(deltas) == 0 {
		return nil
	}
	if db.Ops() == nil {
		return fmt.Errorf("ops store required")
	}
	return db.applyOpsHotpathStats(nil, deltas)
}

// WriteReviewStatsSnapshot sets absolute review counter values.
func (db *DB) WriteReviewStatsSnapshot(s ReviewStatsSnapshot) error {
	if db == nil {
		return nil
	}
	pairs := []struct {
		key   string
		count int64
	}{
		{ReviewKeyTraversalPending, s.TraversalPending},
		{ReviewKeyTraversalPendingRetry, s.TraversalPendingRetry},
		{ReviewKeyTraversalSuccessful, s.TraversalSuccessful},
		{ReviewKeyTraversalFailed, s.TraversalFailed},
		{ReviewKeyCopyPending, s.CopyPending},
		{ReviewKeyCopyPendingRetry, s.CopyPendingRetry},
		{ReviewKeyCopySuccessful, s.CopySuccessful},
		{ReviewKeyCopyFailed, s.CopyFailed},
		{ReviewKeyDeletePending, s.DeletePending},
		{ReviewKeyDeleteDeleted, s.DeleteDeleted},
		{ReviewKeyDeleteFailed, s.DeleteFailed},
		{ReviewKeyDeleteSkipped, s.DeleteSkipped},
		{ReviewKeyExcluded, s.Excluded},
		{ReviewKeyFolders, s.Folders},
		{ReviewKeyFiles, s.Files},
		{ReviewKeySizeSrc, s.SizeSrc},
		{ReviewKeySizeDst, s.SizeDst},
		{ReviewKeySizeSelected, s.SizeSelected},
		{ReviewKeySizeDeleteSelected, s.SizeDeleteSelected},
	}
	if db.Ops() == nil {
		return fmt.Errorf("ops store required")
	}
	ops := db.Ops()
	for _, p := range pairs {
		if err := ops.SetStat(p.key, p.count); err != nil {
			return err
		}
	}
	return nil
}

// AppendQueueStats stores queue metrics JSON (Badger qstat:* keys).
func (db *DB) AppendQueueStats(queueKey, phase, metricsJSON string) error {
	if db == nil {
		return nil
	}
	if db.Ops() == nil {
		return fmt.Errorf("ops store required")
	}
	return db.Ops().AppendQueueStats(opsdb.QueueStatsRecord{
		QueueKey: queueKey, Phase: phase, MetricsJSON: metricsJSON, At: time.Now(),
	})
}

// UpdateNodeIncludeOnly sets SRC node include_only allowlist on Badger node metadata.
func (db *DB) UpdateNodeIncludeOnly(nodeID, includeOnlyJSON string) error {
	if db == nil || nodeID == "" {
		return nil
	}
	if db.Ops() == nil {
		return fmt.Errorf("ops store required")
	}
	ops := db.Ops()
	n, ok, err := ops.GetNode(opsdb.SideSRC, nodeID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("update include_only: node %s not found", nodeID)
	}
	n.IncludeOnly = includeOnlyJSON
	return ops.PutNode(opsdb.SideSRC, n)
}

// ApplyReviewStatsDeltas updates canonical review counters in Badger.
func (db *DB) ApplyReviewStatsDeltas(deltas []ReviewStatsDelta) error {
	if db == nil || len(deltas) == 0 {
		return nil
	}
	if db.Ops() == nil {
		return fmt.Errorf("ops store required")
	}
	return db.incrOpsReviewDeltas(deltas)
}

// SeedDiscoveredNodes writes a discovery batch to Badger.
func (db *DB) SeedDiscoveredNodes(ops []InsertOperation) error {
	if db == nil || len(ops) == 0 {
		return nil
	}
	if db.Ops() == nil {
		return fmt.Errorf("ops store required")
	}
	if err := db.AppendDiscoveredNodes(ops); err != nil {
		return err
	}
	return db.Flush(context.Background())
}

// SeedIDMapEvents writes pairing rows to Badger.
func (db *DB) SeedIDMapEvents(events []IDMapEvent) error {
	if db == nil || len(events) == 0 {
		return nil
	}
	if db.Ops() == nil {
		return fmt.Errorf("ops store required")
	}
	for _, e := range events {
		db.AppendIDMapEvent(e)
	}
	return db.Flush(context.Background())
}
