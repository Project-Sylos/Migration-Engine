// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

// SetNodeExcludedOps updates copy exclusion in Badger (SRC only).
func (db *DB) SetNodeExcludedOps(nodeID string, excluded bool) error {
	if db == nil || db.opsStore == nil {
		return fmt.Errorf("ops store not open")
	}
	if nodeID == "" {
		return fmt.Errorf("empty node id")
	}
	st, ok, err := db.opsStore.GetStatus(opsdb.SideSRC, nodeID)
	if err != nil {
		return err
	}
	if !ok {
		st = opsdb.StatusRecord{}
	}
	if st.TraversalStatus == "" {
		st.TraversalStatus = StatusSuccessful
	}
	if excluded {
		st.CopyStatus = CopyStatusExcludedExplicit
		st.ExclusionSource = ExclusionSourceManual
	} else {
		st.CopyStatus = CopyStatusPending
		st.ExclusionSource = ""
		st.DeterminingRuleID = ""
	}
	if err := db.opsStore.PutStatus(opsdb.SideSRC, nodeID, st); err != nil {
		return err
	}
	deltas := []ReviewStatsDelta{
		{Key: ReviewKeyExcluded, Delta: boolDelta(excluded)},
		{Key: ReviewKeyCopyPending, Delta: boolDelta(!excluded)},
	}
	return db.incrOpsReviewDeltas(deltas)
}

func boolDelta(v bool) int64 {
	if v {
		return 1
	}
	return -1
}

func (db *DB) putOpsStatusField(table, nodeID string, apply func(*opsdb.StatusRecord)) error {
	if db == nil || db.opsStore == nil || nodeID == "" {
		return fmt.Errorf("ops store required")
	}
	side := opsdb.SideSRC
	if table == "DST" {
		side = opsdb.SideDST
	}
	st, _, err := db.opsStore.GetStatus(side, nodeID)
	if err != nil {
		return err
	}
	apply(&st)
	return db.opsStore.PutStatus(side, nodeID, st)
}
