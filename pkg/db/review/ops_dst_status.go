// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func effectiveDSTTraversalByID(d *db.DB, ids []string) (map[string]string, error) {
	out := make(map[string]string, len(ids))
	if d == nil || d.Ops() == nil || len(ids) == 0 {
		return out, nil
	}
	stMap, err := d.Ops().BatchGetStatus(opsdb.SideDST, ids)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		out[id] = stMap[id].TraversalStatus
	}
	return out, nil
}

func attachPairedDSTTraversal(d *db.DB, rows []MergedReviewRow) error {
	ids := make([]string, 0, len(rows))
	for i := range rows {
		if rows[i].DstNodeID != "" {
			ids = append(ids, rows[i].DstNodeID)
		}
	}
	if d == nil || d.Ops() == nil || len(ids) == 0 {
		return nil
	}
	stMap, err := d.Ops().BatchGetStatus(opsdb.SideDST, ids)
	if err != nil {
		return err
	}
	for i := range rows {
		id := rows[i].DstNodeID
		if id == "" {
			continue
		}
		st := stMap[id]
		rows[i].DstTraversalStatus = st.TraversalStatus
		if rows[i].Type != db.NodeTypeFile {
			rows[i].DstSize = st.ChildSize
			rows[i].HasDstSize = true
		}
	}
	return nil
}
