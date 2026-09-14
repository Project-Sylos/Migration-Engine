// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"fmt"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

// queryNodesForReview lists nodes using Badger catalog + sealed status.
func queryNodesForReview(d *db.DB, table string, depth *int, status string, excluded *bool, pathLike string, orderByPath bool, limit, offset int) ([]db.NodeState, error) {
	side := opsdb.SideSRC
	if table == "DST" {
		side = opsdb.SideDST
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	if offset < 0 {
		offset = 0
	}
	ops := d.Ops()
	if ops == nil {
		return nil, fmt.Errorf("ops store required")
	}

	if status != "" && pathLike == "" {
		return queryNodesFromStatusOverlay(d, side, depth, status, excluded, limit, offset)
	}

	fetch := offset + limit + 1
	if status != "" {
		fetch = (offset + limit + 1) * 8
		if fetch < 5000 {
			fetch = 5000
		}
	}
	ids, err := ops.ListSubtreeIDs(side, "/", fetch)
	if err != nil {
		return nil, err
	}
	nodes, err := ops.BatchGetNode(side, ids)
	if err != nil {
		return nil, err
	}
	if pathLike != "" || depth != nil {
		needle := strings.ToLower(pathLike)
		filtered := make([]string, 0, len(ids))
		for _, id := range ids {
			n, ok := nodes[id]
			if !ok {
				continue
			}
			if depth != nil && n.Depth != *depth {
				continue
			}
			if needle != "" && !strings.Contains(strings.ToLower(n.Path), needle) {
				continue
			}
			filtered = append(filtered, id)
		}
		ids = filtered
	}
	if orderByPath {
		sortCandidateIDsByPath(nodes, ids, "path", false)
	}

	stMap, err := ops.BatchGetStatus(side, ids)
	if err != nil {
		return nil, err
	}

	var out []db.NodeState
	for _, id := range ids {
		nrec, ok := nodes[id]
		if !ok {
			continue
		}
		eff := stMap[id]
		n := db.NodeState{
			ID: nrec.ID, ServiceID: nrec.ServiceID, ParentID: nrec.ParentID, ParentServiceID: nrec.ParentServiceID,
			Path: nrec.Path, ParentPath: nrec.ParentPath, Name: nrec.Name, DisplayPath: nrec.DisplayPath,
			Type: nrec.Type, Size: opsdb.DisplayBytes(nrec.Type, nrec.Size, eff.ChildSize), MTime: nrec.MTime, Depth: nrec.Depth,
			TraversalStatus: eff.TraversalStatus, CopyStatus: eff.CopyStatus, DeleteStatus: eff.DeleteStatus,
			GPLStatus: eff.GPLStatus,
		}
		n.Status = n.TraversalStatus
		n.Excluded = eff.CopyStatus == db.CopyStatusExcludedExplicit || eff.CopyStatus == db.CopyStatusExcludedInherited
		if status != "" && n.TraversalStatus != status {
			continue
		}
		if excluded != nil && table == "SRC" {
			if *excluded != n.Excluded {
				continue
			}
		}
		if excluded != nil && table == "DST" && *excluded {
			continue
		}
		out = append(out, n)
		if len(out) >= offset+limit+1 {
			break
		}
	}
	if offset >= len(out) {
		return nil, nil
	}
	out = out[offset:]
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func queryNodesFromStatusOverlay(d *db.DB, side string, depth *int, status string, excluded *bool, limit, offset int) ([]db.NodeState, error) {
	ops := d.Ops()
	if ops == nil {
		return nil, fmt.Errorf("ops store required")
	}
	out := make([]db.NodeState, 0, limit)
	skipped := 0
	batchIDs := make([]string, 0, 256)
	batchSt := make([]opsdb.StatusRecord, 0, 256)
	flush := func() error {
		if len(batchIDs) == 0 {
			return nil
		}
		nodes, err := ops.BatchGetNode(side, batchIDs)
		if err != nil {
			return err
		}
		for i, id := range batchIDs {
			nrec, ok := nodes[id]
			if !ok {
				continue
			}
			if depth != nil && nrec.Depth != *depth {
				continue
			}
			if skipped < offset {
				skipped++
				continue
			}
			st := batchSt[i]
			n := db.NodeState{
				ID: nrec.ID, ServiceID: nrec.ServiceID, ParentID: nrec.ParentID, ParentServiceID: nrec.ParentServiceID,
				Path: nrec.Path, ParentPath: nrec.ParentPath, Type: nrec.Type, Size: opsdb.DisplayBytes(nrec.Type, nrec.Size, st.ChildSize), MTime: nrec.MTime,
				Depth: nrec.Depth, Name: nrec.Name, DisplayPath: nrec.DisplayPath,
				TraversalStatus: st.TraversalStatus, CopyStatus: st.CopyStatus, DeleteStatus: st.DeleteStatus,
				GPLStatus: st.GPLStatus,
			}
			if n.ID == "" {
				n.ID = id
			}
			n.Status = n.TraversalStatus
			n.Excluded = st.CopyStatus == db.CopyStatusExcludedExplicit || st.CopyStatus == db.CopyStatusExcludedInherited
			out = append(out, n)
			if len(out) >= limit {
				return nil
			}
		}
		batchIDs = batchIDs[:0]
		batchSt = batchSt[:0]
		return nil
	}
	err := ops.WalkStatus(side, "", false, func(id string, st opsdb.StatusRecord) (bool, error) {
		if len(out) >= limit {
			return false, nil
		}
		if status != "" && st.TraversalStatus != status {
			return true, nil
		}
		if excluded != nil {
			isEx := st.CopyStatus == db.CopyStatusExcludedExplicit || st.CopyStatus == db.CopyStatusExcludedInherited
			if side != opsdb.SideSRC {
				if *excluded {
					return true, nil
				}
			} else if *excluded != isEx {
				return true, nil
			}
		}
		batchIDs = append(batchIDs, id)
		batchSt = append(batchSt, st)
		if len(batchIDs) < 256 {
			return true, nil
		}
		if err := flush(); err != nil {
			return false, err
		}
		return len(out) < limit, nil
	})
	if err != nil {
		return nil, err
	}
	if len(out) < limit {
		if err := flush(); err != nil {
			return nil, err
		}
	}
	return out, nil
}
