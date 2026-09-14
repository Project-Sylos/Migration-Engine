// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package pull

import (
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

// ListNodesGPLKeyset returns nodes at depth whose gpl_status matches statusFilter
// (typically pending). Includes gpl_state, parent gpl_state, and accepted basename.
func ListNodesGPLKeyset(d *db.DB, table string, depth int, afterID, statusFilter string, limit int) ([]db.FetchResult, error) {
	if d == nil || d.Ops() == nil {
		return nil, fmt.Errorf("ops store not open")
	}
	if statusFilter == "" {
		statusFilter = db.GPLStatusPending
	}
	if limit <= 0 {
		return nil, nil
	}
	side := opsSide(table)
	cursor := afterID
	out := make([]db.FetchResult, 0, limit)
	for len(out) < limit {
		window := db.PullKeysetWindowSize
		if remaining := limit - len(out); window > remaining {
			window = remaining + 1
		}
		chunk, err := listGPLKeysetWindow(d, side, depth, cursor, statusFilter, window)
		if err != nil {
			return nil, err
		}
		if len(chunk) == 0 {
			break
		}
		out = append(out, chunk...)
		if len(out) >= limit {
			return out[:limit], nil
		}
		cursor = chunk[len(chunk)-1].Key
		if len(chunk) < window {
			break
		}
	}
	return out, nil
}

func listGPLKeysetWindow(d *db.DB, side string, depth int, afterID, statusFilter string, window int) ([]db.FetchResult, error) {
	ops := d.Ops()
	cursor := afterID
	matched := make([]db.FetchResult, 0, window)
	for len(matched) < window {
		ids, err := ops.ListNodeIDs(side, cursor, db.PullKeysetWindowSize)
		if err != nil {
			return nil, fmt.Errorf("list node ids: %w", err)
		}
		if len(ids) == 0 {
			break
		}
		nodes, err := loadNodes(d, side, ids)
		if err != nil {
			return nil, err
		}
		parentIDs := make([]string, 0, len(nodes))
		seenParent := map[string]struct{}{}
		srcIDs := make([]string, 0, len(nodes))
		for _, n := range nodes {
			if n == nil || n.Depth != depth || n.GPLStatus != statusFilter {
				continue
			}
			matched = append(matched, db.FetchResult{Key: n.ID, State: n})
			if side == opsdb.SideSRC {
				srcIDs = append(srcIDs, n.ID)
				if n.ParentID != "" {
					if _, ok := seenParent[n.ParentID]; !ok {
						seenParent[n.ParentID] = struct{}{}
						parentIDs = append(parentIDs, n.ParentID)
					}
				}
			}
			if len(matched) >= window {
				break
			}
		}
		if len(matched) >= window {
			break
		}
		cursor = ids[len(ids)-1]
		if len(ids) < db.PullKeysetWindowSize {
			break
		}
	}
	if side != opsdb.SideSRC || len(matched) == 0 {
		return matched, nil
	}
	parentRecs, err := ops.BatchGetNode(opsdb.SideSRC, parentIDsForGPL(matched))
	if err != nil {
		return nil, err
	}
	gplByID, err := ops.BatchGetGPL(srcIDsForGPL(matched))
	if err != nil {
		return nil, err
	}
	for i := range matched {
		st := matched[i].State
		if st == nil {
			continue
		}
		if p, ok := parentRecs[st.ParentID]; ok {
			matched[i].ParentGPLState = p.GPLState
		}
		if g, ok := gplByID[st.ID]; ok && g.ProposedName != "" {
			matched[i].ResolvedDstPath = g.ProposedName
		}
	}
	return matched, nil
}

func parentIDsForGPL(rows []db.FetchResult) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, fr := range rows {
		if fr.State == nil || fr.State.ParentID == "" {
			continue
		}
		if _, ok := seen[fr.State.ParentID]; ok {
			continue
		}
		seen[fr.State.ParentID] = struct{}{}
		out = append(out, fr.State.ParentID)
	}
	return out
}

func srcIDsForGPL(rows []db.FetchResult) []string {
	out := make([]string, 0, len(rows))
	for _, fr := range rows {
		if fr.State != nil && fr.State.ID != "" {
			out = append(out, fr.State.ID)
		}
	}
	return out
}

func basenameFromDisplayPath(p string) string {
	if p == "" || p == "/" {
		return p
	}
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			if i+1 < len(p) {
				return p[i+1:]
			}
			return ""
		}
	}
	return p
}
