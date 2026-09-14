// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package pull

import (
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func opsSide(table string) string {
	if table == "DST" {
		return opsdb.SideDST
	}
	return opsdb.SideSRC
}

func nodeFromOps(rec opsdb.NodeRecord, st opsdb.StatusRecord) *db.NodeState {
	n := &db.NodeState{
		ID:              rec.ID,
		ServiceID:       rec.ServiceID,
		ParentID:        rec.ParentID,
		ParentServiceID: rec.ParentServiceID,
		Path:            rec.Path,
		ParentPath:      rec.ParentPath,
		Name:            rec.Name,
		DisplayPath:     rec.DisplayPath,
		Type:            rec.Type,
		Size:            rec.Size,
		MTime:           rec.MTime,
		Depth:           rec.Depth,
		IncludeOnly:     rec.IncludeOnly,
		GPLState:        rec.GPLState,
	}
	db.HydrateNodeFromOps(n, st)
	return n
}

func nodeFromKid(k opsdb.KidRecord) *db.NodeState {
	return nodeFromOps(opsdb.NodeRecord{
		ID:              k.ID,
		ServiceID:       k.ServiceID,
		ParentServiceID: k.ParentServiceID,
		Path:            k.Path,
		ParentPath:      k.ParentPath,
		Name:            k.Name,
		Type:            k.Type,
		Size:            k.Size,
		MTime:           k.MTime,
		Depth:           k.Depth,
	}, opsdb.StatusRecord{
		TraversalStatus: k.TraversalStatus,
		CopyStatus:      k.CopyStatus,
		DeleteStatus:    k.DeleteStatus,
	})
}

func loadNodes(d *db.DB, side string, ids []string) ([]*db.NodeState, error) {
	ops := d.Ops()
	if ops == nil {
		return nil, fmt.Errorf("ops store not open")
	}
	recs, sts, err := ops.BatchGetNodeStatus(side, ids)
	if err != nil {
		return nil, fmt.Errorf("batch get node status: %w", err)
	}
	out := make([]*db.NodeState, 0, len(ids))
	for _, id := range ids {
		rec, ok := recs[id]
		if !ok {
			continue
		}
		out = append(out, nodeFromOps(rec, sts[id]))
	}
	return out, nil
}

func fetchResultsFromIDs(d *db.DB, side string, ids []string) ([]db.FetchResult, error) {
	nodes, err := loadNodes(d, side, ids)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]*db.NodeState, len(nodes))
	for _, n := range nodes {
		byID[n.ID] = n
	}
	out := make([]db.FetchResult, 0, len(ids))
	for _, id := range ids {
		n := byID[id]
		if n == nil {
			continue
		}
		out = append(out, db.FetchResult{Key: id, State: n})
	}
	return out, nil
}

func listSchedResults(d *db.DB, side, phase string, depth int, nodeType, afterID, wantStatus string, limit int) ([]string, []db.FetchResult, error) {
	if nodeType == "" {
		nodeType = db.NodeTypeFolder
	}
	var ids []string
	var err error
	if wantStatus != "" {
		ids, err = d.Ops().ListPending(side, phase, depth, nodeType, afterID, wantStatus, limit)
	} else {
		ids, err = d.Ops().ListSchedAtDepth(side, phase, depth, nodeType, afterID, limit)
	}
	if err != nil {
		return nil, nil, err
	}
	results, err := fetchResultsFromIDs(d, side, ids)
	if err != nil {
		return nil, nil, err
	}
	return ids, results, err
}

func srcCopyStatusMatchesFilter(copyStatus, filter string) bool {
	if filter == db.CopyStatusSuccessful {
		return db.CopyStatusIsComplete(copyStatus)
	}
	if filter == db.CopyStatusPending {
		return copyStatus == "" || copyStatus == db.CopyStatusPending
	}
	return copyStatus == filter
}

// GetNodeByID returns the node by id from the given table.
func GetNodeByID(d *db.DB, table, id string) (*db.NodeState, error) {
	nodes, err := loadNodes(d, opsSide(table), []string{id})
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, nil
	}
	return nodes[0], nil
}

// GetNodeByPath returns the node by path from the given table.
func GetNodeByPath(d *db.DB, table, path string) (*db.NodeState, error) {
	side := opsSide(table)
	id, err := d.Ops().GetNodeIDByPath(side, db.NormalizeRootRelativePath(path))
	if err != nil {
		return nil, err
	}
	if id == "" {
		return nil, nil
	}
	return GetNodeByID(d, table, id)
}

// GetRootNode returns the root node (path = '/') from the given table.
func GetRootNode(d *db.DB, table string) (id string, state *db.NodeState, ok bool) {
	state, err := GetNodeByPath(d, table, "/")
	if err != nil || state == nil {
		return "", nil, false
	}
	return state.ID, state, true
}

// GetChildrenByParentPath returns up to limit children with the given parent_path.
func GetChildrenByParentPath(d *db.DB, table, parentPath string, limit int) ([]*db.NodeState, error) {
	parentPath = db.NormalizeRootRelativePath(parentPath)
	parent, err := GetNodeByPath(d, table, parentPath)
	if err != nil {
		return nil, err
	}
	if parent == nil {
		return nil, nil
	}
	return GetChildrenByParentID(d, table, parent.ID, limit)
}

// GetChildrenByParentID returns up to limit children with the given parent_id.
func GetChildrenByParentID(d *db.DB, table, parentID string, limit int) ([]*db.NodeState, error) {
	ids, err := d.Ops().ListChildren(opsSide(table), parentID, "", limit)
	if err != nil {
		return nil, err
	}
	return loadNodes(d, opsSide(table), ids)
}

// GetChildrenIDsByParentID returns child ids for the given parent_id (up to limit).
func GetChildrenIDsByParentID(d *db.DB, table, parentID string, limit int) ([]string, error) {
	return d.Ops().ListChildren(opsSide(table), parentID, "", limit)
}

// ListNodesPendingAtDepthKeyset returns traversal frontier nodes at depth from Badger.
func ListNodesPendingAtDepthKeyset(d *db.DB, table string, depth int, afterID string, limit int, nodeType string) ([]db.FetchResult, error) {
	if limit <= 0 {
		return nil, nil
	}
	start := time.Now()
	_, results, err := listSchedResults(d, opsSide(table), opsdb.PhaseTrav, depth, nodeType, afterID, db.StatusPending, limit)
	if err != nil {
		return nil, err
	}
	d.RecordOp(db.OpBadgerPull, "ListNodesPendingAtDepthKeyset", int64(len(results)), time.Since(start), nil)
	return results, nil
}

// ListNodesByDepthKeyset returns nodes at depth ordered by id after afterID.
func ListNodesByDepthKeyset(d *db.DB, table string, depth int, afterID, statusFilter string, limit int) ([]db.FetchResult, error) {
	if limit <= 0 {
		return nil, nil
	}
	side := opsSide(table)
	cursor := afterID
	out := make([]db.FetchResult, 0, limit)
	for len(out) < limit {
		ids, err := d.Ops().ListNodeIDs(side, cursor, db.PullKeysetWindowSize)
		if err != nil {
			return nil, err
		}
		if len(ids) == 0 {
			break
		}
		nodes, err := loadNodes(d, side, ids)
		if err != nil {
			return nil, err
		}
		for _, n := range nodes {
			if n == nil || n.Depth != depth {
				continue
			}
			if statusFilter != "" && n.TraversalStatus != statusFilter {
				continue
			}
			out = append(out, db.FetchResult{Key: n.ID, State: n})
			if len(out) >= limit {
				return out, nil
			}
		}
		cursor = ids[len(ids)-1]
		if len(ids) < db.PullKeysetWindowSize {
			break
		}
	}
	return out, nil
}

func listSrcPhaseKeyset(d *db.DB, phase string, depth int, nodeType, afterID string, limit int) ([]db.FetchResult, error) {
	_, results, err := listSchedResults(d, opsdb.SideSRC, phase, depth, nodeType, afterID, "", limit)
	if err != nil {
		return nil, fmt.Errorf("list src %s keyset: %w", phase, err)
	}
	return results, nil
}

func enrichCopyFetchResults(d *db.DB, results []db.FetchResult) ([]db.FetchResult, error) {
	if len(results) == 0 {
		return results, nil
	}
	srcIDs := make([]string, 0, len(results))
	parentIDs := make([]string, 0, len(results))
	seenParent := map[string]struct{}{}
	for _, fr := range results {
		if fr.State == nil {
			continue
		}
		srcIDs = append(srcIDs, fr.State.ID)
		if fr.State.ParentID == "" {
			continue
		}
		if _, ok := seenParent[fr.State.ParentID]; ok {
			continue
		}
		seenParent[fr.State.ParentID] = struct{}{}
		parentIDs = append(parentIDs, fr.State.ParentID)
	}
	selfMap, err := d.Ops().BatchGetMap(srcIDs, false)
	if err != nil {
		return nil, err
	}
	parentMap, err := d.Ops().BatchGetMap(parentIDs, false)
	if err != nil {
		return nil, err
	}
	dstParentIDs := make([]string, 0, len(parentMap))
	for _, m := range parentMap {
		if m.DstID != "" {
			dstParentIDs = append(dstParentIDs, m.DstID)
		}
	}
	dstNodes, err := d.Ops().BatchGetNode(opsdb.SideDST, dstParentIDs)
	if err != nil {
		return nil, err
	}
	parentSt, err := d.Ops().BatchGetStatus(opsdb.SideSRC, parentIDs)
	if err != nil {
		return nil, err
	}
	gplRecs, err := d.Ops().BatchGetGPL(srcIDs)
	if err != nil {
		return nil, err
	}
	for i := range results {
		st := results[i].State
		if st == nil {
			continue
		}
		if m, ok := parentMap[st.ParentID]; ok {
			results[i].DstParentNodeID = m.DstID
			if dn, ok := dstNodes[m.DstID]; ok {
				results[i].DstParentServiceID = dn.ServiceID
			}
		}
		if sm, ok := selfMap[st.ID]; ok {
			results[i].DstMappedID = sm.DstID
		}
		if ps, ok := parentSt[st.ParentID]; ok {
			results[i].SrcParentDeleteStatus = ps.DeleteStatus
		}
		if g, ok := gplRecs[st.ID]; ok && g.ProposedName != "" {
			results[i].ResolvedDstPath = g.ProposedName
		}
	}
	return results, nil
}

// ListNodesCopyKeyset returns SRC nodes at depth with copy_status = statusFilter.
func ListNodesCopyKeyset(d *db.DB, depth int, nodeType, afterID string, limit int, statusFilter string) ([]db.FetchResult, error) {
	if limit <= 0 {
		return nil, nil
	}
	results, err := listSrcPhaseKeyset(d, opsdb.PhaseCopy, depth, nodeType, afterID, limit)
	if err != nil {
		return nil, err
	}
	return enrichCopyFetchResults(d, results)
}

// ListNodesDeleteKeyset returns SRC nodes at depth with delete_status = statusFilter.
func ListNodesDeleteKeyset(d *db.DB, depth int, nodeType, afterID string, limit int, statusFilter string) ([]db.FetchResult, error) {
	if limit <= 0 {
		return nil, nil
	}
	return listSrcPhaseKeyset(d, opsdb.PhaseDel, depth, nodeType, afterID, limit)
}

// SubtreeStats holds aggregate counts for a subtree.
type SubtreeStats struct {
	TotalNodes   int
	TotalFolders int
	TotalFiles   int
	MaxDepth     int
}

// CountSubtree returns aggregate counts for the subtree at rootPath.
func CountSubtree(d *db.DB, table, rootPath string) (SubtreeStats, error) {
	var stats SubtreeStats
	side := opsSide(table)
	err := d.Ops().ApplySubtreeScan(side, rootPath, opsdb.SubtreeScanOpts{}, 0, func(chunk opsdb.SubtreeChunk) error {
		for _, id := range chunk.IDs {
			n, ok := chunk.Nodes[id]
			if !ok {
				continue
			}
			stats.TotalNodes++
			if n.Type == db.NodeTypeFile {
				stats.TotalFiles++
			} else {
				stats.TotalFolders++
			}
			if n.Depth > stats.MaxDepth {
				stats.MaxDepth = n.Depth
			}
		}
		return nil
	})
	return stats, err
}

func copyStatusExcluded(st string) bool {
	return st == db.CopyStatusExcludedExplicit || st == db.CopyStatusExcludedInherited
}

// CountExcludedInSubtree returns SRC nodes in the subtree with excluded copy_status.
func CountExcludedInSubtree(d *db.DB, table, rootPath string) (int, error) {
	if table == "DST" {
		return 0, nil
	}
	var count int
	err := d.Ops().ApplySubtreeScan(opsdb.SideSRC, rootPath, opsdb.SubtreeScanOpts{}, 0, func(chunk opsdb.SubtreeChunk) error {
		for _, id := range chunk.IDs {
			if copyStatusExcluded(chunk.Status[id].CopyStatus) {
				count++
			}
		}
		return nil
	})
	return count, err
}

// CountExcluded returns the number of SRC nodes with excluded copy_status.
func CountExcluded(d *db.DB, table string) (int, error) {
	if table == "DST" {
		return 0, nil
	}
	var count int
	err := d.Ops().ApplySubtreeScan(opsdb.SideSRC, "/", opsdb.SubtreeScanOpts{}, 0, func(chunk opsdb.SubtreeChunk) error {
		for _, id := range chunk.IDs {
			if copyStatusExcluded(chunk.Status[id].CopyStatus) {
				count++
			}
		}
		return nil
	})
	return count, err
}

// CountNodes returns the total number of nodes in the given table.
func CountNodes(d *db.DB, table string) (int, error) {
	side := opsSide(table)
	var count int
	cursor := ""
	for {
		ids, err := d.Ops().ListNodeIDs(side, cursor, 10_000)
		if err != nil {
			return 0, err
		}
		count += len(ids)
		if len(ids) == 0 {
			break
		}
		cursor = ids[len(ids)-1]
		if len(ids) < 10_000 {
			break
		}
	}
	return count, nil
}

// GetAllLevels returns depth values 0..max for the table.
func GetAllLevels(d *db.DB, table string) ([]int, error) {
	max, err := d.Ops().GetDepthMax(opsSide(table))
	if err != nil {
		return nil, err
	}
	out := make([]int, 0, max+1)
	for i := 0; i <= int(max); i++ {
		out = append(out, i)
	}
	return out, nil
}

// BatchGetNodeMeta batch-loads node metadata from Badger.
func BatchGetNodeMeta(d *db.DB, table string, ids []string) (map[string]db.NodeMeta, error) {
	if len(ids) == 0 {
		return map[string]db.NodeMeta{}, nil
	}
	side := opsSide(table)
	recs, stMap, err := d.Ops().BatchGetNodeStatus(side, ids)
	if err != nil {
		return nil, err
	}
	out := make(map[string]db.NodeMeta, len(ids))
	for _, id := range ids {
		rec, ok := recs[id]
		if !ok {
			continue
		}
		st := stMap[id]
		out[id] = db.NodeMeta{
			ID:              rec.ID,
			Depth:           rec.Depth,
			Type:            rec.Type,
			TraversalStatus: st.TraversalStatus,
			CopyStatus:      st.CopyStatus,
			DeleteStatus:    st.DeleteStatus,
		}
	}
	return out, nil
}

func hydrateSrcChildrenForDstBatch(d *db.DB, results []db.FetchResult) (map[string][]*db.NodeState, map[string]string, error) {
	if len(results) == 0 {
		return map[string][]*db.NodeState{}, map[string]string{}, nil
	}
	dstIDs := make([]string, 0, len(results))
	for _, fr := range results {
		if fr.State != nil {
			dstIDs = append(dstIDs, fr.State.ID)
		}
	}
	maps, err := d.Ops().BatchGetMap(dstIDs, true)
	if err != nil {
		return nil, nil, fmt.Errorf("batch get map by dst: %w", err)
	}
	srcParents := make([]string, 0, len(maps))
	seenSrc := make(map[string]struct{}, len(maps))
	for _, m := range maps {
		if m.SrcID == "" {
			continue
		}
		if _, ok := seenSrc[m.SrcID]; ok {
			continue
		}
		seenSrc[m.SrcID] = struct{}{}
		srcParents = append(srcParents, m.SrcID)
	}
	packs, err := d.Ops().BatchGetKids(opsdb.SideSRC, srcParents)
	if err != nil {
		return nil, nil, fmt.Errorf("batch get kids: %w", err)
	}
	childIDsByParent, err := d.Ops().ListChildrenMany(opsdb.SideSRC, srcParents, 100_000)
	if err != nil {
		return nil, nil, fmt.Errorf("list children many: %w", err)
	}
	var indexChildIDs []string
	for _, srcID := range srcParents {
		if len(packs[srcID]) > 0 {
			continue
		}
		indexChildIDs = append(indexChildIDs, childIDsByParent[srcID]...)
	}
	indexNodesByID := make(map[string]*db.NodeState, len(indexChildIDs))
	if len(indexChildIDs) > 0 {
		indexNodes, err := loadNodes(d, opsdb.SideSRC, indexChildIDs)
		if err != nil {
			return nil, nil, fmt.Errorf("load src children: %w", err)
		}
		for _, n := range indexNodes {
			if n != nil {
				indexNodesByID[n.ID] = n
			}
		}
	}
	parentSt, err := d.Ops().BatchGetStatus(opsdb.SideSRC, srcParents)
	if err != nil {
		return nil, nil, fmt.Errorf("batch get src parent status: %w", err)
	}
	childrenByParent := make(map[string][]*db.NodeState, len(results))
	srcParentDelete := make(map[string]string, len(results))
	for _, fr := range results {
		if fr.State == nil {
			continue
		}
		m, ok := maps[fr.State.ID]
		if !ok {
			continue
		}
		srcParentDelete[fr.State.ID] = parentSt[m.SrcID].DeleteStatus
		if kids := packs[m.SrcID]; len(kids) > 0 {
			children := make([]*db.NodeState, 0, len(kids))
			for _, k := range kids {
				children = append(children, nodeFromKid(k))
			}
			childrenByParent[fr.State.ID] = children
			continue
		}
		childIDs := childIDsByParent[m.SrcID]
		children := make([]*db.NodeState, 0, len(childIDs))
		for _, id := range childIDs {
			if n := indexNodesByID[id]; n != nil {
				children = append(children, n)
			}
		}
		childrenByParent[fr.State.ID] = children
	}
	return childrenByParent, srcParentDelete, nil
}

// ListDstBatchWithSrcChildren pulls DST traversal folders and hydrates SRC expected children.
func ListDstBatchWithSrcChildren(d *db.DB, depth int, afterID string, limit int, traversalStatus string) ([]db.FetchResult, map[string][]*db.NodeState, map[string]string, string, error) {
	batch, children, del, last, _, err := ListDstBatchWithSrcChildrenQuota(d, depth, afterID, DstPullQuota{MaxTasks: limit, MaxChildren: 0}, traversalStatus)
	return batch, children, del, last, err
}

// GetSrcChildrenGroupedByParentPath returns SRC nodes grouped by parent_path.
func GetSrcChildrenGroupedByParentPath(d *db.DB, parentPaths []string) (map[string][]*db.NodeState, error) {
	out := make(map[string][]*db.NodeState, len(parentPaths))
	for _, p := range parentPaths {
		p = db.NormalizeRootRelativePath(p)
		children, err := GetChildrenByParentPath(d, "SRC", p, 10_000)
		if err != nil {
			return nil, err
		}
		out[p] = children
	}
	return out, nil
}

// GetDstIDToSrcPath returns for each DST id the path (SRC join key).
func GetDstIDToSrcPath(d *db.DB, dstIDs []string) (map[string]string, error) {
	out := make(map[string]string)
	for _, id := range dstIDs {
		n, err := GetNodeByID(d, "DST", id)
		if err != nil || n == nil {
			continue
		}
		out[id] = n.Path
	}
	return out, nil
}

// GetDstIDFromSrcID returns the DST node id mapped to the given SRC node id.
func GetDstIDFromSrcID(d *db.DB, srcParentID string) (string, error) {
	m, ok, err := d.Ops().GetMapBySrc(srcParentID)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", nil
	}
	return m.DstID, nil
}

// BatchGetDstIDsFromSrcIDs returns map[srcID]dstID via id_map.
func BatchGetDstIDsFromSrcIDs(d *db.DB, srcIDs []string) (map[string]string, error) {
	out := make(map[string]string)
	if len(srcIDs) == 0 {
		return out, nil
	}
	maps, err := d.Ops().BatchGetMap(srcIDs, false)
	if err != nil {
		return nil, err
	}
	for srcID, m := range maps {
		if m.DstID != "" {
			out[srcID] = m.DstID
		}
	}
	return out, nil
}

// BatchGetNodesByID returns nodes by id for the given table.
func BatchGetNodesByID(d *db.DB, table string, ids []string) (map[string]*db.NodeState, error) {
	out := make(map[string]*db.NodeState)
	if len(ids) == 0 {
		return out, nil
	}
	nodes, err := loadNodes(d, opsSide(table), ids)
	if err != nil {
		return nil, err
	}
	for _, n := range nodes {
		out[n.ID] = n
	}
	return out, nil
}

// BatchGetChildrenIDsByParentIDs returns map[parentID][]childID.
func BatchGetChildrenIDsByParentIDs(d *db.DB, table string, parentIDs []string) (map[string][]string, error) {
	out := make(map[string][]string, len(parentIDs))
	if len(parentIDs) == 0 {
		return out, nil
	}
	packs, err := d.Ops().BatchGetKids(opsSide(table), parentIDs)
	if err != nil {
		return nil, err
	}
	for pid, kids := range packs {
		ids := make([]string, 0, len(kids))
		for _, k := range kids {
			ids = append(ids, k.ID)
		}
		out[pid] = ids
	}
	for _, pid := range parentIDs {
		if _, ok := out[pid]; !ok {
			out[pid] = nil
		}
	}
	return out, nil
}

// ListSrcNodesByCopyStatus returns SRC nodes whose copy_status matches, paginated.
func ListSrcNodesByCopyStatus(d *db.DB, copyStatus string, limit, offset int) ([]db.NodeState, error) {
	if limit <= 0 {
		limit = 1000
	}
	if limit > 5000 {
		limit = 5000
	}
	if offset < 0 {
		offset = 0
	}
	ops := d.Ops()
	if ops == nil {
		return nil, fmt.Errorf("ops store not open")
	}
	var out []db.NodeState
	skipped := 0
	done := false
	err := ops.ApplySubtreeScan(opsdb.SideSRC, "/", opsdb.SubtreeScanOpts{}, 0, func(chunk opsdb.SubtreeChunk) error {
		if done {
			return nil
		}
		for _, id := range chunk.IDs {
			rec, ok := chunk.Nodes[id]
			if !ok {
				continue
			}
			st := chunk.Status[id]
			if !srcCopyStatusMatchesFilter(st.CopyStatus, copyStatus) {
				continue
			}
			if skipped < offset {
				skipped++
				continue
			}
			if len(out) >= limit {
				done = true
				return nil
			}
			n := nodeFromOps(rec, st)
			out = append(out, *n)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
