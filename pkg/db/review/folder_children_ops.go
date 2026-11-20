// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"context"
	"fmt"
	"strings"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func lookupNodeIDByPath(ctx context.Context, d *db.DB, table, path string) (string, error) {
	_ = ctx
	side := opsdb.SideSRC
	if table == db.TableDstNodes {
		side = opsdb.SideDST
	}
	return d.Ops().GetNodeIDByPath(side, path)
}

func listFolderChildrenOps(d *db.DB, f ReviewFilter, parentPath string, sortCol string, sortDesc bool, limit int) ([]MergedReviewRow, []MergedReviewRow, error) {
	ctx := context.Background()
	ops := d.Ops()
	if ops == nil {
		return nil, nil, fmt.Errorf("ops store required")
	}

	side := NormalizeReviewSide(f.Side)
	if side == "dst" {
		dstRows, err := listFolderChildrenDSTSide(d, f, parentPath, sortCol, sortDesc, limit)
		if err != nil {
			return nil, nil, err
		}
		return nil, dstRows, nil
	}

	srcParentID, err := lookupNodeIDByPath(ctx, d, db.TableSrcNodes, parentPath)
	if err != nil {
		return nil, nil, err
	}
	var srcRows []MergedReviewRow
	if srcParentID != "" || parentPath == "/" {
		if srcParentID == "" && parentPath == "/" {
			rootID, err := lookupNodeIDByPath(ctx, d, db.TableSrcNodes, "/")
			if err != nil {
				return nil, nil, err
			}
			srcParentID = rootID
		}
		srcRows, err = childrenMergedRows(d, opsdb.SideSRC, srcParentID, f, sortCol, sortDesc, limit)
		if err != nil {
			return nil, nil, err
		}
	}

	var dstOnly []MergedReviewRow
	if side != "src" && !f.ExcludeDestinationOnly {
		// Merged/remapped folders use distinct SRC vs DST id_paths. Prefer the
		// id_map peer of the SRC parent so destination-only children appear.
		dstParentID, err := dstParentIDForSRCBrowsePath(d, srcParentID, parentPath)
		if err != nil {
			return nil, nil, err
		}
		if dstParentID != "" || parentPath == "/" {
			dstOnly, err = childrenMergedRowsDST(d, dstParentID, f, sortCol, sortDesc, limit)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	return srcRows, dstOnly, nil
}

// listFolderChildrenDSTSide lists DST children of a DST id_path parent, including
// mapped peers (with SRC attached). Used by dual-tree DST pane browse.
func listFolderChildrenDSTSide(d *db.DB, f ReviewFilter, parentPath string, sortCol string, sortDesc bool, limit int) ([]MergedReviewRow, error) {
	ctx := context.Background()
	dstParentID, err := lookupNodeIDByPath(ctx, d, db.TableDstNodes, parentPath)
	if err != nil {
		return nil, err
	}
	if dstParentID == "" && parentPath == "/" {
		dstParentID, err = lookupNodeIDByPath(ctx, d, db.TableDstNodes, "/")
		if err != nil {
			return nil, err
		}
	}
	if dstParentID == "" {
		return nil, nil
	}
	return childrenMergedRowsDSTAll(d, dstParentID, f, sortCol, sortDesc, limit)
}

// dstParentIDForSRCBrowsePath resolves which DST folder to list when the UI
// browses a SRC locationPath. Merge/remap pairs SRC and DST under different
// id_paths, so same-path DST lookup alone misses destination-only children.
func dstParentIDForSRCBrowsePath(d *db.DB, srcParentID, parentPath string) (string, error) {
	ops := d.Ops()
	if ops == nil {
		return "", fmt.Errorf("ops store required")
	}
	if srcParentID != "" {
		if m, ok, err := ops.GetMapBySrc(srcParentID); err != nil {
			return "", err
		} else if ok && m.DstID != "" && idMapBrowseActive(m.Status) {
			return m.DstID, nil
		}
	}
	dstParentID, err := lookupNodeIDByPath(context.Background(), d, db.TableDstNodes, parentPath)
	if err != nil {
		return "", err
	}
	if dstParentID == "" && parentPath == "/" {
		return lookupNodeIDByPath(context.Background(), d, db.TableDstNodes, "/")
	}
	return dstParentID, nil
}

func idMapBrowseActive(status string) bool {
	return status == "" || status == db.IDMapStatusActive
}

func childrenMergedRows(d *db.DB, side, parentID string, f ReviewFilter, sortCol string, sortDesc bool, limit int) ([]MergedReviewRow, error) {
	ids, err := d.Ops().ListChildren(side, parentID, "", limit*2)
	if err != nil {
		return nil, err
	}
	stMap, err := d.Ops().BatchGetStatus(side, ids)
	if err != nil {
		return nil, err
	}
	var rows []MergedReviewRow
	for _, id := range ids {
		rec, ok, err := d.Ops().GetNode(side, id)
		if err != nil || !ok {
			continue
		}
		if f.FoldersOnly && rec.Type != db.NodeTypeFolder {
			continue
		}
		if strings.EqualFold(f.TypeFilter, "file") && rec.Type != db.NodeTypeFile {
			continue
		}
		st := stMap[id]
		marked, excludedFromFailed := copyRetryFlags(st)
		row := MergedReviewRow{
			Path:                   rec.Path,
			Name:                   rec.Name,
			Depth:                  rec.Depth,
			Type:                   rec.Type,
			SrcNodeID:              rec.ID,
			Size:                   opsdb.DisplayBytes(rec.Type, rec.Size, st.ChildSize),
			SrcTraversalStatus:     st.TraversalStatus,
			CopyStatus:             st.CopyStatus,
			DeleteStatus:           st.DeleteStatus,
			Excluded:               st.CopyStatus == db.CopyStatusExcludedExplicit || st.CopyStatus == db.CopyStatusExcludedInherited,
			CopyRetryMarked:        marked,
			CopyExcludedFromFailed: excludedFromFailed,
			CopyForceReplaceMarked: copyForceReplaceMarked(st),
			TraversalAckFromFailed: traversalAckFromFailed(st),
		}
		if side == opsdb.SideSRC {
			if m, ok, _ := d.Ops().GetMapBySrc(rec.ID); ok {
				row.DstNodeID = m.DstID
				if dstRec, ok2, _ := d.Ops().GetNode(opsdb.SideDST, m.DstID); ok2 {
					row.DstPath = dstRec.Path
				}
			}
		}
		rows = append(rows, row)
	}
	if side == opsdb.SideSRC {
		attachAcceptedGPL(d, rows)
	}
	if err := attachPairedDSTTraversal(d, rows); err != nil {
		return nil, err
	}
	sortMergedRows(rows, sortCol, sortDesc)
	if len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

func attachAcceptedGPL(d *db.DB, rows []MergedReviewRow) {
	if d == nil || d.Ops() == nil || len(rows) == 0 {
		return
	}
	ids := make([]string, 0, len(rows))
	for _, r := range rows {
		if r.SrcNodeID != "" {
			ids = append(ids, r.SrcNodeID)
		}
	}
	if len(ids) == 0 {
		return
	}
	recs, err := d.Ops().BatchGetGPL(ids)
	if err != nil {
		return
	}
	for i := range rows {
		id := rows[i].SrcNodeID
		gpl, ok := recs[id]
		if !ok || gpl.Status != db.GPLIssueStatusAccepted {
			continue
		}
		rows[i].ResolvedDstName = gpl.ProposedName
		rows[i].PathDstAction = gpl.DstAction
	}
}

func childrenMergedRowsDST(d *db.DB, parentID string, f ReviewFilter, sortCol string, sortDesc bool, limit int) ([]MergedReviewRow, error) {
	ops := d.Ops()
	ids, err := ops.ListChildren(opsdb.SideDST, parentID, "", limit*2)
	if err != nil {
		return nil, err
	}
	trav, err := effectiveDSTTraversalByID(d, ids)
	if err != nil {
		return nil, err
	}
	stMap, err := ops.BatchGetStatus(opsdb.SideDST, ids)
	if err != nil {
		return nil, err
	}
	var rows []MergedReviewRow
	for _, id := range ids {
		if _, ok, _ := ops.GetMapByDst(id); ok {
			continue
		}
		rec, ok, err := ops.GetNode(opsdb.SideDST, id)
		if err != nil || !ok {
			continue
		}
		if f.FoldersOnly && rec.Type != db.NodeTypeFolder {
			continue
		}
		rows = append(rows, MergedReviewRow{
			Path:               rec.Path,
			DstPath:            rec.Path,
			Name:               rec.Name,
			Depth:              rec.Depth,
			Type:               rec.Type,
			DstNodeID:          rec.ID,
			Size:               opsdb.DisplayBytes(rec.Type, rec.Size, stMap[id].ChildSize),
			DstTraversalStatus: trav[id],
		})
	}
	sortMergedRows(rows, sortCol, sortDesc)
	if len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

// childrenMergedRowsDSTAll lists every DST child (mapped + unmapped) under a DST parent.
func childrenMergedRowsDSTAll(d *db.DB, parentID string, f ReviewFilter, sortCol string, sortDesc bool, limit int) ([]MergedReviewRow, error) {
	ops := d.Ops()
	ids, err := ops.ListChildren(opsdb.SideDST, parentID, "", limit*2)
	if err != nil {
		return nil, err
	}
	trav, err := effectiveDSTTraversalByID(d, ids)
	if err != nil {
		return nil, err
	}
	stMap, err := ops.BatchGetStatus(opsdb.SideDST, ids)
	if err != nil {
		return nil, err
	}
	srcIDs := make([]string, 0, len(ids))
	srcByDst := make(map[string]string, len(ids))
	for _, id := range ids {
		if m, ok, _ := ops.GetMapByDst(id); ok && idMapBrowseActive(m.Status) && m.SrcID != "" {
			srcByDst[id] = m.SrcID
			srcIDs = append(srcIDs, m.SrcID)
		}
	}
	srcNodes, err := ops.BatchGetNode(opsdb.SideSRC, srcIDs)
	if err != nil {
		return nil, err
	}
	srcSt, err := ops.BatchGetStatus(opsdb.SideSRC, srcIDs)
	if err != nil {
		return nil, err
	}
	var rows []MergedReviewRow
	for _, id := range ids {
		rec, ok, err := ops.GetNode(opsdb.SideDST, id)
		if err != nil || !ok {
			continue
		}
		if f.FoldersOnly && rec.Type != db.NodeTypeFolder {
			continue
		}
		if strings.EqualFold(f.TypeFilter, "file") && rec.Type != db.NodeTypeFile {
			continue
		}
		row := MergedReviewRow{
			// Path stays DST id_path so dual-tree DST pane can navigate natively.
			Path:               rec.Path,
			DstPath:            rec.Path,
			Name:               rec.Name,
			Depth:              rec.Depth,
			Type:               rec.Type,
			DstNodeID:          rec.ID,
			Size:               opsdb.DisplayBytes(rec.Type, rec.Size, stMap[id].ChildSize),
			DstTraversalStatus: trav[id],
		}
		if srcID := srcByDst[id]; srcID != "" {
			row.SrcNodeID = srcID
			if srcRec, ok2 := srcNodes[srcID]; ok2 {
				row.SrcPath = srcRec.Path
				// Prefer SRC display name for paired rows (actions / inspector identity).
				row.Name = srcRec.Name
				if st, has := srcSt[srcID]; has {
					row.SrcTraversalStatus = st.TraversalStatus
					row.CopyStatus = st.CopyStatus
					row.DeleteStatus = st.DeleteStatus
					row.Excluded = st.CopyStatus == db.CopyStatusExcludedExplicit || st.CopyStatus == db.CopyStatusExcludedInherited
					marked, excludedFromFailed := copyRetryFlags(st)
					row.CopyRetryMarked = marked
					row.CopyExcludedFromFailed = excludedFromFailed
					row.CopyForceReplaceMarked = copyForceReplaceMarked(st)
					row.TraversalAckFromFailed = traversalAckFromFailed(st)
					row.Size = opsdb.DisplayBytes(srcRec.Type, srcRec.Size, st.ChildSize)
				}
			}
		}
		rows = append(rows, row)
	}
	attachAcceptedGPL(d, rows)
	sortMergedRows(rows, sortCol, sortDesc)
	if len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

func sortMergedRows(rows []MergedReviewRow, sortCol string, sortDesc bool) {
	// Simple sort by path for ops tree (matches default review order).
	if sortCol != "path" && sortCol != "name" {
		sortCol = "path"
	}
	for i := 0; i < len(rows); i++ {
		for j := i + 1; j < len(rows); j++ {
			less := rows[i].Path > rows[j].Path
			if sortDesc {
				less = rows[i].Path < rows[j].Path
			}
			if less {
				rows[i], rows[j] = rows[j], rows[i]
			}
		}
	}
}

// ListFolderChildrenPage lists direct children of parentPath without COUNT(*).
func ListFolderChildrenPage(d *db.DB, f ReviewFilter, orderBy string, limit, offset int) ([]MergedReviewRow, bool, error) {
	parent := db.NormalizeRootRelativePath(f.ParentPath)
	if parent == "" {
		parent = "/"
	}
	if limit <= 0 {
		limit = 100
	}
	sortCol, sortDesc := parseReviewOrderBy(orderBy)
	switch sortCol {
	case "src_traversal_status", "copy_status":
		sortCol = "path"
		sortDesc = false
	}
	fetch := offset + limit + 1
	start := time.Now()
	srcRows, dstOnly, err := listFolderChildrenOps(d, f, parent, sortCol, sortDesc, fetch)
	if err != nil {
		d.RecordOp(db.OpReviewFolderPage, "", 0, time.Since(start), err)
		return nil, false, err
	}
	merged := mergeReviewSearchRows(srcRows, dstOnly, sortCol, sortDesc)
	if offset >= len(merged) {
		d.RecordOp(db.OpReviewFolderPage, "", 0, time.Since(start), nil)
		return nil, false, nil
	}
	merged = merged[offset:]
	hasMore := len(merged) > limit
	if hasMore {
		merged = merged[:limit]
	}
	d.RecordOp(db.OpReviewFolderPage, "", int64(len(merged)), time.Since(start), nil)
	return merged, hasMore, nil
}
