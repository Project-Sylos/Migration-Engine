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
	if !f.ExcludeDestinationOnly {
		dstParentID, err := lookupNodeIDByPath(ctx, d, db.TableDstNodes, parentPath)
		if err != nil {
			return nil, nil, err
		}
		if dstParentID == "" && parentPath == "/" {
			dstParentID, err = lookupNodeIDByPath(ctx, d, db.TableDstNodes, "/")
			if err != nil {
				return nil, nil, err
			}
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
		row := MergedReviewRow{
			Path:               rec.Path,
			Name:               rec.Name,
			Depth:              rec.Depth,
			Type:               rec.Type,
			SrcNodeID:          rec.ID,
			Size:               opsdb.DisplayBytes(rec.Type, rec.Size, st.ChildSize),
			SrcTraversalStatus: st.TraversalStatus,
			CopyStatus:         st.CopyStatus,
			DeleteStatus:       st.DeleteStatus,
			Excluded:           st.CopyStatus == db.CopyStatusExcludedExplicit || st.CopyStatus == db.CopyStatusExcludedInherited,
		}
		if side == opsdb.SideSRC {
			if m, ok, _ := d.Ops().GetMapBySrc(rec.ID); ok {
				row.DstNodeID = m.DstID
			}
		}
		rows = append(rows, row)
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

func listFolderChildrenPage(d *db.DB, f ReviewFilter, orderBy string, limit, offset int) ([]MergedReviewRow, bool, error) {
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
