// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

// deltaKeyToReviewKey maps a migration-layer delta key to the universal stats table key.
func deltaKeyToReviewKey(k string) string {
	switch k {
	case DeltaTraversalPending:
		return db.ReviewKeyTraversalPending
	case DeltaTraversalPendingRetry:
		return db.ReviewKeyTraversalPendingRetry
	case DeltaTraversalFailed:
		return db.ReviewKeyTraversalFailed
	case DeltaCopyPending:
		return db.ReviewKeyCopyPending
	case DeltaCopyFailed:
		return db.ReviewKeyCopyFailed
	case DeltaCopySuccessful:
		return db.ReviewKeyCopySuccessful
	case DeltaExcluded:
		return db.ReviewKeyExcluded
	case DeltaFolders:
		return db.ReviewKeyFolders
	case DeltaFiles:
		return db.ReviewKeyFiles
	case DeltaSizeSrc:
		return db.ReviewKeySizeSrc
	case DeltaSizeDst:
		return db.ReviewKeySizeDst
	default:
		return ""
	}
}

// persistReviewDeltas applies migration-layer deltas to the universal stats table.
func (s *migrationStore) persistReviewDeltas(deltas map[string]int64) error {
	if len(deltas) == 0 {
		return nil
	}
	dbDeltas := make([]db.ReviewStatsDelta, 0, len(deltas))
	for k, v := range deltas {
		if rk := deltaKeyToReviewKey(k); rk != "" {
			dbDeltas = append(dbDeltas, db.ReviewStatsDelta{Key: rk, Delta: v})
		}
	}
	if len(dbDeltas) == 0 {
		return nil
	}
	return s.db.RunWrite(context.Background(), func(sess *db.WriteSession) error {
		return sess.WithTx(func(w *db.Writer) error {
			return w.ApplyReviewStatsDeltas(dbDeltas)
		})
	})
}

func mergedRowToDiffItem(r db.MergedReviewRow) DiffItem {
	item := DiffItem{
			Path:               r.Path,
			Name:               r.Name,
			Depth:              r.Depth,
			Type:               r.Type,
			SrcNodeID:          r.SrcNodeID,
			DstNodeID:          r.DstNodeID,
			SrcTraversalStatus: r.SrcTraversalStatus,
			DstTraversalStatus: r.DstTraversalStatus,
			CopyStatus:         db.CopyStatusForDisplay(r.CopyStatus),
			Excluded:           r.Excluded,
		Size:               r.Size,
		MissingOnSource:    r.SrcNodeID == "",
		MissingOnDest:      r.DstNodeID == "",
	}
	if item.MissingOnSource {
		item.CopyStatus = ""
	}
	if item.Name == "" {
		item.Name = path.Base(item.Path)
	}
	return item
}

func diffItemNeedsSrcFailureLog(item DiffItem) bool {
	if item.SrcNodeID == "" {
		return false
	}
	if strings.EqualFold(item.SrcTraversalStatus, db.StatusFailed) {
		return true
	}
	return strings.EqualFold(item.CopyStatus, db.CopyStatusFailed)
}

func diffItemNeedsDstFailureLog(item DiffItem) bool {
	if item.DstNodeID == "" {
		return false
	}
	return strings.EqualFold(item.DstTraversalStatus, db.StatusFailed)
}

func (s *migrationStore) enrichDiffItemsWithFailureLogs(items []DiffItem) error {
	if s == nil || s.db == nil || len(items) == 0 {
		return nil
	}
	ctx := context.Background()

	srcIDs := make([]string, 0)
	dstIDs := make([]string, 0)
	for _, item := range items {
		if diffItemNeedsSrcFailureLog(item) {
			srcIDs = append(srcIDs, item.SrcNodeID)
		}
		if diffItemNeedsDstFailureLog(item) {
			dstIDs = append(dstIDs, item.DstNodeID)
		}
	}

	srcLogByNode, err := db.LatestSrcFailureLogIDsByNodeIDs(ctx, s.db, srcIDs)
	if err != nil {
		return fmt.Errorf("load src failure log ids: %w", err)
	}
	dstLogByNode, err := db.LatestDstFailureLogIDsByNodeIDs(ctx, s.db, dstIDs)
	if err != nil {
		return fmt.Errorf("load dst failure log ids: %w", err)
	}

	logIDs := make([]string, 0, len(srcLogByNode)+len(dstLogByNode))
	for _, id := range srcLogByNode {
		logIDs = append(logIDs, id)
	}
	for _, id := range dstLogByNode {
		logIDs = append(logIDs, id)
	}
	logsByID, err := db.GetFailureLogsByIDs(ctx, s.db, logIDs)
	if err != nil {
		return fmt.Errorf("load failure logs: %w", err)
	}

	for i := range items {
		if logID, ok := srcLogByNode[items[i].SrcNodeID]; ok {
			items[i].SrcFailureLogID = logID
			if log, ok := logsByID[logID]; ok {
				items[i].SrcFailureMessage = db.FailureLogDisplayText(log)
			}
		}
		if logID, ok := dstLogByNode[items[i].DstNodeID]; ok {
			items[i].DstFailureLogID = logID
			if log, ok := logsByID[logID]; ok {
				items[i].DstFailureMessage = db.FailureLogDisplayText(log)
			}
		}
	}
	return nil
}

func (s *migrationStore) queryNodes(filter NodeQueryFilter) ([]db.NodeState, error) {
	table := "SRC"
	if strings.ToUpper(filter.Queue) == "DST" {
		table = "DST"
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}
	return db.QueryNodesForReview(s.db, table, filter.Depth, filter.Status, filter.Excluded, filter.PathLike, filter.OrderByPath, limit, offset)
}

// addReviewDeltaLeaveCopyBucketForExclude applies -1 to the copy-status bucket the node is leaving when excluding (copy_status → excluded). Matches Writer.SetNodeExcluded depth stats.
func addReviewDeltaLeaveCopyBucketForExclude(deltas map[string]int64, copyStatus string) {
	switch copyStatus {
	case db.CopyStatusPending, "":
		addReviewDelta(deltas, DeltaCopyPending, -1)
	case db.CopyStatusFailed:
		addReviewDelta(deltas, DeltaCopyFailed, -1)
	case db.CopyStatusSuccessful:
		addReviewDelta(deltas, DeltaCopySuccessful, -1)
	case db.CopyStatusInProgress:
		// Writer does not decrement in_progress in per-depth stats when switching to excluded.
	case db.CopyStatusSkipped:
		// No universal review key for skipped; excluded aggregate still increments.
	case db.CopyStatusExcludedExplicit, db.CopyStatusExcludedInherited:
		// Exclude path should not run when already excluded.
	default:
		addReviewDelta(deltas, DeltaCopyPending, -1)
	}
}

func (s *migrationStore) setNodeExcluded(nodeID string, excluded bool) (int64, map[string]int64, error) {
	// Exclude/unexclude is SRC-only: only source nodes can be excluded from copy; no DST lookup.
	node, err := db.GetNodeByID(s.db, "SRC", nodeID)
	if err != nil {
		return 0, nil, err
	}
	if node == nil {
		return 0, nil, fmt.Errorf("node %s not found in SRC", nodeID)
	}
	q := "SRC"
	if node.Excluded == excluded {
		return 0, nil, nil
	}
	err = s.db.RunWrite(context.Background(), func(sess *db.WriteSession) error {
		return sess.WithTx(func(w *db.Writer) error {
			return w.SetNodeExcluded(q, nodeID, excluded)
		})
	})
	if err != nil {
		return 0, nil, err
	}
	deltas := make(map[string]int64)
	if excluded {
		addReviewDelta(deltas, DeltaExcluded, 1)
		addReviewDeltaLeaveCopyBucketForExclude(deltas, node.CopyStatus)
	} else {
		addReviewDelta(deltas, DeltaExcluded, -1)
		addReviewDelta(deltas, DeltaCopyPending, 1)
	}
	if err := s.persistReviewDeltas(deltas); err != nil {
		return 0, nil, fmt.Errorf("persist review deltas: %w", err)
	}
	return 1, deltas, nil
}

func (s *migrationStore) listRecentLogs(limit int) ([]LogEntry, error) {
	if limit <= 0 {
		limit = 50
	}
	conn, err := s.db.GetDB()
	if err != nil {
		return nil, err
	}
	rows, err := conn.QueryContext(
		context.Background(),
		`SELECT created_at, level, message FROM logs ORDER BY created_at DESC LIMIT $1`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("get recent logs: %w", err)
	}
	defer rows.Close()
	out := make([]LogEntry, 0, limit)
	for rows.Next() {
		var (
			timestamp time.Time
			level     string
			message   string
		)
		if err := rows.Scan(&timestamp, &level, &message); err != nil {
			return nil, err
		}
		out = append(out, LogEntry{
			Timestamp: timestamp,
			Level:     level,
			Message:   message,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *migrationStore) setNodeTraversalStatus(nodeID, status string) error {
	// Try SRC first, then DST. This avoids a read-before-write table resolution hop.
	err := s.db.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			return w.SetNodeTraversalStatus("SRC", nodeID, status)
		})
	})
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	err = s.db.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			return w.SetNodeTraversalStatus("DST", nodeID, status)
		})
	})
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("node %s not found", nodeID)
	}
	return err
}

func (s *migrationStore) setNodeCopyStatus(nodeID, status string) (int64, map[string]int64, error) {
	err := s.db.RunWrite(context.Background(), func(sess *db.WriteSession) error {
		return sess.WithTx(func(w *db.Writer) error {
			return w.SetNodeCopyStatus("SRC", nodeID, status)
		})
	})
	if err != nil {
		return 0, nil, err
	}
	deltas := make(map[string]int64)
	if status == db.CopyStatusPending {
		addReviewDelta(deltas, DeltaCopyFailed, -1)
		addReviewDelta(deltas, DeltaCopyPending, 1)
	} else {
		addReviewDelta(deltas, DeltaCopyPending, -1)
		addReviewDelta(deltas, DeltaCopyFailed, 1)
	}
	if err := s.persistReviewDeltas(deltas); err != nil {
		return 0, nil, fmt.Errorf("persist review deltas: %w", err)
	}
	return 1, deltas, nil
}

// markNodeForRetryDiscovery looks up the node by ID in SRC then DST (nodeID is either a SRC or DST node ID).
// For a SRC node at path P: deletes all DST descendants under P (not the DST node at P), marks SRC and DST node at P as pending. Non-recursive; DST children are removed so they can be re-derived on next traversal.
func (s *migrationStore) markNodeForRetryDiscovery(nodeID string) (int64, map[string]int64, error) {
	srcNode, err := db.GetNodeByID(s.db, "SRC", nodeID)
	if err != nil {
		return 0, nil, err
	}
	if srcNode != nil {
		path := srcNode.Path
		dstAtPath, _ := db.GetNodeByPath(s.db, "DST", path)
		var desc db.DstDescendantsReviewStats
		err := s.db.RunWrite(context.Background(), func(sess *db.WriteSession) error {
			return sess.WithTx(func(w *db.Writer) error {
				var err error
				desc, err = w.CountDstDescendantsReviewStats(path)
				if err != nil {
					return err
				}
				if err := w.SetNodeTraversalStatus("SRC", nodeID, db.StatusPending); err != nil {
					return err
				}
				if dstAtPath != nil {
					if err := w.SetNodeTraversalStatus("DST", dstAtPath.ID, db.StatusPending); err != nil {
						return err
					}
					if err := w.DeleteDescendantsUnderPath("DST", path); err != nil {
						return err
					}
				}
				return nil
			})
		})
		if err != nil {
			return 0, nil, err
		}
		deltas := make(map[string]int64)
		addReviewDelta(deltas, DeltaTraversalFailed, -1+-desc.TraversalFailed)
		addReviewDelta(deltas, DeltaTraversalPendingRetry, 1)
		addReviewDelta(deltas, DeltaFolders, -desc.Folders)
		addReviewDelta(deltas, DeltaFiles, -desc.Files)
		addReviewDelta(deltas, DeltaExcluded, -desc.Excluded)
		addReviewDelta(deltas, DeltaSizeDst, -desc.SizeDst)
		if err := s.persistReviewDeltas(deltas); err != nil {
			return 0, nil, fmt.Errorf("persist review deltas: %w", err)
		}
		return 1, deltas, nil
	}
	dstNode, err := db.GetNodeByID(s.db, "DST", nodeID)
	if err != nil || dstNode == nil {
		if err != nil {
			return 0, nil, err
		}
		return 0, nil, fmt.Errorf("node %s not found", nodeID)
	}
	err = s.db.RunWrite(context.Background(), func(sess *db.WriteSession) error {
		return sess.WithTx(func(w *db.Writer) error {
			return w.SetNodeTraversalStatus("DST", nodeID, db.StatusPending)
		})
	})
	if err != nil {
		return 0, nil, err
	}
	deltas := make(map[string]int64)
	addReviewDelta(deltas, DeltaTraversalFailed, -1)
	addReviewDelta(deltas, DeltaTraversalPendingRetry, 1)
	if err := s.persistReviewDeltas(deltas); err != nil {
		return 0, nil, fmt.Errorf("persist review deltas: %w", err)
	}
	return 1, deltas, nil
}

// unmarkNodeForRetryDiscovery sets SRC node back to failed and DST node at same path (if any) to not_on_src. Does not recreate DST children that were deleted on mark-for-retry.
func (s *migrationStore) unmarkNodeForRetryDiscovery(nodeID string) (int64, map[string]int64, error) {
	srcNode, err := db.GetNodeByID(s.db, "SRC", nodeID)
	if err != nil {
		return 0, nil, err
	}
	if srcNode != nil {
		path := srcNode.Path
		dstAtPath, _ := db.GetNodeByPath(s.db, "DST", path)
		err := s.db.RunWrite(context.Background(), func(sess *db.WriteSession) error {
			return sess.WithTx(func(w *db.Writer) error {
				if err := w.SetNodeTraversalStatus("SRC", nodeID, db.StatusFailed); err != nil {
					return err
				}
				if dstAtPath != nil {
					if err := w.SetNodeTraversalStatus("DST", dstAtPath.ID, db.StatusNotOnSrc); err != nil {
						return err
					}
				}
				return nil
			})
		})
		if err != nil {
			return 0, nil, err
		}
		deltas := make(map[string]int64)
		addReviewDelta(deltas, DeltaTraversalPendingRetry, -1)
		addReviewDelta(deltas, DeltaTraversalFailed, 1)
		if err := s.persistReviewDeltas(deltas); err != nil {
			return 0, nil, fmt.Errorf("persist review deltas: %w", err)
		}
		return 1, deltas, nil
	}
	dstNode, err := db.GetNodeByID(s.db, "DST", nodeID)
	if err != nil || dstNode == nil {
		if err != nil {
			return 0, nil, err
		}
		return 0, nil, fmt.Errorf("node %s not found", nodeID)
	}
	err = s.db.RunWrite(context.Background(), func(sess *db.WriteSession) error {
		return sess.WithTx(func(w *db.Writer) error {
			return w.SetNodeTraversalStatus("DST", nodeID, db.StatusFailed)
		})
	})
	if err != nil {
		return 0, nil, err
	}
	deltas := make(map[string]int64)
	addReviewDelta(deltas, DeltaTraversalPendingRetry, -1)
	addReviewDelta(deltas, DeltaTraversalFailed, 1)
	if err := s.persistReviewDeltas(deltas); err != nil {
		return 0, nil, fmt.Errorf("persist review deltas: %w", err)
	}
	return 1, deltas, nil
}

func (s *migrationStore) setNodeExcludedWithPropagation(nodeID string, excluded bool) (int64, map[string]int64, error) {
	// Exclude/unexclude is SRC-only: only source nodes can be excluded from copy; no DST lookup.
	node, err := db.GetNodeByID(s.db, "SRC", nodeID)
	if err != nil {
		return 0, nil, err
	}
	if node == nil {
		return 0, nil, fmt.Errorf("node %s not found in SRC", nodeID)
	}
	q := "SRC"
	rootPath := node.Path
	var affected int64
	var buckets db.CopyStatusBucketsSubtreeNotExcluded
	err = s.db.RunWrite(context.Background(), func(sess *db.WriteSession) error {
		return sess.WithTx(func(w *db.Writer) error {
			excl, notExcl, err := w.CountExcludedInSubtree(q, rootPath)
			if err != nil {
				return err
			}
			if excluded {
				affected = notExcl
			} else {
				affected = excl
			}
			if excluded {
				var err2 error
				buckets, err2 = w.CountCopyStatusBucketsSubtreeNotExcluded(rootPath)
				if err2 != nil {
					return err2
				}
				return w.InsertExclusionEventsForSubtree(q, rootPath)
			}
			return w.InsertUnexcludeEventsForSubtree(q, rootPath)
		})
	})
	if err != nil {
		return 0, nil, fmt.Errorf("set exclusion with propagation: %w", err)
	}
	deltas := make(map[string]int64)
	if excluded {
		addReviewDelta(deltas, DeltaExcluded, affected)
		addReviewDelta(deltas, DeltaCopyPending, -buckets.Pending)
		addReviewDelta(deltas, DeltaCopyFailed, -buckets.Failed)
		addReviewDelta(deltas, DeltaCopySuccessful, -buckets.Successful)
	} else {
		addReviewDelta(deltas, DeltaExcluded, -affected)
		addReviewDelta(deltas, DeltaCopyPending, affected)
	}
	if err := s.persistReviewDeltas(deltas); err != nil {
		return 0, nil, fmt.Errorf("persist review deltas: %w", err)
	}
	return affected, deltas, nil
}

func conditionStringValue(v any) (string, bool) {
	if v == nil {
		return "", false
	}
	switch t := v.(type) {
	case string:
		return t, true
	case float64:
		return strconv.FormatInt(int64(t), 10), true
	case int:
		return strconv.Itoa(t), true
	case int64:
		return strconv.FormatInt(t, 10), true
	default:
		return fmt.Sprintf("%v", t), true
	}
}

func conditionIntValue(v any) (int, bool) {
	switch t := v.(type) {
	case int:
		return t, true
	case int64:
		return int(t), true
	case float64:
		return int(t), true
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(t))
		return n, err == nil
	default:
		return 0, false
	}
}

func conditionInt64Value(v any) (int64, bool) {
	switch t := v.(type) {
	case int:
		return int64(t), true
	case int64:
		return t, true
	case float64:
		return int64(t), true
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func normalizeDepthSizeOp(op string) string {
	switch strings.ToLower(strings.TrimSpace(op)) {
	case "gt", ">":
		return ">"
	case "gte", ">=":
		return ">="
	case "lt", "<":
		return "<"
	case "lte", "<=":
		return "<="
	case "equals", "=":
		return "="
	default:
		return "="
	}
}

// searchRequestToReviewFilter maps API/UI SearchRequest onto db.ReviewFilter (merged view, status from events).
func searchRequestToReviewFilter(req SearchRequest) db.ReviewFilter {
	f := db.ReviewFilter{
		ParentPath:       strings.TrimSpace(req.Path),
		Query:            strings.TrimSpace(req.Query),
		FoldersOnly:      req.FoldersOnly,
		ExcludeRoot:      req.Path == "",
		Status:           strings.TrimSpace(req.Status),
		StatusSearchType: strings.TrimSpace(req.StatusSearchType),
	}
	for _, c := range req.Conditions {
		field := strings.ToLower(strings.TrimSpace(c.Field))
		switch field {
		case "path":
			if s, ok := conditionStringValue(c.Value); ok && strings.TrimSpace(s) != "" {
				f.Query = strings.TrimSpace(s)
				f.QueryField = "path"
			}
		case "name":
			if s, ok := conditionStringValue(c.Value); ok && strings.TrimSpace(s) != "" {
				f.Query = strings.TrimSpace(s)
				f.QueryField = "name"
			}
		case "type":
			if s, ok := conditionStringValue(c.Value); ok {
				f.TypeFilter = strings.ToLower(strings.TrimSpace(s))
			}
		case "traversalstatus":
			if s, ok := conditionStringValue(c.Value); ok {
				f.TraversalStatus = strings.TrimSpace(s)
			}
		case "copystatus":
			if s, ok := conditionStringValue(c.Value); ok {
				f.CopyStatus = strings.TrimSpace(s)
			}
		case "depth":
			f.DepthOperator = normalizeDepthSizeOp(c.Operator)
			if n, ok := conditionIntValue(c.Value); ok {
				f.DepthValue = &n
			}
		case "size":
			f.SizeOperator = normalizeDepthSizeOp(c.Operator)
			if n, ok := conditionInt64Value(c.Value); ok {
				f.SizeValue = &n
			}
		}
	}
	if f.StatusSearchType != "" || f.TraversalStatus != "" || f.CopyStatus != "" {
		f.Status = ""
	}
	return f
}

func sanitizeSort(sortBy, sortDirection string) string {
	column := "path"
	switch strings.ToLower(strings.TrimSpace(sortBy)) {
	case "name":
		column = "name"
	case "depth":
		column = "depth"
	case "size":
		column = "size"
	case "type":
		column = "type"
	case "status", "traversalstatus", "traversal_status":
		column = "src_traversal_status"
	case "copystatus", "copy_status":
		column = "copy_status"
	case "path":
		column = "path"
	}
	direction := "ASC"
	if strings.EqualFold(strings.TrimSpace(sortDirection), "desc") {
		direction = "DESC"
	}
	primary := column + " " + direction
	// Stable ordering and deterministic pagination when primary values tie.
	if column == "path" {
		return primary
	}
	return primary + ", path ASC"
}

func (s *migrationStore) listChildrenDiffs(req ListChildrenDiffsRequest) (ListChildrenDiffsResult, error) {
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	offset := req.Offset
	if offset < 0 {
		offset = 0
	}
	orderBy := sanitizeSort(req.SortBy, req.SortDirection)
	f := db.ReviewFilter{
		ParentPath:  req.Path,
		Status:      req.Status,
		FoldersOnly: req.FoldersOnly,
	}
	rows, total, err := db.ListMergedReviewDiffs(s.db, f, orderBy, limit, offset)
	if err != nil {
		return ListChildrenDiffsResult{}, err
	}
	items := make([]DiffItem, 0, len(rows))
	for i := range rows {
		items = append(items, mergedRowToDiffItem(rows[i]))
	}
	if err := s.enrichDiffItemsWithFailureLogs(items); err != nil {
		return ListChildrenDiffsResult{}, err
	}
	return ListChildrenDiffsResult{
		Items:  items,
		Total:  total,
		Limit:  limit,
		Offset: offset,
	}, nil
}

func (s *migrationStore) searchPathReviewItems(req SearchRequest) (SearchResult, error) {
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	offset := req.Offset
	if offset < 0 {
		offset = 0
	}
	orderBy := sanitizeSort(req.SortBy, req.SortDirection)
	f := searchRequestToReviewFilter(req)
	rows, total, err := db.ListMergedReviewDiffs(s.db, f, orderBy, limit, offset)
	if err != nil {
		return SearchResult{}, err
	}
	items := make([]DiffItem, 0, len(rows))
	for i := range rows {
		items = append(items, mergedRowToDiffItem(rows[i]))
	}
	if err := s.enrichDiffItemsWithFailureLogs(items); err != nil {
		return SearchResult{}, err
	}
	return SearchResult{
		Items:  items,
		Total:  total,
		Limit:  limit,
		Offset: offset,
	}, nil
}

func (s *migrationStore) getChildrenDiffsStats(path string, foldersOnly bool) (DiffsStats, error) {
	f := db.ReviewFilter{ParentPath: path, FoldersOnly: foldersOnly}
	stats, err := db.GetMergedReviewStats(s.db, f)
	if err != nil {
		return DiffsStats{}, err
	}
	return DiffsStats{
		Total:           stats.Total,
		Folders:         stats.Folders,
		Files:           stats.Files,
		MissingOnSource: stats.MissingOnSource,
		MissingOnDest:   stats.MissingOnDest,
		Excluded:        stats.Excluded,
	}, nil
}

func (s *migrationStore) getSearchStats(req SearchRequest) (DiffsStats, error) {
	f := searchRequestToReviewFilter(req)
	stats, err := db.GetMergedReviewStats(s.db, f)
	if err != nil {
		return DiffsStats{}, err
	}
	return DiffsStats{
		Total:           stats.Total,
		Folders:         stats.Folders,
		Files:           stats.Files,
		MissingOnSource: stats.MissingOnSource,
		MissingOnDest:   stats.MissingOnDest,
		Excluded:        stats.Excluded,
	}, nil
}

func (s *migrationStore) getQueueMetrics() (QueueMetricsSnapshot, error) {
	raw, err := s.db.GetAllQueueStats()
	if err != nil {
		return QueueMetricsSnapshot{}, err
	}
	out := QueueMetricsSnapshot{
		Queues: make(map[string]map[string]any, len(raw)),
	}
	for key, blob := range raw {
		var parsed map[string]any
		if err := json.Unmarshal(blob, &parsed); err != nil {
			parsed = map[string]any{
				"raw": string(blob),
			}
		}
		out.Queues[key] = parsed
	}
	return out, nil
}
