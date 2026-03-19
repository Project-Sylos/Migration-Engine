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
		switch node.TraversalStatus {
		case db.StatusPending:
			addReviewDelta(deltas, DeltaTraversalPending, -1)
		case db.StatusFailed:
			addReviewDelta(deltas, DeltaTraversalFailed, -1)
		default:
			addReviewDelta(deltas, DeltaTraversalPending, -1)
		}
	} else {
		addReviewDelta(deltas, DeltaExcluded, -1)
		addReviewDelta(deltas, DeltaTraversalPending, 1)
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
				if err := w.InsertExclusionEventsForSubtree(q, rootPath); err != nil {
					return err
				}
			} else {
				if err := w.InsertUnexcludeEventsForSubtree(q, rootPath); err != nil {
					return err
				}
			}
			return nil
		})
	})
	if err != nil {
		return 0, nil, fmt.Errorf("set exclusion with propagation: %w", err)
	}
	deltas := make(map[string]int64)
	if excluded {
		addReviewDelta(deltas, DeltaExcluded, affected)
	} else {
		addReviewDelta(deltas, DeltaExcluded, -affected)
	}
	if err := s.persistReviewDeltas(deltas); err != nil {
		return 0, nil, fmt.Errorf("persist review deltas: %w", err)
	}
	return affected, deltas, nil
}

func sanitizeSort(sortBy, sortDirection string) string {
	column := "path"
	switch strings.ToLower(sortBy) {
	case "name":
		column = "name"
	case "depth":
		column = "depth"
	case "size":
		column = "size"
	case "type":
		column = "type"
	case "status":
		column = "src_traversal_status"
	case "path":
		column = "path"
	}
	direction := "ASC"
	if strings.EqualFold(sortDirection, "desc") {
		direction = "DESC"
	}
	return column + " " + direction
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
	f := db.ReviewFilter{
		ParentPath:  req.Path,
		Query:       strings.TrimSpace(req.Query),
		Status:      req.Status,
		FoldersOnly: req.FoldersOnly,
		ExcludeRoot: req.Path == "", // global search: exclude root from results
	}
	rows, total, err := db.ListMergedReviewDiffs(s.db, f, orderBy, limit, offset)
	if err != nil {
		return SearchResult{}, err
	}
	items := make([]DiffItem, 0, len(rows))
	for i := range rows {
		items = append(items, mergedRowToDiffItem(rows[i]))
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
	f := db.ReviewFilter{
		ParentPath:  req.Path,
		Query:       strings.TrimSpace(req.Query),
		Status:      req.Status,
		FoldersOnly: req.FoldersOnly,
		ExcludeRoot: req.Path == "",
	}
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
