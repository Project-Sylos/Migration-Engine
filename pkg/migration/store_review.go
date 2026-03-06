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
		CopyStatus:         r.CopyStatus,
		Excluded:           r.Excluded,
		Size:               r.Size,
		MissingOnSource:    r.SrcNodeID == "",
		MissingOnDest:      r.DstNodeID == "",
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

func (s *migrationStore) setNodeExcluded(queueType, nodeID string, excluded bool) error {
	q := strings.ToUpper(queueType)
	if q != "DST" {
		q = "SRC"
	}
	return s.db.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			return w.SetNodeExcluded(q, nodeID, excluded)
		})
	})
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

func (s *migrationStore) setNodeCopyStatus(nodeID, status string) error {
	return s.db.RunWrite(context.Background(), func(sess *db.WriteSession) error {
		return sess.WithTx(func(w *db.Writer) error {
			return w.SetNodeCopyStatus("SRC", nodeID, status)
		})
	})
}

// markNodeForRetryDiscovery looks up the node by ID in SRC then DST (nodeID is either a SRC or DST node ID).
// For a SRC node, the DST counterpart is resolved by path (path_hash), not by ID; then DST children of that path are marked pending.
func (s *migrationStore) markNodeForRetryDiscovery(nodeID string) error {
	srcNode, err := db.GetNodeByID(s.db, "SRC", nodeID)
	if err != nil {
		return err
	}
	if srcNode != nil {
		return s.db.RunWrite(context.Background(), func(sess *db.WriteSession) error {
			return sess.WithTx(func(w *db.Writer) error {
				if err := w.SetNodeTraversalStatus("SRC", nodeID, db.StatusPending); err != nil {
					return err
				}
				// DST counterpart is same path (join by path_hash), not same ID
				dstAtPath, _ := db.GetNodeByPath(s.db, "DST", srcNode.Path)
				if dstAtPath != nil {
					return w.InsertDstChildrenTraversalStatusEvents(srcNode.Path, db.StatusPending)
				}
				return nil
			})
		})
	}
	dstNode, err := db.GetNodeByID(s.db, "DST", nodeID)
	if err != nil || dstNode == nil {
		if err != nil {
			return err
		}
		return fmt.Errorf("node %s not found", nodeID)
	}
	return s.db.RunWrite(context.Background(), func(sess *db.WriteSession) error {
		return sess.WithTx(func(w *db.Writer) error {
			return w.SetNodeTraversalStatus("DST", nodeID, db.StatusPending)
		})
	})
}

// unmarkNodeForRetryDiscovery looks up the node by ID in SRC then DST. For a SRC node, the DST counterpart is by path (path_hash); then DST children of that path are set back to not_on_src.
func (s *migrationStore) unmarkNodeForRetryDiscovery(nodeID string) error {
	srcNode, err := db.GetNodeByID(s.db, "SRC", nodeID)
	if err != nil {
		return err
	}
	if srcNode != nil {
		return s.db.RunWrite(context.Background(), func(sess *db.WriteSession) error {
			return sess.WithTx(func(w *db.Writer) error {
				if err := w.SetNodeTraversalStatus("SRC", nodeID, db.StatusFailed); err != nil {
					return err
				}
				dstAtPath, _ := db.GetNodeByPath(s.db, "DST", srcNode.Path)
				if dstAtPath != nil {
					return w.InsertDstChildrenTraversalStatusEvents(srcNode.Path, db.StatusNotOnSrc)
				}
				return nil
			})
		})
	}
	dstNode, err := db.GetNodeByID(s.db, "DST", nodeID)
	if err != nil || dstNode == nil {
		if err != nil {
			return err
		}
		return fmt.Errorf("node %s not found", nodeID)
	}
	return s.db.RunWrite(context.Background(), func(sess *db.WriteSession) error {
		return sess.WithTx(func(w *db.Writer) error {
			return w.SetNodeTraversalStatus("DST", nodeID, db.StatusFailed)
		})
	})
}

func (s *migrationStore) setNodeExcludedWithPropagation(queueType, nodeID string, excluded bool) error {
	q := strings.ToUpper(queueType)
	if q != "DST" {
		q = "SRC"
	}
	node, err := db.GetNodeByID(s.db, q, nodeID)
	if err != nil {
		return err
	}
	if node == nil {
		return fmt.Errorf("node %s not found in %s", nodeID, q)
	}
	err = s.db.RunWrite(context.Background(), func(sess *db.WriteSession) error {
		return sess.WithTx(func(w *db.Writer) error {
			if excluded {
				return w.InsertExclusionEventsForSubtree(q, node.Path)
			}
			return w.InsertUnexcludeEventsForSubtree(q, node.Path)
		})
	})
	if err != nil {
		return fmt.Errorf("set exclusion with propagation: %w", err)
	}
	return nil
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
