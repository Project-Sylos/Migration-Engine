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
	if !excluded {
		return fmt.Errorf("setNodeExcludedWithPropagation: unexclude with propagation not implemented")
	}
	err = s.db.RunWrite(context.Background(), func(sess *db.WriteSession) error {
		return sess.WithTx(func(w *db.Writer) error {
			return w.InsertExclusionEventsForSubtree(q, node.Path)
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

	conn, err := s.db.GetDB()
	if err != nil {
		return ListChildrenDiffsResult{}, err
	}

	where := ` WHERE (src_parent_path = $1 OR dst_parent_path = $1) `
	args := []any{req.Path}
	if req.FoldersOnly {
		where += ` AND type = 'folder'`
	}
	if req.Status != "" {
		where += ` AND (src_traversal_status = $2 OR dst_traversal_status = $2 OR copy_status = $2)`
		args = append(args, req.Status)
	}

	base := db.MergedReviewQueryBase()

	countQuery := base + ` SELECT COUNT(*) FROM merged` + where
	var total int
	if err := conn.QueryRowContext(context.Background(), countQuery, args...).Scan(&total); err != nil {
		return ListChildrenDiffsResult{}, err
	}

	listQuery := base + ` SELECT path, name, depth, type, src_node_id, dst_node_id, src_traversal_status, dst_traversal_status, copy_status, excluded, size FROM merged` +
		where + ` ORDER BY ` + orderBy + ` LIMIT $` + fmt.Sprintf("%d", len(args)+1) + ` OFFSET $` + fmt.Sprintf("%d", len(args)+2)
	queryArgs := append(args, limit, offset)
	rows, err := conn.QueryContext(context.Background(), listQuery, queryArgs...)
	if err != nil {
		return ListChildrenDiffsResult{}, err
	}
	defer rows.Close()

	items := make([]DiffItem, 0, limit)
	for rows.Next() {
		var item DiffItem
		if err := rows.Scan(
			&item.Path,
			&item.Name,
			&item.Depth,
			&item.Type,
			&item.SrcNodeID,
			&item.DstNodeID,
			&item.SrcTraversalStatus,
			&item.DstTraversalStatus,
			&item.CopyStatus,
			&item.Excluded,
			&item.Size,
		); err != nil {
			return ListChildrenDiffsResult{}, err
		}
		item.MissingOnSource = item.SrcNodeID == ""
		item.MissingOnDest = item.DstNodeID == ""
		if item.Name == "" {
			item.Name = path.Base(item.Path)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
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
	listReq := ListChildrenDiffsRequest{
		Path:          req.Path,
		Limit:         req.Limit,
		Offset:        req.Offset,
		SortBy:        req.SortBy,
		SortDirection: req.SortDirection,
	}
	res, err := s.listChildrenDiffs(listReq)
	if err != nil {
		return SearchResult{}, err
	}
	if strings.TrimSpace(req.Query) == "" {
		return SearchResult{
			Items:  res.Items,
			Total:  res.Total,
			Limit:  res.Limit,
			Offset: res.Offset,
		}, nil
	}
	query := strings.ToLower(strings.TrimSpace(req.Query))
	filtered := make([]DiffItem, 0, len(res.Items))
	for i := range res.Items {
		item := res.Items[i]
		if strings.Contains(strings.ToLower(item.Path), query) || strings.Contains(strings.ToLower(item.Name), query) {
			filtered = append(filtered, item)
		}
	}
	return SearchResult{
		Items:  filtered,
		Total:  len(filtered),
		Limit:  res.Limit,
		Offset: res.Offset,
	}, nil
}

func (s *migrationStore) getChildrenDiffsStats(path string, foldersOnly bool) (DiffsStats, error) {
	res, err := s.listChildrenDiffs(ListChildrenDiffsRequest{
		Path:        path,
		Limit:       10000,
		Offset:      0,
		FoldersOnly: foldersOnly,
	})
	if err != nil {
		return DiffsStats{}, err
	}
	stats := DiffsStats{Total: res.Total}
	for i := range res.Items {
		item := res.Items[i]
		if item.Type == "folder" {
			stats.Folders++
		}
		if item.Type == "file" {
			stats.Files++
		}
		if item.MissingOnSource {
			stats.MissingOnSource++
		}
		if item.MissingOnDest {
			stats.MissingOnDest++
		}
		if item.Excluded {
			stats.Excluded++
		}
	}
	return stats, nil
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
