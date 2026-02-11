// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
)

func tableName(table string) string {
	if table == "DST" {
		return tableDstNodes
	}
	return tableSrcNodes
}

// GetNodeByID returns the node by id from the given table. Uses pull conn so we see appender-written data.
func GetNodeByID(d *DB, table, id string) (*NodeState, error) {
	queueType := table
	if queueType != "SRC" && queueType != "DST" {
		queueType = "SRC"
	}
	conn, err := d.GetDBForPulls(queueType)
	if err != nil {
		return nil, err
	}
	t := tableName(table)
	ctx := context.Background()
	var n NodeState
	var size sql.NullInt64
	err = conn.QueryRowContext(ctx,
		`SELECT id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, traversal_status, copy_status, excluded, errors
		 FROM `+t+` WHERE id = $1`,
		id,
	).Scan(&n.ID, &n.ServiceID, &n.ParentID, &n.ParentServiceID, &n.Path, &n.ParentPath, &n.Type, &size, &n.MTime, &n.Depth, &n.TraversalStatus, &n.CopyStatus, &n.Excluded, &n.Errors)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if size.Valid {
		n.Size = size.Int64
	}
	n.Status = n.TraversalStatus
	return &n, nil
}

// GetNodeByPath returns the node by path from the given table. Uses pull conn so we see appender-written data.
func GetNodeByPath(d *DB, table, path string) (*NodeState, error) {
	queueType := table
	if queueType != "SRC" && queueType != "DST" {
		queueType = "SRC"
	}
	conn, err := d.GetDBForPulls(queueType)
	if err != nil {
		return nil, err
	}
	t := tableName(table)
	ctx := context.Background()
	var n NodeState
	var size sql.NullInt64
	err = conn.QueryRowContext(ctx,
		`SELECT id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, traversal_status, copy_status, excluded, errors
		 FROM `+t+` WHERE path = $1`,
		path,
	).Scan(&n.ID, &n.ServiceID, &n.ParentID, &n.ParentServiceID, &n.Path, &n.ParentPath, &n.Type, &size, &n.MTime, &n.Depth, &n.TraversalStatus, &n.CopyStatus, &n.Excluded, &n.Errors)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if size.Valid {
		n.Size = size.Int64
	}
	n.Status = n.TraversalStatus
	n.Name = n.Path
	if n.Path != "" && n.ParentPath != n.Path {
		// Name could be last segment; for simplicity use path
		n.Name = n.Path
	}
	return &n, nil
}

// GetRootNode returns the root node (path = '/') from the given table.
func GetRootNode(d *DB, table string) (id string, state *NodeState, ok bool) {
	state, err := GetNodeByPath(d, table, "/")
	if err != nil || state == nil {
		return "", nil, false
	}
	return state.ID, state, true
}

// GetChildrenByParentPath returns up to limit children with the given parent_path.
func GetChildrenByParentPath(d *DB, table, parentPath string, limit int) ([]*NodeState, error) {
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	t := tableName(table)
	ctx := context.Background()
	rows, err := conn.QueryContext(ctx,
		`SELECT id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, traversal_status, copy_status, excluded, errors
		 FROM `+t+` WHERE parent_path = $1 ORDER BY id LIMIT $2`,
		parentPath, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*NodeState
	for rows.Next() {
		var n NodeState
		var size sql.NullInt64
		if err := rows.Scan(&n.ID, &n.ServiceID, &n.ParentID, &n.ParentServiceID, &n.Path, &n.ParentPath, &n.Type, &size, &n.MTime, &n.Depth, &n.TraversalStatus, &n.CopyStatus, &n.Excluded, &n.Errors); err != nil {
			return nil, err
		}
		if size.Valid {
			n.Size = size.Int64
		}
		n.Status = n.TraversalStatus
		n.Name = n.Path
		out = append(out, &n)
	}
	return out, rows.Err()
}

// GetChildrenByParentID returns up to limit children with the given parent_id.
func GetChildrenByParentID(d *DB, table, parentID string, limit int) ([]*NodeState, error) {
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	t := tableName(table)
	ctx := context.Background()
	rows, err := conn.QueryContext(ctx,
		`SELECT id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, traversal_status, copy_status, excluded, errors
		 FROM `+t+` WHERE parent_id = $1 ORDER BY id LIMIT $2`,
		parentID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*NodeState
	for rows.Next() {
		var n NodeState
		var size sql.NullInt64
		if err := rows.Scan(&n.ID, &n.ServiceID, &n.ParentID, &n.ParentServiceID, &n.Path, &n.ParentPath, &n.Type, &size, &n.MTime, &n.Depth, &n.TraversalStatus, &n.CopyStatus, &n.Excluded, &n.Errors); err != nil {
			return nil, err
		}
		if size.Valid {
			n.Size = size.Int64
		}
		n.Status = n.TraversalStatus
		n.Name = n.Path
		out = append(out, &n)
	}
	return out, rows.Err()
}

// GetChildrenIDsByParentID returns child ids for the given parent_id (up to limit).
func GetChildrenIDsByParentID(d *DB, table, parentID string, limit int) ([]string, error) {
	children, err := GetChildrenByParentID(d, table, parentID, limit)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(children))
	for _, c := range children {
		ids = append(ids, c.ID)
	}
	return ids, nil
}

// ListNodesByDepthKeyset returns nodes at the given depth, ordered by id, after afterID, limit rows.
// If statusFilter is non-empty, only rows with traversal_status = statusFilter are returned (e.g. StatusPending).
// Uses pull conn so pulls see the same data as writes (roots, node inserts).
func ListNodesByDepthKeyset(d *DB, table string, depth int, afterID, statusFilter string, limit int) ([]FetchResult, error) {
	queueType := table
	if queueType != "SRC" && queueType != "DST" {
		queueType = "SRC"
	}
	conn, err := d.GetDBForPulls(queueType)
	if err != nil {
		return nil, err
	}
	t := tableName(table)
	ctx := context.Background()
	var rows *sql.Rows
	if statusFilter != "" {
		if afterID == "" {
			// fmt.Printf("[ListNodesByDepthKeyset] table=%s depth=%d afterID=%q statusFilter=%q limit=%d\n  SQL: SELECT ... FROM %s WHERE depth = $1 AND traversal_status = $2 ORDER BY id LIMIT $3  ($1=%d $2=%q $3=%d)\n", t, depth, afterID, statusFilter, limit, t, depth, statusFilter, limit)
			rows, err = conn.QueryContext(ctx,
				`SELECT id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, traversal_status, copy_status, excluded, errors
				 FROM `+t+` WHERE depth = $1 AND traversal_status = $2 ORDER BY id LIMIT $3`,
				depth, statusFilter, limit,
			)
		} else {
			// fmt.Printf("[ListNodesByDepthKeyset] table=%s depth=%d afterID=%q statusFilter=%q limit=%d\n  SQL: SELECT ... FROM %s WHERE depth = $1 AND traversal_status = $2 AND id > $3 ORDER BY id LIMIT $4  ($1=%d $2=%q $3=%q $4=%d)\n", t, depth, afterID, statusFilter, limit, t, depth, statusFilter, afterID, limit)
			rows, err = conn.QueryContext(ctx,
				`SELECT id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, traversal_status, copy_status, excluded, errors
				 FROM `+t+` WHERE depth = $1 AND traversal_status = $2 AND id > $3 ORDER BY id LIMIT $4`,
				depth, statusFilter, afterID, limit,
			)
		}
	} else {
		if afterID == "" {
			// fmt.Printf("[ListNodesByDepthKeyset] table=%s depth=%d afterID=%q statusFilter=(none) limit=%d\n  SQL: SELECT ... FROM %s WHERE depth = $1 ORDER BY id LIMIT $2  ($1=%d $2=%d)\n", t, depth, afterID, limit, t, depth, limit)
			rows, err = conn.QueryContext(ctx,
				`SELECT id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, traversal_status, copy_status, excluded, errors
				 FROM `+t+` WHERE depth = $1 ORDER BY id LIMIT $2`,
				depth, limit,
			)
		} else {
			// fmt.Printf("[ListNodesByDepthKeyset] table=%s depth=%d afterID=%q statusFilter=(none) limit=%d\n  SQL: SELECT ... FROM %s WHERE depth = $1 AND id > $2 ORDER BY id LIMIT $3  ($1=%d $2=%q $3=%d)\n", t, depth, afterID, limit, t, depth, afterID, limit)
			rows, err = conn.QueryContext(ctx,
				`SELECT id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, traversal_status, copy_status, excluded, errors
				 FROM `+t+` WHERE depth = $1 AND id > $2 ORDER BY id LIMIT $3`,
				depth, afterID, limit,
			)
		}
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FetchResult
	for rows.Next() {
		var n NodeState
		var size sql.NullInt64
		if err := rows.Scan(&n.ID, &n.ServiceID, &n.ParentID, &n.ParentServiceID, &n.Path, &n.ParentPath, &n.Type, &size, &n.MTime, &n.Depth, &n.TraversalStatus, &n.CopyStatus, &n.Excluded, &n.Errors); err != nil {
			return nil, err
		}
		if size.Valid {
			n.Size = size.Int64
		}
		n.Status = n.TraversalStatus
		n.Name = n.Path
		out = append(out, FetchResult{Key: n.ID, State: &n})
	}
	return out, rows.Err()
}

// ListNodesCopyKeyset returns src_nodes at depth for copy phase with copy_status = 'pending', ordered by id, after afterID, limit. Optional nodeType filter (folder/file or "" for both).
// Uses pull conn so pulls see the same data as writes.
func ListNodesCopyKeyset(d *DB, depth int, nodeType, afterID string, limit int) ([]FetchResult, error) {
	conn, err := d.GetDBForPulls("SRC")
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	var rows *sql.Rows
	if nodeType == "" {
		if afterID == "" {
			rows, err = conn.QueryContext(ctx,
				`SELECT id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, traversal_status, copy_status, excluded, errors
				 FROM src_nodes WHERE depth = $1 AND copy_status = 'pending' ORDER BY id LIMIT $2`,
				depth, limit,
			)
		} else {
			rows, err = conn.QueryContext(ctx,
				`SELECT id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, traversal_status, copy_status, excluded, errors
				 FROM src_nodes WHERE depth = $1 AND copy_status = 'pending' AND id > $2 ORDER BY id LIMIT $3`,
				depth, afterID, limit,
			)
		}
	} else {
		if afterID == "" {
			rows, err = conn.QueryContext(ctx,
				`SELECT id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, traversal_status, copy_status, excluded, errors
				 FROM src_nodes WHERE depth = $1 AND type = $2 AND copy_status = 'pending' ORDER BY id LIMIT $3`,
				depth, nodeType, limit,
			)
		} else {
			rows, err = conn.QueryContext(ctx,
				`SELECT id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, traversal_status, copy_status, excluded, errors
				 FROM src_nodes WHERE depth = $1 AND type = $2 AND copy_status = 'pending' AND id > $3 ORDER BY id LIMIT $4`,
				depth, nodeType, afterID, limit,
			)
		}
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FetchResult
	for rows.Next() {
		var n NodeState
		var size sql.NullInt64
		if err := rows.Scan(&n.ID, &n.ServiceID, &n.ParentID, &n.ParentServiceID, &n.Path, &n.ParentPath, &n.Type, &size, &n.MTime, &n.Depth, &n.TraversalStatus, &n.CopyStatus, &n.Excluded, &n.Errors); err != nil {
			return nil, err
		}
		if size.Valid {
			n.Size = size.Int64
		}
		n.Status = n.TraversalStatus
		n.Name = n.Path
		out = append(out, FetchResult{Key: n.ID, State: &n})
	}
	return out, rows.Err()
}

// SubtreeStats holds aggregate counts for a subtree (path = rootPath OR path LIKE rootPath || '/%').
type SubtreeStats struct {
	TotalNodes   int
	TotalFolders int
	TotalFiles   int
	MaxDepth     int
}

// CountSubtree returns aggregate counts for the subtree at rootPath (path = rootPath OR path LIKE rootPath || '/%'; for rootPath "/" uses path LIKE '/%'). Single SQL query, no DFS.
func CountSubtree(d *DB, table, rootPath string) (SubtreeStats, error) {
	conn, err := d.GetDB()
	if err != nil {
		return SubtreeStats{}, err
	}
	t := tableName(table)
	ctx := context.Background()
	var total, folders, files int
	var maxDepth sql.NullInt64
	if rootPath == "/" {
		err = conn.QueryRowContext(ctx,
			`SELECT COUNT(*)::INT, COUNT(*) FILTER (WHERE type = 'folder')::INT, COUNT(*) FILTER (WHERE type = 'file')::INT, COALESCE(MAX(depth), 0)::BIGINT FROM `+t+` WHERE path LIKE '/%'`,
		).Scan(&total, &folders, &files, &maxDepth)
	} else {
		prefix := rootPath + "/%"
		err = conn.QueryRowContext(ctx,
			`SELECT COUNT(*)::INT, COUNT(*) FILTER (WHERE type = 'folder')::INT, COUNT(*) FILTER (WHERE type = 'file')::INT, COALESCE(MAX(depth), 0)::BIGINT FROM `+t+` WHERE path = $1 OR path LIKE $2`,
			rootPath, prefix,
		).Scan(&total, &folders, &files, &maxDepth)
	}
	if err != nil {
		return SubtreeStats{}, err
	}
	stats := SubtreeStats{TotalNodes: total, TotalFolders: folders, TotalFiles: files}
	if maxDepth.Valid {
		stats.MaxDepth = int(maxDepth.Int64)
	}
	return stats, nil
}

// CountExcludedInSubtree returns the number of excluded nodes in the subtree at rootPath. Single SQL query.
func CountExcludedInSubtree(d *DB, table, rootPath string) (int, error) {
	conn, err := d.GetDB()
	if err != nil {
		return 0, err
	}
	t := tableName(table)
	ctx := context.Background()
	var n int
	if rootPath == "/" {
		err = conn.QueryRowContext(ctx, `SELECT COUNT(*)::INT FROM `+t+` WHERE excluded = true AND path LIKE '/%'`).Scan(&n)
	} else {
		prefix := rootPath + "/%"
		err = conn.QueryRowContext(ctx, `SELECT COUNT(*)::INT FROM `+t+` WHERE excluded = true AND (path = $1 OR path LIKE $2)`, rootPath, prefix).Scan(&n)
	}
	if err != nil {
		return 0, err
	}
	return n, nil
}

// CountExcluded returns the number of nodes with excluded = true in the given table.
func CountExcluded(d *DB, table string) (int, error) {
	conn, err := d.GetDB()
	if err != nil {
		return 0, err
	}
	t := tableName(table)
	ctx := context.Background()
	var n int
	err = conn.QueryRowContext(ctx, `SELECT COUNT(*)::INT FROM `+t+` WHERE excluded = true`).Scan(&n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// CountNodes returns the total number of nodes in the given table (src_nodes or dst_nodes). Live table count, not from stats. Uses pull conn so we see appender-written data.
func CountNodes(d *DB, table string) (int, error) {
	queueType := table
	if queueType != "SRC" && queueType != "DST" {
		queueType = "SRC"
	}
	conn, err := d.GetDBForPulls(queueType)
	if err != nil {
		return 0, err
	}
	t := tableName(table)
	ctx := context.Background()
	var n int
	err = conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+t).Scan(&n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// GetAllLevels returns distinct depth values for the table, sorted.
func GetAllLevels(d *DB, table string) ([]int, error) {
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	t := tableName(table)
	ctx := context.Background()
	rows, err := conn.QueryContext(ctx, `SELECT DISTINCT depth FROM `+t+` ORDER BY depth`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int
	for rows.Next() {
		var depth int
		if err := rows.Scan(&depth); err != nil {
			return nil, err
		}
		out = append(out, depth)
	}
	return out, rows.Err()
}

// BatchGetNodeMeta returns meta (id, depth, type, traversal_status, copy_status) for the given ids.
func BatchGetNodeMeta(d *DB, table string, ids []string) (map[string]NodeMeta, error) {
	if len(ids) == 0 {
		return make(map[string]NodeMeta), nil
	}
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	t := tableName(table)
	ctx := context.Background()
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	q := "SELECT id, depth, type, traversal_status, copy_status FROM " + t + " WHERE id IN ("
	for i := 0; i < len(ids); i++ {
		if i > 0 {
			q += ","
		}
		q += "$" + strconv.Itoa(i+1)
	}
	q += ")"
	rows, err := conn.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]NodeMeta)
	for rows.Next() {
		var m NodeMeta
		if err := rows.Scan(&m.ID, &m.Depth, &m.Type, &m.TraversalStatus, &m.CopyStatus); err != nil {
			return nil, err
		}
		out[m.ID] = m
	}
	return out, rows.Err()
}

// ListDstBatchWithSrcChildren returns the next batch of DST nodes at depth (keyset afterID, limit) and their SRC children (join by parent_path = d.path) in one query. Optional traversalStatus filter (e.g. StatusPending).
// Returns DST rows as []FetchResult and per-DST-ID SRC children as map[string][]*NodeState. Cursor must be round-scoped; reset on round advance, mode switch, and after seal.
// Uses DST pull conn so pulls see the same data as writes.
func ListDstBatchWithSrcChildren(d *DB, depth int, afterID string, limit int, traversalStatus string) ([]FetchResult, map[string][]*NodeState, error) {
	conn, err := d.GetDBForPulls("DST")
	if err != nil {
		return nil, nil, err
	}
	ctx := context.Background()
	// CTE: next batch of DST nodes at depth with optional status filter
	cteWhere := "depth = $1"
	args := []interface{}{depth}
	argNum := 2
	if afterID != "" {
		cteWhere += " AND id > $" + strconv.Itoa(argNum)
		args = append(args, afterID)
		argNum++
	}
	if traversalStatus != "" {
		cteWhere += " AND traversal_status = $" + strconv.Itoa(argNum)
		args = append(args, traversalStatus)
		argNum++
	}
	args = append(args, limit)
	limitParam := "$" + strconv.Itoa(argNum)

	q := `WITH dst_batch AS (
  SELECT id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, traversal_status, copy_status, excluded, errors
  FROM ` + tableDstNodes + ` WHERE ` + cteWhere + ` ORDER BY id LIMIT ` + limitParam + `
)
SELECT
  d.id AS d_id, d.service_id AS d_service_id, d.parent_id AS d_parent_id, d.parent_service_id AS d_parent_service_id, d.path AS d_path, d.parent_path AS d_parent_path, d.type AS d_type, d.size AS d_size, d.mtime AS d_mtime, d.depth AS d_depth, d.traversal_status AS d_traversal_status, d.copy_status AS d_copy_status, d.excluded AS d_excluded, d.errors AS d_errors,
  COALESCE(s.id, '') AS s_id, COALESCE(s.service_id, '') AS s_service_id, COALESCE(s.parent_id, '') AS s_parent_id, COALESCE(s.parent_service_id, '') AS s_parent_service_id, COALESCE(s.path, '') AS s_path, COALESCE(s.parent_path, '') AS s_parent_path, COALESCE(s.type, '') AS s_type, s.size AS s_size, COALESCE(s.mtime, '') AS s_mtime, COALESCE(s.depth, 0) AS s_depth, COALESCE(s.traversal_status, '') AS s_traversal_status, COALESCE(s.copy_status, '') AS s_copy_status, COALESCE(s.excluded, false) AS s_excluded, COALESCE(s.errors, '') AS s_errors
FROM dst_batch d
LEFT JOIN ` + tableSrcNodes + ` s ON s.parent_path = d.path
ORDER BY d.id, s.id`

	rows, err := conn.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var dstBatch []FetchResult
	childrenByDstID := make(map[string][]*NodeState)
	var lastDstID string
	var lastDst *NodeState
	var dSize, sSize sql.NullInt64

	for rows.Next() {
		var dID, dServiceID, dParentID, dParentServiceID, dPath, dParentPath, dType, dMTime, dTraversalStatus, dCopyStatus, dErrors string
		var dExcluded bool
		var dDepth int
		var sID, sServiceID, sParentID, sParentServiceID, sPath, sParentPath, sType, sMTime, sTraversalStatus, sCopyStatus, sErrors string
		var sExcluded bool
		var sDepth int

		if err := rows.Scan(
			&dID, &dServiceID, &dParentID, &dParentServiceID, &dPath, &dParentPath, &dType, &dSize, &dMTime, &dDepth, &dTraversalStatus, &dCopyStatus, &dExcluded, &dErrors,
			&sID, &sServiceID, &sParentID, &sParentServiceID, &sPath, &sParentPath, &sType, &sSize, &sMTime, &sDepth, &sTraversalStatus, &sCopyStatus, &sExcluded, &sErrors,
		); err != nil {
			return nil, nil, err
		}

		if dID != lastDstID {
			lastDstID = dID
			lastDst = &NodeState{
				ID: dID, ServiceID: dServiceID, ParentID: dParentID, ParentServiceID: dParentServiceID, Path: dPath, ParentPath: dParentPath,
				Type: dType, MTime: dMTime, Depth: dDepth, TraversalStatus: dTraversalStatus, CopyStatus: dCopyStatus, Excluded: dExcluded, Errors: dErrors,
			}
			if dSize.Valid {
				lastDst.Size = dSize.Int64
			}
			lastDst.Status = lastDst.TraversalStatus
			lastDst.Name = lastDst.Path
			dstBatch = append(dstBatch, FetchResult{Key: dID, State: lastDst})
		}

		if sID != "" {
			child := &NodeState{
				ID: sID, ServiceID: sServiceID, ParentID: sParentID, ParentServiceID: sParentServiceID, Path: sPath, ParentPath: sParentPath,
				Type: sType, MTime: sMTime, Depth: sDepth, TraversalStatus: sTraversalStatus, CopyStatus: sCopyStatus, Excluded: sExcluded, Errors: sErrors,
			}
			if sSize.Valid {
				child.Size = sSize.Int64
			}
			child.Status = child.TraversalStatus
			if child.Path != "" {
				last := child.Path
				for i := len(last) - 1; i >= 0; i-- {
					if last[i] == '/' {
						if i+1 < len(last) {
							last = last[i+1:]
						}
						break
					}
				}
				child.Name = last
			} else {
				child.Name = child.Path
			}
			childrenByDstID[dID] = append(childrenByDstID[dID], child)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return dstBatch, childrenByDstID, nil
}

// GetSrcChildrenGroupedByParentPath returns SRC nodes grouped by parent_path. Used to batch-load expected children for DST folder tasks (join by parent_path = dst.path).
// Deprecated: prefer ListDstBatchWithSrcChildren for keyset-based DST pull + SRC children in one query.
func GetSrcChildrenGroupedByParentPath(d *DB, parentPaths []string) (map[string][]*NodeState, error) {
	out := make(map[string][]*NodeState)
	for _, p := range parentPaths {
		out[p] = nil
	}
	if len(parentPaths) == 0 {
		return out, nil
	}
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	const chunk = 500
	for i := 0; i < len(parentPaths); i += chunk {
		end := i + chunk
		if end > len(parentPaths) {
			end = len(parentPaths)
		}
		chunkPaths := parentPaths[i:end]
		q := `SELECT parent_path, id, service_id, parent_id, parent_service_id, path, type, size, mtime, depth, traversal_status, copy_status, excluded, errors
		 FROM src_nodes WHERE parent_path IN (`
		for j := 0; j < len(chunkPaths); j++ {
			if j > 0 {
				q += ","
			}
			q += "$" + strconv.Itoa(j+1)
		}
		q += ") ORDER BY parent_path, id"
		args := make([]interface{}, len(chunkPaths))
		for j, p := range chunkPaths {
			args[j] = p
		}
		rows, err := conn.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var n NodeState
			var parentPath string
			var size sql.NullInt64
			if err := rows.Scan(&parentPath, &n.ID, &n.ServiceID, &n.ParentID, &n.ParentServiceID, &n.Path, &n.Type, &size, &n.MTime, &n.Depth, &n.TraversalStatus, &n.CopyStatus, &n.Excluded, &n.Errors); err != nil {
				rows.Close()
				return nil, err
			}
			if size.Valid {
				n.Size = size.Int64
			}
			n.ParentPath = parentPath
			n.Status = n.TraversalStatus
			if n.Path != "" {
				// Name as display: last segment or path
				last := n.Path
				for i := len(last) - 1; i >= 0; i-- {
					if last[i] == '/' {
						if i+1 < len(last) {
							last = last[i+1:]
						}
						break
					}
				}
				n.Name = last
			} else {
				n.Name = n.Path
			}
			out[parentPath] = append(out[parentPath], &n)
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// GetDstIDToSrcPath returns for each DST id the path (which is the join key = "src path").
func GetDstIDToSrcPath(d *DB, dstIDs []string) (map[string]string, error) {
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

// GetDstIDFromSrcID returns the DST node id that has the same path as the given SRC node id (join by path).
func GetDstIDFromSrcID(d *DB, srcParentID string) (string, error) {
	src, err := GetNodeByID(d, "SRC", srcParentID)
	if err != nil || src == nil {
		return "", err
	}
	dst, err := GetNodeByPath(d, "DST", src.Path)
	if err != nil || dst == nil {
		return "", err
	}
	return dst.ID, nil
}

// BatchGetDstIDsFromSrcIDs returns map[srcID]dstID by resolving SRC id->path then DST path->id in two queries (no per-item reads).
func BatchGetDstIDsFromSrcIDs(d *DB, srcIDs []string) (map[string]string, error) {
	out := make(map[string]string)
	if len(srcIDs) == 0 {
		return out, nil
	}
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	// 1) SRC: id -> path
	placeholders := make([]string, len(srcIDs))
	args := make([]interface{}, len(srcIDs))
	for i := range srcIDs {
		placeholders[i] = "$" + strconv.Itoa(i+1)
		args[i] = srcIDs[i]
	}
	qSrc := "SELECT id, path FROM " + tableSrcNodes + " WHERE id IN (" + strings.Join(placeholders, ",") + ")"
	rows, err := conn.QueryContext(ctx, qSrc, args...)
	if err != nil {
		return nil, err
	}
	srcIDToPath := make(map[string]string)
	for rows.Next() {
		var id, path string
		if err := rows.Scan(&id, &path); err != nil {
			rows.Close()
			return nil, err
		}
		srcIDToPath[id] = path
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(srcIDToPath) == 0 {
		return out, nil
	}
	// 2) DST: path -> id (unique paths)
	paths := make([]string, 0, len(srcIDToPath))
	pathSet := make(map[string]struct{})
	for _, p := range srcIDToPath {
		if p == "" {
			continue
		}
		if _, ok := pathSet[p]; !ok {
			pathSet[p] = struct{}{}
			paths = append(paths, p)
		}
	}
	if len(paths) == 0 {
		return out, nil
	}
	placeholders = make([]string, len(paths))
	args = make([]interface{}, len(paths))
	for i := range paths {
		placeholders[i] = "$" + strconv.Itoa(i+1)
		args[i] = paths[i]
	}
	qDst := "SELECT id, path FROM " + tableDstNodes + " WHERE path IN (" + strings.Join(placeholders, ",") + ")"
	rows, err = conn.QueryContext(ctx, qDst, args...)
	if err != nil {
		return nil, err
	}
	pathToDstID := make(map[string]string)
	for rows.Next() {
		var id, path string
		if err := rows.Scan(&id, &path); err != nil {
			rows.Close()
			return nil, err
		}
		pathToDstID[path] = id
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for srcID, path := range srcIDToPath {
		if dstID, ok := pathToDstID[path]; ok && dstID != "" {
			out[srcID] = dstID
		}
	}
	return out, nil
}

// BatchGetNodesByID returns nodes by id for the given table in one query. Missing ids are omitted from the map.
func BatchGetNodesByID(d *DB, table string, ids []string) (map[string]*NodeState, error) {
	out := make(map[string]*NodeState)
	if len(ids) == 0 {
		return out, nil
	}
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	t := tableName(table)
	ctx := context.Background()
	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i := range ids {
		placeholders[i] = "$" + strconv.Itoa(i+1)
		args[i] = ids[i]
	}
	q := "SELECT id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, traversal_status, copy_status, excluded, errors FROM " + t + " WHERE id IN (" + strings.Join(placeholders, ",") + ")"
	rows, err := conn.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var size sql.NullInt64
	for rows.Next() {
		var n NodeState
		if err := rows.Scan(&n.ID, &n.ServiceID, &n.ParentID, &n.ParentServiceID, &n.Path, &n.ParentPath, &n.Type, &size, &n.MTime, &n.Depth, &n.TraversalStatus, &n.CopyStatus, &n.Excluded, &n.Errors); err != nil {
			return nil, err
		}
		if size.Valid {
			n.Size = size.Int64
		}
		n.Status = n.TraversalStatus
		out[n.ID] = &n
	}
	return out, rows.Err()
}

// BatchGetChildrenIDsByParentIDs returns map[parentID][]childID for the given table and parent ids.
func BatchGetChildrenIDsByParentIDs(d *DB, table string, parentIDs []string) (map[string][]string, error) {
	out := make(map[string][]string)
	for _, pid := range parentIDs {
		out[pid] = nil
	}
	if len(parentIDs) == 0 {
		return out, nil
	}
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	t := tableName(table)
	ctx := context.Background()
	q := "SELECT parent_id, id FROM " + t + " WHERE parent_id IN ("
	for i := 0; i < len(parentIDs); i++ {
		if i > 0 {
			q += ","
		}
		q += "$" + strconv.Itoa(i+1)
	}
	q += ")"
	args := make([]interface{}, len(parentIDs))
	for i, id := range parentIDs {
		args[i] = id
	}
	rows, err := conn.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var parentID, childID string
		if err := rows.Scan(&parentID, &childID); err != nil {
			return nil, err
		}
		out[parentID] = append(out[parentID], childID)
	}
	return out, rows.Err()
}
