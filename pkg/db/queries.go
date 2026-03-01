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

// Current status from events (join with nodes to get traversal_status, copy_status, excluded).
const (
	cteSrcCurrentStatus = `(SELECT id, arg_max(traversal_status, event_time) AS traversal_status, arg_max(copy_status, event_time) AS copy_status FROM src_status_events GROUP BY id)`
	cteDstCurrentStatus = `(SELECT id, arg_max(traversal_status, event_time) AS traversal_status FROM dst_status_events GROUP BY id)`
)

func statusJoinExpr(table string) (nodesAlias, cteAlias, cte string) {
	if table == "DST" {
		return "n", "e", cteDstCurrentStatus
	}
	return "n", "e", cteSrcCurrentStatus
}

// Node columns from joined form: n.* plus e.traversal_status, e.copy_status (SRC only), excluded derived, errors as ''.
func selectNodeColsWithStatus(table string) string {
	t := tableName(table)
	n, e, cte := statusJoinExpr(table)
	if table == "DST" {
		return `SELECT ` + n + `.id, ` + n + `.service_id, ` + n + `.parent_id, ` + n + `.parent_service_id, ` + n + `.path, ` + n + `.parent_path, ` + n + `.type, ` + n + `.size, ` + n + `.mtime, ` + n + `.depth, COALESCE(` + e + `.traversal_status,'') AS traversal_status, '' AS copy_status, (COALESCE(` + e + `.traversal_status,'') IN ('excluded','exclusion_inherited')) AS excluded, '' AS errors FROM ` + t + ` ` + n + ` LEFT JOIN ` + cte + ` ` + e + ` ON ` + n + `.id = ` + e + `.id`
	}
	return `SELECT ` + n + `.id, ` + n + `.service_id, ` + n + `.parent_id, ` + n + `.parent_service_id, ` + n + `.path, ` + n + `.parent_path, ` + n + `.type, ` + n + `.size, ` + n + `.mtime, ` + n + `.depth, COALESCE(` + e + `.traversal_status,'') AS traversal_status, COALESCE(` + e + `.copy_status,'') AS copy_status, (COALESCE(` + e + `.traversal_status,'') IN ('excluded','exclusion_inherited')) AS excluded, '' AS errors FROM ` + t + ` ` + n + ` LEFT JOIN ` + cte + ` ` + e + ` ON ` + n + `.id = ` + e + `.id`
}

// QueryNodesForReview returns nodes from the given table (SRC or DST) with optional depth, status, excluded, pathLike filters. Status from events. Used by review API.
func QueryNodesForReview(d *DB, table string, depth *int, status string, excluded *bool, pathLike string, orderByPath bool, limit, offset int) ([]NodeState, error) {
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	nodeAlias, e, _ := statusJoinExpr(table)
	ctx := context.Background()
	base := selectNodeColsWithStatus(table) + ` WHERE 1=1`
	args := []interface{}{}
	param := 1
	if depth != nil {
		base += ` AND ` + nodeAlias + `.depth = $` + strconv.Itoa(param)
		args = append(args, *depth)
		param++
	}
	if status != "" {
		base += ` AND ` + e + `.traversal_status = $` + strconv.Itoa(param)
		args = append(args, status)
		param++
	}
	if excluded != nil && *excluded {
		base += ` AND (` + e + `.traversal_status = 'excluded' OR ` + e + `.traversal_status = 'exclusion_inherited')`
	} else if excluded != nil && !*excluded {
		base += ` AND (` + e + `.traversal_status IS NULL OR (` + e + `.traversal_status <> 'excluded' AND ` + e + `.traversal_status <> 'exclusion_inherited'))`
	}
	if pathLike != "" {
		base += ` AND ` + nodeAlias + `.path LIKE $` + strconv.Itoa(param)
		args = append(args, "%"+pathLike+"%")
		param++
	}
	if orderByPath {
		base += ` ORDER BY ` + nodeAlias + `.path`
	} else {
		base += ` ORDER BY ` + nodeAlias + `.id`
	}
	base += ` LIMIT $` + strconv.Itoa(param) + ` OFFSET $` + strconv.Itoa(param+1)
	args = append(args, limit, offset)
	rows, err := conn.QueryContext(ctx, base, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []NodeState
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
		out = append(out, n)
	}
	return out, rows.Err()
}

// MergedReviewQueryBase returns the WITH clause for the merged src+dst review view (status from events). Use with " SELECT ... FROM merged" + where.
func MergedReviewQueryBase() string {
	return `WITH src_cur AS ` + cteSrcCurrentStatus + `, dst_cur AS ` + cteDstCurrentStatus + `,
merged AS (
SELECT
	COALESCE(s.path, d.path) AS path,
	COALESCE(s.parent_path, d.parent_path) AS parent_path,
	COALESCE(s.depth, d.depth, 0) AS depth,
	COALESCE(s.type, d.type, '') AS type,
	COALESCE(s.id, '') AS src_node_id,
	COALESCE(d.id, '') AS dst_node_id,
	COALESCE(se.traversal_status, '') AS src_traversal_status,
	COALESCE(de.traversal_status, '') AS dst_traversal_status,
	COALESCE(se.copy_status, '') AS copy_status,
	(COALESCE(se.traversal_status,'') IN ('excluded','exclusion_inherited') OR COALESCE(de.traversal_status,'') IN ('excluded','exclusion_inherited')) AS excluded,
	COALESCE(s.size, d.size, 0) AS size,
	COALESCE(s.parent_path, '') AS src_parent_path,
	COALESCE(d.parent_path, '') AS dst_parent_path,
	CASE
		WHEN COALESCE(s.path, d.path) = '' THEN ''
		WHEN strpos(reverse(COALESCE(s.path, d.path)), '/') = 0 THEN COALESCE(s.path, d.path)
		ELSE right(COALESCE(s.path, d.path), strpos(reverse(COALESCE(s.path, d.path)), '/') - 1)
	END AS name
FROM src_nodes s
LEFT JOIN src_cur se ON s.id = se.id
FULL OUTER JOIN dst_nodes d ON s.path = d.path
LEFT JOIN dst_cur de ON d.id = de.id
)`
}

// GetNodeByID returns the node by id from the given table. Status is derived from latest status event.
func GetNodeByID(d *DB, table, id string) (*NodeState, error) {
	queueType := table
	if queueType != "SRC" && queueType != "DST" {
		queueType = "SRC"
	}
	conn, err := d.GetDBForPulls(queueType)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	var n NodeState
	var size sql.NullInt64
	q := selectNodeColsWithStatus(table) + ` WHERE n.id = $1`
	err = conn.QueryRowContext(ctx, q, id).Scan(&n.ID, &n.ServiceID, &n.ParentID, &n.ParentServiceID, &n.Path, &n.ParentPath, &n.Type, &size, &n.MTime, &n.Depth, &n.TraversalStatus, &n.CopyStatus, &n.Excluded, &n.Errors)
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

// GetNodeByPath returns the node by path from the given table. Status is derived from latest status event.
func GetNodeByPath(d *DB, table, path string) (*NodeState, error) {
	queueType := table
	if queueType != "SRC" && queueType != "DST" {
		queueType = "SRC"
	}
	conn, err := d.GetDBForPulls(queueType)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	var n NodeState
	var size sql.NullInt64
	q := selectNodeColsWithStatus(table) + ` WHERE n.path = $1`
	err = conn.QueryRowContext(ctx, q, path).Scan(&n.ID, &n.ServiceID, &n.ParentID, &n.ParentServiceID, &n.Path, &n.ParentPath, &n.Type, &size, &n.MTime, &n.Depth, &n.TraversalStatus, &n.CopyStatus, &n.Excluded, &n.Errors)
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

// GetChildrenByParentPath returns up to limit children with the given parent_path. Status from latest events.
func GetChildrenByParentPath(d *DB, table, parentPath string, limit int) ([]*NodeState, error) {
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	q := selectNodeColsWithStatus(table) + ` WHERE n.parent_path = $1 ORDER BY n.id LIMIT $2`
	rows, err := conn.QueryContext(ctx, q, parentPath, limit)
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

// GetChildrenByParentID returns up to limit children with the given parent_id. Status from latest events.
func GetChildrenByParentID(d *DB, table, parentID string, limit int) ([]*NodeState, error) {
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	q := selectNodeColsWithStatus(table) + ` WHERE n.parent_id = $1 ORDER BY n.id LIMIT $2`
	rows, err := conn.QueryContext(ctx, q, parentID, limit)
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
// If statusFilter is non-empty, only rows with current traversal_status = statusFilter are returned (event-derived).
func ListNodesByDepthKeyset(d *DB, table string, depth int, afterID, statusFilter string, limit int) ([]FetchResult, error) {
	queueType := table
	if queueType != "SRC" && queueType != "DST" {
		queueType = "SRC"
	}
	conn, err := d.GetDBForPulls(queueType)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	_, e, _ := statusJoinExpr(table)
	base := selectNodeColsWithStatus(table) + ` WHERE n.depth = $1`
	args := []interface{}{depth}
	param := 2
	if statusFilter != "" {
		base += ` AND ` + e + `.traversal_status = $` + strconv.Itoa(param)
		args = append(args, statusFilter)
		param++
	}
	if afterID != "" {
		base += ` AND n.id > $` + strconv.Itoa(param)
		args = append(args, afterID)
		param++
	}
	base += ` ORDER BY n.id LIMIT $` + strconv.Itoa(param)
	args = append(args, limit)
	rows, err := conn.QueryContext(ctx, base, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FetchResult
	for rows.Next() {
		var node NodeState
		var size sql.NullInt64
		if err := rows.Scan(&node.ID, &node.ServiceID, &node.ParentID, &node.ParentServiceID, &node.Path, &node.ParentPath, &node.Type, &size, &node.MTime, &node.Depth, &node.TraversalStatus, &node.CopyStatus, &node.Excluded, &node.Errors); err != nil {
			return nil, err
		}
		if size.Valid {
			node.Size = size.Int64
		}
		node.Status = node.TraversalStatus
		node.Name = node.Path
		out = append(out, FetchResult{Key: node.ID, State: &node})
	}
	return out, rows.Err()
}

// ListNodesCopyKeyset returns src_nodes at depth for copy phase with current copy_status = 'pending' (event-derived), ordered by id.
func ListNodesCopyKeyset(d *DB, depth int, nodeType, afterID string, limit int) ([]FetchResult, error) {
	conn, err := d.GetDBForPulls("SRC")
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	_, e, _ := statusJoinExpr("SRC")
	base := selectNodeColsWithStatus("SRC") + ` WHERE n.depth = $1 AND ` + e + `.copy_status = 'pending'`
	args := []interface{}{depth}
	param := 2
	if nodeType != "" {
		base += ` AND n.type = $` + strconv.Itoa(param)
		args = append(args, nodeType)
		param++
	}
	if afterID != "" {
		base += ` AND n.id > $` + strconv.Itoa(param)
		args = append(args, afterID)
		param++
	}
	base += ` ORDER BY n.id LIMIT $` + strconv.Itoa(param)
	args = append(args, limit)
	rows, err := conn.QueryContext(ctx, base, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FetchResult
	for rows.Next() {
		var node NodeState
		var size sql.NullInt64
		if err := rows.Scan(&node.ID, &node.ServiceID, &node.ParentID, &node.ParentServiceID, &node.Path, &node.ParentPath, &node.Type, &size, &node.MTime, &node.Depth, &node.TraversalStatus, &node.CopyStatus, &node.Excluded, &node.Errors); err != nil {
			return nil, err
		}
		if size.Valid {
			node.Size = size.Int64
		}
		node.Status = node.TraversalStatus
		node.Name = node.Path
		out = append(out, FetchResult{Key: node.ID, State: &node})
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

// CountExcludedInSubtree returns the number of nodes in the subtree whose current traversal_status is excluded or exclusion_inherited.
func CountExcludedInSubtree(d *DB, table, rootPath string) (int, error) {
	conn, err := d.GetDB()
	if err != nil {
		return 0, err
	}
	t := tableName(table)
	nodeAlias, e, cte := statusJoinExpr(table)
	ctx := context.Background()
	base := `SELECT COUNT(*)::INT FROM ` + t + ` ` + nodeAlias + ` LEFT JOIN ` + cte + ` ` + e + ` ON ` + nodeAlias + `.id = ` + e + `.id WHERE (` + e + `.traversal_status = 'excluded' OR ` + e + `.traversal_status = 'exclusion_inherited')`
	var nCount int
	if rootPath == "/" {
		err = conn.QueryRowContext(ctx, base+` AND `+nodeAlias+`.path LIKE '/%'`).Scan(&nCount)
	} else {
		prefix := rootPath + "/%"
		err = conn.QueryRowContext(ctx, base+` AND (`+nodeAlias+`.path = $1 OR `+nodeAlias+`.path LIKE $2)`, rootPath, prefix).Scan(&nCount)
	}
	if err != nil {
		return 0, err
	}
	return nCount, nil
}

// CountExcluded returns the number of nodes whose current traversal_status is excluded or exclusion_inherited.
func CountExcluded(d *DB, table string) (int, error) {
	conn, err := d.GetDB()
	if err != nil {
		return 0, err
	}
	t := tableName(table)
	nodeAlias, e, cte := statusJoinExpr(table)
	ctx := context.Background()
	q := `SELECT COUNT(*)::INT FROM ` + t + ` ` + nodeAlias + ` LEFT JOIN ` + cte + ` ` + e + ` ON ` + nodeAlias + `.id = ` + e + `.id WHERE (` + e + `.traversal_status = 'excluded' OR ` + e + `.traversal_status = 'exclusion_inherited')`
	var nCount int
	err = conn.QueryRowContext(ctx, q).Scan(&nCount)
	if err != nil {
		return 0, err
	}
	return nCount, nil
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

// BatchGetNodeMeta returns meta (id, depth, type, traversal_status, copy_status) for the given ids. Status from latest events.
func BatchGetNodeMeta(d *DB, table string, ids []string) (map[string]NodeMeta, error) {
	if len(ids) == 0 {
		return make(map[string]NodeMeta), nil
	}
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	t := tableName(table)
	nodeAlias, e, cte := statusJoinExpr(table)
	ctx := context.Background()
	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i := range ids {
		placeholders[i] = "$" + strconv.Itoa(i+1)
		args[i] = ids[i]
	}
	q := `SELECT ` + nodeAlias + `.id, ` + nodeAlias + `.depth, ` + nodeAlias + `.type, COALESCE(` + e + `.traversal_status,'') AS traversal_status, COALESCE(` + e + `.copy_status,'') AS copy_status FROM ` + t + ` ` + nodeAlias + ` LEFT JOIN ` + cte + ` ` + e + ` ON ` + nodeAlias + `.id = ` + e + `.id WHERE ` + nodeAlias + `.id IN (` + strings.Join(placeholders, ",") + `)`
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

// ListDstBatchWithSrcChildren returns the next batch of DST nodes at depth (keyset afterID, limit) and their SRC children (join by parent_path = d.path). Status from events.
func ListDstBatchWithSrcChildren(d *DB, depth int, afterID string, limit int, traversalStatus string) ([]FetchResult, map[string][]*NodeState, error) {
	conn, err := d.GetDBForPulls("DST")
	if err != nil {
		return nil, nil, err
	}
	ctx := context.Background()
	cteWhere := "dn.depth = $1"
	args := []interface{}{depth}
	argNum := 2
	if afterID != "" {
		cteWhere += " AND dn.id > $" + strconv.Itoa(argNum)
		args = append(args, afterID)
		argNum++
	}
	if traversalStatus != "" {
		cteWhere += " AND de.traversal_status = $" + strconv.Itoa(argNum)
		args = append(args, traversalStatus)
		argNum++
	}
	args = append(args, limit)
	limitParam := "$" + strconv.Itoa(argNum)

	q := `WITH dst_current AS ` + cteDstCurrentStatus + `,
dst_batch AS (
  SELECT dn.id, dn.service_id, dn.parent_id, dn.parent_service_id, dn.path, dn.parent_path, dn.type, dn.size, dn.mtime, dn.depth,
    COALESCE(de.traversal_status,'') AS traversal_status, '' AS copy_status,
    (COALESCE(de.traversal_status,'') IN ('excluded','exclusion_inherited')) AS excluded, '' AS errors
  FROM ` + tableDstNodes + ` dn LEFT JOIN dst_current de ON dn.id = de.id WHERE ` + cteWhere + ` ORDER BY dn.id LIMIT ` + limitParam + `
),
src_current AS ` + cteSrcCurrentStatus + `
SELECT
  d.id AS d_id, d.service_id AS d_service_id, d.parent_id AS d_parent_id, d.parent_service_id AS d_parent_service_id, d.path AS d_path, d.parent_path AS d_parent_path, d.type AS d_type, d.size AS d_size, d.mtime AS d_mtime, d.depth AS d_depth, d.traversal_status AS d_traversal_status, d.copy_status AS d_copy_status, d.excluded AS d_excluded, d.errors AS d_errors,
  COALESCE(sn.id, '') AS s_id, COALESCE(sn.service_id, '') AS s_service_id, COALESCE(sn.parent_id, '') AS s_parent_id, COALESCE(sn.parent_service_id, '') AS s_parent_service_id, COALESCE(sn.path, '') AS s_path, COALESCE(sn.parent_path, '') AS s_parent_path, COALESCE(sn.type, '') AS s_type, sn.size AS s_size, COALESCE(sn.mtime, '') AS s_mtime, COALESCE(sn.depth, 0) AS s_depth, COALESCE(se.traversal_status, '') AS s_traversal_status, COALESCE(se.copy_status, '') AS s_copy_status, (COALESCE(se.traversal_status,'') IN ('excluded','exclusion_inherited')) AS s_excluded, '' AS s_errors
FROM dst_batch d
LEFT JOIN ` + tableSrcNodes + ` sn ON sn.parent_path = d.path
LEFT JOIN src_current se ON sn.id = se.id
ORDER BY d.id, sn.id`

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

// GetSrcChildrenGroupedByParentPath returns SRC nodes grouped by parent_path. Status from latest events.
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
	nodeAlias, e, cte := statusJoinExpr("SRC")
	sel := `SELECT ` + nodeAlias + `.parent_path, ` + nodeAlias + `.id, ` + nodeAlias + `.service_id, ` + nodeAlias + `.parent_id, ` + nodeAlias + `.parent_service_id, ` + nodeAlias + `.path, ` + nodeAlias + `.type, ` + nodeAlias + `.size, ` + nodeAlias + `.mtime, ` + nodeAlias + `.depth, COALESCE(` + e + `.traversal_status,'') AS traversal_status, COALESCE(` + e + `.copy_status,'') AS copy_status, (COALESCE(` + e + `.traversal_status,'') IN ('excluded','exclusion_inherited')) AS excluded, '' AS errors FROM ` + tableSrcNodes + ` ` + nodeAlias + ` LEFT JOIN ` + cte + ` ` + e + ` ON ` + nodeAlias + `.id = ` + e + `.id WHERE ` + nodeAlias + `.parent_path IN (`
	for i := 0; i < len(parentPaths); i += chunk {
		end := i + chunk
		if end > len(parentPaths) {
			end = len(parentPaths)
		}
		chunkPaths := parentPaths[i:end]
		q := sel
		for j := 0; j < len(chunkPaths); j++ {
			if j > 0 {
				q += ","
			}
			q += "$" + strconv.Itoa(j+1)
		}
		q += ") ORDER BY " + nodeAlias + ".parent_path, " + nodeAlias + ".id"
		args := make([]interface{}, len(chunkPaths))
		for j, p := range chunkPaths {
			args[j] = p
		}
		rows, err := conn.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var node NodeState
			var parentPath string
			var size sql.NullInt64
			if err := rows.Scan(&parentPath, &node.ID, &node.ServiceID, &node.ParentID, &node.ParentServiceID, &node.Path, &node.Type, &size, &node.MTime, &node.Depth, &node.TraversalStatus, &node.CopyStatus, &node.Excluded, &node.Errors); err != nil {
				rows.Close()
				return nil, err
			}
			if size.Valid {
				node.Size = size.Int64
			}
			node.ParentPath = parentPath
			node.Status = node.TraversalStatus
			if node.Path != "" {
				last := node.Path
				for i := len(last) - 1; i >= 0; i-- {
					if last[i] == '/' {
						if i+1 < len(last) {
							last = last[i+1:]
						}
						break
					}
				}
				node.Name = last
			} else {
				node.Name = node.Path
			}
			out[parentPath] = append(out[parentPath], &node)
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

// BatchGetNodesByID returns nodes by id for the given table in one query. Status from latest events.
func BatchGetNodesByID(d *DB, table string, ids []string) (map[string]*NodeState, error) {
	out := make(map[string]*NodeState)
	if len(ids) == 0 {
		return out, nil
	}
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i := range ids {
		placeholders[i] = "$" + strconv.Itoa(i+1)
		args[i] = ids[i]
	}
	q := selectNodeColsWithStatus(table) + ` WHERE n.id IN (` + strings.Join(placeholders, ",") + `)`
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
