// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package pull

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"context"
	"database/sql"
	"strconv"
	"strings"
)

// GetNodeByID returns the node by id from the given table. Status is derived from latest status event.
func GetNodeByID(d *db.DB, table, id string) (*db.NodeState, error) {
	queueType := table
	if queueType != "SRC" && queueType != "DST" {
		queueType = "SRC"
	}
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	var n db.NodeState
	var size sql.NullInt64
	q := db.SelectNodeColsWithStatus(table) + ` WHERE n.id = $1`
	err = conn.QueryRowContext(ctx, q, id).Scan(&n.ID, &n.ServiceID, &n.ParentID, &n.ParentServiceID, &n.Path, &n.ParentPath, &n.Type, &size, &n.MTime, &n.Depth, &n.TraversalStatus, &n.CopyStatus, &n.DeleteStatus, &n.Excluded, &n.Errors, &n.Name)
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
func GetNodeByPath(d *db.DB, table, path string) (*db.NodeState, error) {
	queueType := table
	if queueType != "SRC" && queueType != "DST" {
		queueType = "SRC"
	}
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	var n db.NodeState
	var size sql.NullInt64
	q := db.SelectNodeColsWithStatus(table) + ` WHERE n.path = $1`
	err = conn.QueryRowContext(ctx, q, db.NormalizeRootRelativePath(path)).Scan(&n.ID, &n.ServiceID, &n.ParentID, &n.ParentServiceID, &n.Path, &n.ParentPath, &n.Type, &size, &n.MTime, &n.Depth, &n.TraversalStatus, &n.CopyStatus, &n.DeleteStatus, &n.Excluded, &n.Errors, &n.Name)
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

// GetRootNode returns the root node (path = '/') from the given table.
func GetRootNode(d *db.DB, table string) (id string, state *db.NodeState, ok bool) {
	state, err := GetNodeByPath(d, table, "/")
	if err != nil || state == nil {
		return "", nil, false
	}
	return state.ID, state, true
}

// GetChildrenByParentPath returns up to limit children with the given parent_path. Status from latest events.
func GetChildrenByParentPath(d *db.DB, table, parentPath string, limit int) ([]*db.NodeState, error) {
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	q := db.SelectNodeColsWithStatus(table) + ` WHERE n.parent_path = $1 ORDER BY n.id LIMIT $2`
	rows, err := conn.QueryContext(ctx, q, db.NormalizeRootRelativePath(parentPath), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*db.NodeState
	for rows.Next() {
		var n db.NodeState
		var size sql.NullInt64
		if err := rows.Scan(&n.ID, &n.ServiceID, &n.ParentID, &n.ParentServiceID, &n.Path, &n.ParentPath, &n.Type, &size, &n.MTime, &n.Depth, &n.TraversalStatus, &n.CopyStatus, &n.DeleteStatus, &n.Excluded, &n.Errors, &n.Name); err != nil {
			return nil, err
		}
		if size.Valid {
			n.Size = size.Int64
		}
		n.Status = n.TraversalStatus
		out = append(out, &n)
	}
	return out, rows.Err()
}

// GetChildrenByParentID returns up to limit children with the given parent_id. Status from latest events.
func GetChildrenByParentID(d *db.DB, table, parentID string, limit int) ([]*db.NodeState, error) {
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	q := db.SelectNodeColsWithStatus(table) + ` WHERE n.parent_id = $1 ORDER BY n.id LIMIT $2`
	rows, err := conn.QueryContext(ctx, q, parentID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*db.NodeState
	for rows.Next() {
		var n db.NodeState
		var size sql.NullInt64
		if err := rows.Scan(&n.ID, &n.ServiceID, &n.ParentID, &n.ParentServiceID, &n.Path, &n.ParentPath, &n.Type, &size, &n.MTime, &n.Depth, &n.TraversalStatus, &n.CopyStatus, &n.DeleteStatus, &n.Excluded, &n.Errors, &n.Name); err != nil {
			return nil, err
		}
		if size.Valid {
			n.Size = size.Int64
		}
		n.Status = n.TraversalStatus
		out = append(out, &n)
	}
	return out, rows.Err()
}

// GetChildrenIDsByParentID returns child ids for the given parent_id (up to limit).
func GetChildrenIDsByParentID(d *db.DB, table, parentID string, limit int) ([]string, error) {
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

func listNodesByDepthKeysetNoStatus(ctx context.Context, conn *sql.DB, table string, depth int, afterID string, limit int) ([]db.FetchResult, error) {
	base := db.SelectNodeColsRaw(table) + ` WHERE n.depth = $1`
	args := []any{depth}
	param := 2
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
	return scanFetchResults(rows)
}

// queryDepthKeysetWindowWithStatus returns up to window rows at depth (keyset), with traversal/copy status aggregated only for ids in that window.
func queryDepthKeysetWindowWithStatus(ctx context.Context, conn *sql.DB, table string, isDST bool, depth int, afterID string, window int) ([]db.FetchResult, error) {
	t := db.TableName(table)
	var q string
	args := []any{depth}
	param := 2
	candWhere := `WHERE n.depth = $1`
	if afterID != "" {
		candWhere += ` AND n.id > $` + strconv.Itoa(param)
		args = append(args, afterID)
		param++
	}
	limitParam := `$` + strconv.Itoa(param)
	args = append(args, window)

	if isDST {
		q = `WITH cand AS (
	SELECT id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, name
	FROM ` + t + ` n
	` + candWhere + `
	ORDER BY n.id
	LIMIT ` + limitParam + `
),
trav AS (
	SELECT se.id, arg_max(se.traversal_status, se.event_time) AS traversal_status
	FROM dst_status_events se
	INNER JOIN cand c ON c.id = se.id
	GROUP BY se.id
)
SELECT c.id, c.service_id, c.parent_id, c.parent_service_id, c.path, c.parent_path, c.type, c.size, c.mtime, c.depth,
	COALESCE(trav.traversal_status,'') AS traversal_status,
	'' AS copy_status,
	'' AS delete_status,
	0 AS excluded,
	'' AS errors,
	COALESCE(c.name,'') AS name
FROM cand c
LEFT JOIN trav ON trav.id = c.id
ORDER BY c.id`
	} else {
		q = `WITH cand AS (
	SELECT id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, name
	FROM ` + t + ` n
	` + candWhere + `
	ORDER BY n.id
	LIMIT ` + limitParam + `
),
trav AS (
	SELECT se.id, arg_max(se.traversal_status, se.event_time) AS traversal_status
	FROM src_status_events se
	INNER JOIN cand c ON c.id = se.id
	GROUP BY se.id
),
cpy AS (
	SELECT se.id, arg_max(se.copy_status, se.event_time) AS copy_status
	FROM src_status_events se
	INNER JOIN cand c ON c.id = se.id
	WHERE COALESCE(se.copy_status, '') <> ''
	GROUP BY se.id
),
del AS (
	SELECT se.id, arg_max(se.delete_status, se.event_time) AS delete_status
	FROM src_status_events se
	INNER JOIN cand c ON c.id = se.id
	WHERE COALESCE(se.delete_status, '') <> ''
	GROUP BY se.id
)
SELECT c.id, c.service_id, c.parent_id, c.parent_service_id, c.path, c.parent_path, c.type, c.size, c.mtime, c.depth,
	COALESCE(trav.traversal_status,'') AS traversal_status,
	COALESCE(cpy.copy_status,'') AS copy_status,
	COALESCE(del.delete_status,'') AS delete_status,
	(COALESCE(cpy.copy_status,'') IN ('excluded_explicit','excluded_inherited')) AS excluded,
	'' AS errors,
	COALESCE(c.name,'') AS name
FROM cand c
LEFT JOIN trav ON trav.id = c.id
LEFT JOIN cpy ON cpy.id = c.id
LEFT JOIN del ON del.id = c.id
ORDER BY c.id`
	}
	rows, err := conn.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanFetchResults(rows)
}

func scanFetchResults(rows *sql.Rows) ([]db.FetchResult, error) {
	var out []db.FetchResult
	for rows.Next() {
		var node db.NodeState
		var size sql.NullInt64
		if err := rows.Scan(&node.ID, &node.ServiceID, &node.ParentID, &node.ParentServiceID, &node.Path, &node.ParentPath, &node.Type, &size, &node.MTime, &node.Depth, &node.TraversalStatus, &node.CopyStatus, &node.DeleteStatus, &node.Excluded, &node.Errors, &node.Name); err != nil {
			return nil, err
		}
		if size.Valid {
			node.Size = size.Int64
		}
		node.Status = node.TraversalStatus
		out = append(out, db.FetchResult{Key: node.ID, State: &node})
	}
	return out, rows.Err()
}

// ListNodesByDepthKeyset returns nodes at the given depth, ordered by id, after afterID, up to limit rows.
// If statusFilter is empty, returns rows without joining status events (traversal/copy columns empty).
// If statusFilter is non-empty, only rows whose current traversal_status matches are returned; status is derived from events aggregated only for ids in each keyset window (not whole tables).
func ListNodesByDepthKeyset(d *db.DB, table string, depth int, afterID, statusFilter string, limit int) ([]db.FetchResult, error) {
	queueType := table
	if queueType != "SRC" && queueType != "DST" {
		queueType = "SRC"
	}
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	if statusFilter == "" {
		return listNodesByDepthKeysetNoStatus(ctx, conn, table, depth, afterID, limit)
	}
	isDST := table == "DST"
	out := make([]db.FetchResult, 0, limit)
	cursor := afterID
	for len(out) < limit {
		window, err := queryDepthKeysetWindowWithStatus(ctx, conn, table, isDST, depth, cursor, db.PullKeysetWindowSize)
		if err != nil {
			return nil, err
		}
		if len(window) == 0 {
			break
		}
		for i := range window {
			st := window[i].State
			if st != nil && st.TraversalStatus == statusFilter {
				out = append(out, window[i])
				if len(out) == limit {
					return out, nil
				}
			}
		}
		cursor = window[len(window)-1].Key
		if len(window) < db.PullKeysetWindowSize {
			break
		}
	}
	return out, nil
}

func queryCopyKeysetWindow(ctx context.Context, conn *sql.DB, depth int, nodeType, afterID string, window int) ([]db.FetchResult, error) {
	candWhere := `WHERE n.depth = $1`
	args := []any{depth}
	param := 2
	if nodeType != "" {
		candWhere += ` AND n.type = $` + strconv.Itoa(param)
		args = append(args, nodeType)
		param++
	}
	if afterID != "" {
		candWhere += ` AND n.id > $` + strconv.Itoa(param)
		args = append(args, afterID)
		param++
	}
	limitParam := `$` + strconv.Itoa(param)
	args = append(args, window)

	q := `WITH cand AS (
	SELECT id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, name
	FROM ` + db.TableSrcNodes + ` n
	` + candWhere + `
	ORDER BY n.id
	LIMIT ` + limitParam + `
),
trav AS (
	SELECT se.id, arg_max(se.traversal_status, se.event_time) AS traversal_status
	FROM src_status_events se
	INNER JOIN cand c ON c.id = se.id
	GROUP BY se.id
),
cpy AS (
	SELECT se.id, arg_max(se.copy_status, se.event_time) AS copy_status
	FROM src_status_events se
	INNER JOIN cand c ON c.id = se.id
	WHERE COALESCE(se.copy_status, '') <> ''
	GROUP BY se.id
),
del AS (
	SELECT se.id, arg_max(se.delete_status, se.event_time) AS delete_status
	FROM src_status_events se
	INNER JOIN cand c ON c.id = se.id
	WHERE COALESCE(se.delete_status, '') <> ''
	GROUP BY se.id
),
idmap_self AS (
	SELECT src_internal_id, arg_max(dst_internal_id, event_time) AS dst_internal_id
	FROM ` + db.TableIDMap + `
	INNER JOIN cand c ON c.id = ` + db.TableIDMap + `.src_internal_id
	WHERE status = 'active'
	GROUP BY src_internal_id
),
idmap_parent AS (
	SELECT src_internal_id, arg_max(dst_internal_id, event_time) AS dst_internal_id
	FROM ` + db.TableIDMap + `
	INNER JOIN cand c ON c.parent_id = ` + db.TableIDMap + `.src_internal_id
	WHERE status = 'active'
	GROUP BY src_internal_id
)
SELECT c.id, c.service_id, c.parent_id, c.parent_service_id, c.path, c.parent_path, c.type, c.size, c.mtime, c.depth,
	COALESCE(trav.traversal_status,'') AS traversal_status,
	COALESCE(cpy.copy_status,'') AS copy_status,
	COALESCE(del.delete_status,'') AS delete_status,
	(COALESCE(cpy.copy_status,'') IN ('excluded_explicit','excluded_inherited')) AS excluded,
	'' AS errors,
	COALESCE(c.name,'') AS name,
	COALESCE(dst_parent.service_id,'') AS dst_parent_service_id,
	COALESCE(dst_parent.id,'') AS dst_parent_internal_id,
	COALESCE(cur.resolved_dst_name,'') AS resolved_dst_path,
	COALESCE(idmap_self.dst_internal_id,'') AS dst_mapped_id
FROM cand c
LEFT JOIN trav ON trav.id = c.id
LEFT JOIN cpy ON cpy.id = c.id
LEFT JOIN del ON del.id = c.id
LEFT JOIN idmap_parent ON idmap_parent.src_internal_id = c.parent_id
LEFT JOIN ` + db.TableDstNodes + ` dst_parent ON dst_parent.id = idmap_parent.dst_internal_id
LEFT JOIN ` + db.TableSrcCurrent + ` cur ON cur.id = c.id
LEFT JOIN idmap_self ON idmap_self.src_internal_id = c.id
ORDER BY c.id`
	rows, err := conn.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []db.FetchResult
	for rows.Next() {
		var node db.NodeState
		var size sql.NullInt64
		var dstParentServiceID, dstParentNodeID, resolvedDstPath, dstMappedID string
		if err := rows.Scan(&node.ID, &node.ServiceID, &node.ParentID, &node.ParentServiceID, &node.Path, &node.ParentPath, &node.Type, &size, &node.MTime, &node.Depth, &node.TraversalStatus, &node.CopyStatus, &node.DeleteStatus, &node.Excluded, &node.Errors, &node.Name, &dstParentServiceID, &dstParentNodeID, &resolvedDstPath, &dstMappedID); err != nil {
			return nil, err
		}
		if size.Valid {
			node.Size = size.Int64
		}
		node.Status = node.TraversalStatus
		out = append(out, db.FetchResult{
			Key:                node.ID,
			State:              &node,
			DstParentServiceID: dstParentServiceID,
			DstParentNodeID:    dstParentNodeID,
			ResolvedDstPath:    resolvedDstPath,
			DstMappedID:        dstMappedID,
		})
	}
	return out, rows.Err()
}

// ListNodesCopyKeyset returns src_nodes at depth with current copy_status = statusFilter (event-derived), ordered by id. Pass db.CopyStatusPending for copy phase, db.CopyStatusFailed for copy-retry.
func ListNodesCopyKeyset(d *db.DB, depth int, nodeType, afterID string, limit int, statusFilter string) ([]db.FetchResult, error) {
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	out := make([]db.FetchResult, 0, limit)
	cursor := afterID
	for len(out) < limit {
		window, err := queryCopyKeysetWindow(ctx, conn, depth, nodeType, cursor, db.PullKeysetWindowSize)
		if err != nil {
			return nil, err
		}
		if len(window) == 0 {
			break
		}
		for i := range window {
			st := window[i].State
			if st != nil && st.CopyStatus == statusFilter {
				out = append(out, window[i])
				if len(out) == limit {
					return out, nil
				}
			}
		}
		cursor = window[len(window)-1].Key
		if len(window) < db.PullKeysetWindowSize {
			break
		}
	}
	return out, nil
}

// queryDeleteKeysetWindow is like queryCopyKeysetWindow but without dst parent join.
func queryDeleteKeysetWindow(ctx context.Context, conn *sql.DB, depth int, nodeType, afterID string, window int) ([]db.FetchResult, error) {
	candWhere := `WHERE n.depth = $1`
	args := []any{depth}
	param := 2
	if nodeType != "" {
		candWhere += ` AND n.type = $` + strconv.Itoa(param)
		args = append(args, nodeType)
		param++
	}
	if afterID != "" {
		candWhere += ` AND n.id > $` + strconv.Itoa(param)
		args = append(args, afterID)
		param++
	}
	limitParam := `$` + strconv.Itoa(param)
	args = append(args, window)

	q := `WITH cand AS (
	SELECT id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, name
	FROM ` + db.TableSrcNodes + ` n
	` + candWhere + `
	ORDER BY n.id
	LIMIT ` + limitParam + `
),
trav AS (
	SELECT se.id, arg_max(se.traversal_status, se.event_time) AS traversal_status
	FROM src_status_events se
	INNER JOIN cand c ON c.id = se.id
	GROUP BY se.id
),
cpy AS (
	SELECT se.id, arg_max(se.copy_status, se.event_time) AS copy_status
	FROM src_status_events se
	INNER JOIN cand c ON c.id = se.id
	WHERE COALESCE(se.copy_status, '') <> ''
	GROUP BY se.id
),
del AS (
	SELECT se.id, arg_max(se.delete_status, se.event_time) AS delete_status
	FROM src_status_events se
	INNER JOIN cand c ON c.id = se.id
	WHERE COALESCE(se.delete_status, '') <> ''
	GROUP BY se.id
)
SELECT c.id, c.service_id, c.parent_id, c.parent_service_id, c.path, c.parent_path, c.type, c.size, c.mtime, c.depth,
	COALESCE(trav.traversal_status,'') AS traversal_status,
	COALESCE(cpy.copy_status,'') AS copy_status,
	COALESCE(del.delete_status,'') AS delete_status,
	(COALESCE(cpy.copy_status,'') IN ('excluded_explicit','excluded_inherited')) AS excluded,
	'' AS errors,
	COALESCE(c.name,'') AS name
FROM cand c
LEFT JOIN trav ON trav.id = c.id
LEFT JOIN cpy ON cpy.id = c.id
LEFT JOIN del ON del.id = c.id
ORDER BY c.id`
	rows, err := conn.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanFetchResults(rows)
}

// ListNodesDeleteKeyset returns src_nodes at depth with current delete_status = statusFilter (event-derived), ordered by id.
func ListNodesDeleteKeyset(d *db.DB, depth int, nodeType, afterID string, limit int, statusFilter string) ([]db.FetchResult, error) {
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	out := make([]db.FetchResult, 0, limit)
	cursor := afterID
	for len(out) < limit {
		window, err := queryDeleteKeysetWindow(ctx, conn, depth, nodeType, cursor, db.PullKeysetWindowSize)
		if err != nil {
			return nil, err
		}
		if len(window) == 0 {
			break
		}
		for i := range window {
			st := window[i].State
			if st != nil && !st.Excluded && db.CopyStatusIsComplete(st.CopyStatus) && st.DeleteStatus == statusFilter {
				out = append(out, window[i])
				if len(out) == limit {
					return out, nil
				}
			}
		}
		cursor = window[len(window)-1].Key
		if len(window) < db.PullKeysetWindowSize {
			break
		}
	}
	return out, nil
}

// FolderDeleteBlockedIDs returns folder node IDs (subset of parentIDs) that have a direct non-excluded child whose delete_status is not 'deleted'.
func FolderDeleteBlockedIDs(d *db.DB, parentIDs []string) (map[string]bool, error) {
	blocked := make(map[string]bool)
	if len(parentIDs) == 0 {
		return blocked, nil
	}
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	const chunk = 500
	for start := 0; start < len(parentIDs); start += chunk {
		end := start + chunk
		if end > len(parentIDs) {
			end = len(parentIDs)
		}
		part := parentIDs[start:end]
		ph := make([]string, len(part))
		args := make([]any, len(part))
		for i := range part {
			ph[i] = "$" + strconv.Itoa(i+1)
			args[i] = part[i]
		}
		q := `SELECT DISTINCT p.id
FROM ` + db.TableSrcNodes + ` p
INNER JOIN ` + db.TableSrcNodes + ` ch ON ch.parent_id = p.id
LEFT JOIN ` + db.CTESrcCurrentStatus + ` ce ON ch.id = ce.id
WHERE p.id IN (` + strings.Join(ph, ",") + `)
  AND p.type = 'folder'
  AND COALESCE(ce.copy_status,'') NOT IN ('excluded_explicit','excluded_inherited')
  AND COALESCE(ce.delete_status,'') <> 'deleted'`
		rows, err := conn.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			blocked[id] = true
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	return blocked, nil
}

// SubtreeStats holds aggregate counts for a subtree (path = rootPath OR path LIKE rootPath || '/%').
type SubtreeStats struct {
	TotalNodes   int
	TotalFolders int
	TotalFiles   int
	MaxDepth     int
}

// CountSubtree returns aggregate counts for the subtree at rootPath (path = rootPath OR path LIKE rootPath || '/%'; for rootPath "/" uses path LIKE '/%'). Single SQL query, no DFS.
func CountSubtree(d *db.DB, table, rootPath string) (SubtreeStats, error) {
	conn, err := d.GetDB()
	if err != nil {
		return SubtreeStats{}, err
	}
	t := db.TableName(table)
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

// CountExcludedInSubtree returns the number of SRC nodes in the subtree with copy_status in (excluded_explicit, excluded_inherited). Exclusion is SRC-only; returns 0 for DST.
func CountExcludedInSubtree(d *db.DB, table, rootPath string) (int, error) {
	if table == "DST" {
		return 0, nil
	}
	conn, err := d.GetDB()
	if err != nil {
		return 0, err
	}
	nodeAlias, e, cte := db.StatusJoinExpr("SRC")
	ctx := context.Background()
	base := `SELECT COUNT(*)::INT FROM ` + db.TableSrcNodes + ` ` + nodeAlias + ` LEFT JOIN ` + cte + ` ` + e + ` ON ` + nodeAlias + `.id = ` + e + `.id WHERE (COALESCE(` + e + `.copy_status,'') IN ('excluded_explicit','excluded_inherited'))`
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

// CountExcluded returns the number of SRC nodes with copy_status in (excluded_explicit, excluded_inherited). Exclusion is SRC-only; returns 0 for DST.
func CountExcluded(d *db.DB, table string) (int, error) {
	if table == "DST" {
		return 0, nil
	}
	conn, err := d.GetDB()
	if err != nil {
		return 0, err
	}
	nodeAlias, e, cte := db.StatusJoinExpr("SRC")
	ctx := context.Background()
	q := `SELECT COUNT(*)::INT FROM ` + db.TableSrcNodes + ` ` + nodeAlias + ` LEFT JOIN ` + cte + ` ` + e + ` ON ` + nodeAlias + `.id = ` + e + `.id WHERE (COALESCE(` + e + `.copy_status,'') IN ('excluded_explicit','excluded_inherited'))`
	var nCount int
	err = conn.QueryRowContext(ctx, q).Scan(&nCount)
	if err != nil {
		return 0, err
	}
	return nCount, nil
}

// CountNodes returns the total number of nodes in the given table (src_nodes or dst_nodes). Live table count, not from stats. Uses pull conn so we see appender-written data.
func CountNodes(d *db.DB, table string) (int, error) {
	queueType := table
	if queueType != "SRC" && queueType != "DST" {
		queueType = "SRC"
	}
	conn, err := d.GetDB()
	if err != nil {
		return 0, err
	}
	t := db.TableName(table)
	ctx := context.Background()
	var n int
	err = conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+t).Scan(&n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// GetAllLevels returns distinct depth values for the table, sorted.
func GetAllLevels(d *db.DB, table string) ([]int, error) {
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	t := db.TableName(table)
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
func BatchGetNodeMeta(d *db.DB, table string, ids []string) (map[string]db.NodeMeta, error) {
	if len(ids) == 0 {
		return make(map[string]db.NodeMeta), nil
	}
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	t := db.TableName(table)
	nodeAlias, e, cte := db.StatusJoinExpr(table)
	ctx := context.Background()
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
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
	out := make(map[string]db.NodeMeta)
	for rows.Next() {
		var m db.NodeMeta
		if err := rows.Scan(&m.ID, &m.Depth, &m.Type, &m.TraversalStatus, &m.CopyStatus); err != nil {
			return nil, err
		}
		out[m.ID] = m
	}
	return out, rows.Err()
}

// dstKeysetWindowRow is one DST node row from a keyset window.
type dstKeysetWindowRow struct {
	id, serviceID, parentID, parentServiceID, path, parentPath, typ, mtime, name string
	size                                                                         sql.NullInt64
	depth                                                                        int
}

// queryDstNodesWindowRaw returns a keyset window of DST folders at depth.
// Files are excluded: DST traversal only leases folders (files are discovered as successful
// during parent compare). Scanning successful files at the same depth made gathers slow and
// widened concurrent-pull race windows.
func queryDstNodesWindowRaw(ctx context.Context, conn *sql.DB, depth int, afterID string, window int) ([]dstKeysetWindowRow, error) {
	candWhere := `WHERE n.depth = $1 AND n.type = $2`
	args := []any{depth, db.NodeTypeFolder}
	param := 3
	if afterID != "" {
		candWhere += ` AND n.id > $` + strconv.Itoa(param)
		args = append(args, afterID)
		param++
	}
	limitParam := `$` + strconv.Itoa(param)
	args = append(args, window)
	q := `SELECT n.id, n.service_id, n.parent_id, n.parent_service_id, n.path, n.parent_path, n.type, n.size, n.mtime, n.depth, COALESCE(n.name,'') AS name
FROM ` + db.TableDstNodes + ` n
` + candWhere + `
ORDER BY n.id
LIMIT ` + limitParam
	rows, err := conn.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dstKeysetWindowRow
	for rows.Next() {
		var r dstKeysetWindowRow
		if err := rows.Scan(&r.id, &r.serviceID, &r.parentID, &r.parentServiceID, &r.path, &r.parentPath, &r.typ, &r.size, &r.mtime, &r.depth, &r.name); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func latestDstTraversalByIDs(ctx context.Context, conn *sql.DB, ids []string) (map[string]string, error) {
	out := make(map[string]string)
	if len(ids) == 0 {
		return out, nil
	}
	const chunk = 8000
	for start := 0; start < len(ids); start += chunk {
		end := start + chunk
		if end > len(ids) {
			end = len(ids)
		}
		part := ids[start:end]
		ph := make([]string, len(part))
		args := make([]any, len(part))
		for i := range part {
			ph[i] = "$" + strconv.Itoa(i+1)
			args[i] = part[i]
		}
		q := `SELECT id, arg_max(traversal_status, event_time) AS traversal_status
FROM dst_status_events
WHERE id IN (` + strings.Join(ph, ",") + `)
GROUP BY id`
		rows, err := conn.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id, st string
			if err := rows.Scan(&id, &st); err != nil {
				rows.Close()
				return nil, err
			}
			out[id] = st
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func batchReverseSrcIDFromDstIDs(ctx context.Context, conn *sql.DB, dstIDs []string) (map[string]string, error) {
	out := make(map[string]string)
	if len(dstIDs) == 0 {
		return out, nil
	}
	const chunk = 500
	for start := 0; start < len(dstIDs); start += chunk {
		end := start + chunk
		if end > len(dstIDs) {
			end = len(dstIDs)
		}
		part := dstIDs[start:end]
		ph := make([]string, len(part))
		args := make([]any, len(part))
		for i := range part {
			ph[i] = "$" + strconv.Itoa(i+1)
			args[i] = part[i]
		}
		q := `SELECT dst_internal_id, arg_max(src_internal_id, event_time) AS src_internal_id
FROM ` + db.TableIDMap + `
WHERE dst_internal_id IN (` + strings.Join(ph, ",") + `) AND status = 'active'
GROUP BY dst_internal_id`
		rows, err := conn.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var dstID, srcID string
			if err := rows.Scan(&dstID, &srcID); err != nil {
				rows.Close()
				return nil, err
			}
			out[dstID] = srcID
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// querySrcChildrenByParentIDs loads SRC node rows for the given parent IDs (identity columns only).
// Status fields are filled by attachSrcStatusByIDs — keeps the hot path free of event-table joins.
func querySrcChildrenByParentIDs(ctx context.Context, conn *sql.DB, parentIDs []string) (map[string][]*db.NodeState, error) {
	out := make(map[string][]*db.NodeState)
	if len(parentIDs) == 0 {
		return out, nil
	}
	const chunk = 500
	for start := 0; start < len(parentIDs); start += chunk {
		end := start + chunk
		if end > len(parentIDs) {
			end = len(parentIDs)
		}
		part := parentIDs[start:end]
		ph := make([]string, len(part))
		args := make([]any, len(part))
		for i := range part {
			ph[i] = "$" + strconv.Itoa(i+1)
			args[i] = part[i]
		}
		q := `SELECT sn.id, sn.service_id, sn.parent_id, sn.parent_service_id, sn.path, sn.parent_path, sn.type, sn.size, sn.mtime, sn.depth,
	COALESCE(sn.name,'') AS name, COALESCE(sn.gpl_state,'') AS gpl_state
FROM ` + db.TableSrcNodes + ` sn
WHERE sn.parent_id IN (` + strings.Join(ph, ",") + `)
ORDER BY sn.parent_id, sn.id`
		rows, err := conn.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var node db.NodeState
			var size sql.NullInt64
			if err := rows.Scan(&node.ID, &node.ServiceID, &node.ParentID, &node.ParentServiceID, &node.Path, &node.ParentPath, &node.Type, &size, &node.MTime, &node.Depth, &node.Name, &node.GPLState); err != nil {
				rows.Close()
				return nil, err
			}
			if size.Valid {
				node.Size = size.Int64
			}
			out[node.ParentID] = append(out[node.ParentID], &node)
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// attachSrcStatusByIDs fills traversal/copy/delete status on nodes via narrow event IN queries.
func attachSrcStatusByIDs(ctx context.Context, conn *sql.DB, nodes []*db.NodeState) error {
	if len(nodes) == 0 {
		return nil
	}
	byID := make(map[string]*db.NodeState, len(nodes))
	ids := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if n == nil || n.ID == "" {
			continue
		}
		if _, ok := byID[n.ID]; ok {
			continue
		}
		byID[n.ID] = n
		ids = append(ids, n.ID)
	}
	trav, err := latestSrcEventFieldByIDs(ctx, conn, ids, "traversal_status", false)
	if err != nil {
		return err
	}
	cpy, err := latestSrcEventFieldByIDs(ctx, conn, ids, "copy_status", true)
	if err != nil {
		return err
	}
	del, err := latestSrcEventFieldByIDs(ctx, conn, ids, "delete_status", true)
	if err != nil {
		return err
	}
	for id, n := range byID {
		n.TraversalStatus = trav[id]
		n.CopyStatus = cpy[id]
		n.DeleteStatus = del[id]
		n.Excluded = n.CopyStatus == "excluded_explicit" || n.CopyStatus == "excluded_inherited"
		n.Status = n.TraversalStatus
	}
	return nil
}

func latestSrcEventFieldByIDs(ctx context.Context, conn *sql.DB, ids []string, field string, skipEmpty bool) (map[string]string, error) {
	out := make(map[string]string)
	if len(ids) == 0 {
		return out, nil
	}
	const chunk = 8000
	for start := 0; start < len(ids); start += chunk {
		end := start + chunk
		if end > len(ids) {
			end = len(ids)
		}
		part := ids[start:end]
		ph := make([]string, len(part))
		args := make([]any, len(part))
		for i := range part {
			ph[i] = "$" + strconv.Itoa(i+1)
			args[i] = part[i]
		}
		whereEmpty := ""
		if skipEmpty {
			whereEmpty = ` AND COALESCE(` + field + `, '') <> ''`
		}
		q := `SELECT id, arg_max(` + field + `, event_time) AS v
FROM src_status_events
WHERE id IN (` + strings.Join(ph, ",") + `)` + whereEmpty + `
GROUP BY id`
		rows, err := conn.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id, v string
			if err := rows.Scan(&id, &v); err != nil {
				rows.Close()
				return nil, err
			}
			out[id] = v
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ListDstBatchWithSrcChildren returns the next batch of DST folders at depth (keyset afterID, limit)
// and their SRC children (via reverse id_map → parent_id).
//
// Gather loop: small folder keyset subsections → status filter → accumulate pending; id_map and SRC
// children run only for accepted IDs (late join). lastScannedID is the furthest DST folder id examined
// (or the last accepted pending when the batch fills mid-window) for queue cursor advancement.
func ListDstBatchWithSrcChildren(d *db.DB, depth int, afterID string, limit int, traversalStatus string) ([]db.FetchResult, map[string][]*db.NodeState, string, error) {
	conn, err := d.GetDB()
	if err != nil {
		return nil, nil, "", err
	}
	ctx := context.Background()
	scanWindow := db.DstPullScanWindow(limit)

	dstBatch := make([]db.FetchResult, 0, limit)
	pendingIDs := make([]string, 0, limit)
	cursor := afterID
	lastScannedID := afterID

dstGather:
	for len(dstBatch) < limit {
		window, err := queryDstNodesWindowRaw(ctx, conn, depth, cursor, scanWindow)
		if err != nil {
			return nil, nil, lastScannedID, err
		}
		if len(window) == 0 {
			break
		}
		lastScannedID = window[len(window)-1].id
		ids := make([]string, len(window))
		for i := range window {
			ids[i] = window[i].id
		}
		travByID, err := latestDstTraversalByIDs(ctx, conn, ids)
		if err != nil {
			return nil, nil, lastScannedID, err
		}
		for i := range window {
			w := window[i]
			st := travByID[w.id]
			if traversalStatus != "" && st != traversalStatus {
				continue
			}
			ns := &db.NodeState{
				ID: w.id, ServiceID: w.serviceID, ParentID: w.parentID, ParentServiceID: w.parentServiceID,
				Path: w.path, ParentPath: w.parentPath, Name: w.name, Type: w.typ, MTime: w.mtime, Depth: w.depth,
				TraversalStatus: st, CopyStatus: "", Excluded: false, Errors: "",
			}
			if w.size.Valid {
				ns.Size = w.size.Int64
			}
			ns.Status = ns.TraversalStatus
			dstBatch = append(dstBatch, db.FetchResult{Key: w.id, State: ns})
			pendingIDs = append(pendingIDs, w.id)
			if len(dstBatch) == limit {
				// Do not skip later pending rows still in this window.
				lastScannedID = w.id
				break dstGather
			}
		}
		cursor = lastScannedID
		if len(window) < scanWindow {
			break
		}
	}

	childrenByDstID := make(map[string][]*db.NodeState)
	if len(dstBatch) == 0 {
		return dstBatch, childrenByDstID, lastScannedID, nil
	}

	srcParentByDstID := make(map[string]string, len(pendingIDs))
	if depth == 0 {
		for _, id := range pendingIDs {
			srcParentByDstID[id] = db.MintNodeID("SRC", "", db.NodeTypeFolder, "/")
		}
	} else {
		reverseMap, err := batchReverseSrcIDFromDstIDs(ctx, conn, pendingIDs)
		if err != nil {
			return nil, nil, lastScannedID, err
		}
		for _, id := range pendingIDs {
			if srcID := reverseMap[id]; srcID != "" {
				srcParentByDstID[id] = srcID
			}
		}
	}

	parentIDSet := make(map[string]struct{})
	parentIDs := make([]string, 0, len(srcParentByDstID))
	for _, srcParentID := range srcParentByDstID {
		if srcParentID == "" {
			continue
		}
		if _, ok := parentIDSet[srcParentID]; ok {
			continue
		}
		parentIDSet[srcParentID] = struct{}{}
		parentIDs = append(parentIDs, srcParentID)
	}
	byParentID, err := querySrcChildrenByParentIDs(ctx, conn, parentIDs)
	if err != nil {
		return nil, nil, lastScannedID, err
	}
	var allChildren []*db.NodeState
	for _, fr := range dstBatch {
		srcParentID := srcParentByDstID[fr.Key]
		if srcParentID == "" {
			continue
		}
		if ch := byParentID[srcParentID]; len(ch) > 0 {
			childrenByDstID[fr.Key] = ch
			allChildren = append(allChildren, ch...)
		}
	}
	if err := attachSrcStatusByIDs(ctx, conn, allChildren); err != nil {
		return nil, nil, lastScannedID, err
	}
	return dstBatch, childrenByDstID, lastScannedID, nil
}

// GetSrcChildrenGroupedByParentPath returns SRC nodes grouped by parent_path. Status from latest events.
func GetSrcChildrenGroupedByParentPath(d *db.DB, parentPaths []string) (map[string][]*db.NodeState, error) {
	out := make(map[string][]*db.NodeState)
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
	normalizedPaths := make([]string, len(parentPaths))
	for i, p := range parentPaths {
		normalizedPaths[i] = db.NormalizeRootRelativePath(p)
	}
	nodeAlias, e, cte := db.StatusJoinExpr("SRC")
	sel := `SELECT ` + nodeAlias + `.parent_path, ` + nodeAlias + `.id, ` + nodeAlias + `.service_id, ` + nodeAlias + `.parent_id, ` + nodeAlias + `.parent_service_id, ` + nodeAlias + `.path, ` + nodeAlias + `.type, ` + nodeAlias + `.size, ` + nodeAlias + `.mtime, ` + nodeAlias + `.depth, COALESCE(` + e + `.traversal_status,'') AS traversal_status, COALESCE(` + e + `.copy_status,'') AS copy_status, COALESCE(` + e + `.delete_status,'') AS delete_status, (COALESCE(` + e + `.copy_status,'') IN ('excluded_explicit','excluded_inherited')) AS excluded, '' AS errors, COALESCE(` + nodeAlias + `.name,'') AS name FROM ` + db.TableSrcNodes + ` ` + nodeAlias + ` LEFT JOIN ` + cte + ` ` + e + ` ON ` + nodeAlias + `.id = ` + e + `.id WHERE ` + nodeAlias + `.parent_path IN (`
	for i := 0; i < len(normalizedPaths); i += chunk {
		end := i + chunk
		if end > len(normalizedPaths) {
			end = len(normalizedPaths)
		}
		chunkPaths := normalizedPaths[i:end]
		q := sel
		for j := 0; j < len(chunkPaths); j++ {
			if j > 0 {
				q += ","
			}
			q += "$" + strconv.Itoa(j+1)
		}
		q += ") ORDER BY " + nodeAlias + ".parent_path, " + nodeAlias + ".id"
		args := make([]any, len(chunkPaths))
		for j, p := range chunkPaths {
			args[j] = p
		}
		rows, err := conn.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var node db.NodeState
			var parentPath string
			var size sql.NullInt64
			if err := rows.Scan(&parentPath, &node.ID, &node.ServiceID, &node.ParentID, &node.ParentServiceID, &node.Path, &node.Type, &size, &node.MTime, &node.Depth, &node.TraversalStatus, &node.CopyStatus, &node.DeleteStatus, &node.Excluded, &node.Errors, &node.Name); err != nil {
				rows.Close()
				return nil, err
			}
			if size.Valid {
				node.Size = size.Int64
			}
			node.ParentPath = parentPath
			node.Status = node.TraversalStatus
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

// GetDstIDFromSrcID returns the DST node id mapped to the given SRC node id via id_map (path fallback when unmapped).
func GetDstIDFromSrcID(d *db.DB, srcParentID string) (string, error) {
	conn, err := d.GetDB()
	if err != nil {
		return "", err
	}
	ctx := context.Background()
	var dstID sql.NullString
	err = conn.QueryRowContext(ctx,
		`SELECT arg_max(dst_internal_id, event_time) FROM `+db.TableIDMap+` WHERE src_internal_id = $1 AND status = 'active'`,
		srcParentID,
	).Scan(&dstID)
	if err != nil && err != sql.ErrNoRows {
		return "", err
	}
	if dstID.Valid && dstID.String != "" {
		return dstID.String, nil
	}
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

// BatchGetDstIDsFromSrcIDs returns map[srcID]dstID via id_map (path fallback for unmapped ids).
func BatchGetDstIDsFromSrcIDs(d *db.DB, srcIDs []string) (map[string]string, error) {
	out := make(map[string]string)
	if len(srcIDs) == 0 {
		return out, nil
	}
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	placeholders := make([]string, len(srcIDs))
	args := make([]any, len(srcIDs))
	for i := range srcIDs {
		placeholders[i] = "$" + strconv.Itoa(i+1)
		args[i] = srcIDs[i]
	}
	q := `SELECT src_internal_id, arg_max(dst_internal_id, event_time) AS dst_internal_id
FROM ` + db.TableIDMap + `
WHERE src_internal_id IN (` + strings.Join(placeholders, ",") + `) AND status = 'active'
GROUP BY src_internal_id`
	rows, err := conn.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	mapped := make(map[string]string)
	for rows.Next() {
		var srcID, dstID string
		if err := rows.Scan(&srcID, &dstID); err != nil {
			rows.Close()
			return nil, err
		}
		if dstID != "" {
			mapped[srcID] = dstID
			out[srcID] = dstID
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var unmapped []string
	for _, srcID := range srcIDs {
		if _, ok := mapped[srcID]; !ok {
			unmapped = append(unmapped, srcID)
		}
	}
	if len(unmapped) == 0 {
		return out, nil
	}
	placeholders = make([]string, len(unmapped))
	args = make([]any, len(unmapped))
	for i := range unmapped {
		placeholders[i] = "$" + strconv.Itoa(i+1)
		args[i] = unmapped[i]
	}
	qSrc := "SELECT id, path FROM " + db.TableSrcNodes + " WHERE id IN (" + strings.Join(placeholders, ",") + ")"
	rows, err = conn.QueryContext(ctx, qSrc, args...)
	if err != nil {
		return nil, err
	}
	srcIDToPath := make(map[string]string)
	paths := make([]string, 0)
	pathSet := make(map[string]struct{})
	for rows.Next() {
		var id, path string
		if err := rows.Scan(&id, &path); err != nil {
			rows.Close()
			return nil, err
		}
		srcIDToPath[id] = path
		if path != "" {
			if _, ok := pathSet[path]; !ok {
				pathSet[path] = struct{}{}
				paths = append(paths, path)
			}
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return out, nil
	}
	placeholders = make([]string, len(paths))
	args = make([]any, len(paths))
	for i := range paths {
		placeholders[i] = "$" + strconv.Itoa(i+1)
		args[i] = paths[i]
	}
	qDst := "SELECT id, path FROM " + db.TableDstNodes + " WHERE path IN (" + strings.Join(placeholders, ",") + ")"
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
func BatchGetNodesByID(d *db.DB, table string, ids []string) (map[string]*db.NodeState, error) {
	out := make(map[string]*db.NodeState)
	if len(ids) == 0 {
		return out, nil
	}
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i := range ids {
		placeholders[i] = "$" + strconv.Itoa(i+1)
		args[i] = ids[i]
	}
	q := db.SelectNodeColsWithStatus(table) + ` WHERE n.id IN (` + strings.Join(placeholders, ",") + `)`
	rows, err := conn.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var size sql.NullInt64
	for rows.Next() {
		var n db.NodeState
		if err := rows.Scan(&n.ID, &n.ServiceID, &n.ParentID, &n.ParentServiceID, &n.Path, &n.ParentPath, &n.Type, &size, &n.MTime, &n.Depth, &n.TraversalStatus, &n.CopyStatus, &n.DeleteStatus, &n.Excluded, &n.Errors, &n.Name); err != nil {
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
func BatchGetChildrenIDsByParentIDs(d *db.DB, table string, parentIDs []string) (map[string][]string, error) {
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
	t := db.TableName(table)
	ctx := context.Background()
	q := "SELECT parent_id, id FROM " + t + " WHERE parent_id IN ("
	for i := 0; i < len(parentIDs); i++ {
		if i > 0 {
			q += ","
		}
		q += "$" + strconv.Itoa(i+1)
	}
	q += ")"
	args := make([]any, len(parentIDs))
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

// ListSrcNodesByCopyStatus returns SRC nodes whose current copy_status matches, paginated.
// Asking for db.CopyStatusSuccessful also returns already_existed (copy-complete for cleanup).
func ListSrcNodesByCopyStatus(d *db.DB, copyStatus string, limit, offset int) ([]db.NodeState, error) {
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 1000
	}
	if limit > 5000 {
		limit = 5000
	}
	if offset < 0 {
		offset = 0
	}
	ctx := context.Background()
	base := db.SelectNodeColsWithStatus("SRC")
	var rows *sql.Rows
	if copyStatus == db.CopyStatusSuccessful {
		q := base + ` WHERE COALESCE(e.copy_status,'') IN ` + db.SQLCopyStatusCompleteIN + ` ORDER BY n.path LIMIT $1 OFFSET $2`
		rows, err = conn.QueryContext(ctx, q, limit, offset)
	} else {
		q := base + ` WHERE COALESCE(e.copy_status,'') = $1 ORDER BY n.path LIMIT $2 OFFSET $3`
		rows, err = conn.QueryContext(ctx, q, copyStatus, limit, offset)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []db.NodeState
	var size sql.NullInt64
	for rows.Next() {
		var n db.NodeState
		if err := rows.Scan(&n.ID, &n.ServiceID, &n.ParentID, &n.ParentServiceID, &n.Path, &n.ParentPath, &n.Type, &size, &n.MTime, &n.Depth, &n.TraversalStatus, &n.CopyStatus, &n.DeleteStatus, &n.Excluded, &n.Errors, &n.Name); err != nil {
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
