// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package pull

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"context"
	"database/sql"
	"strconv"
)

// ListNodesGPLKeyset returns nodes at depth whose current gpl_status matches statusFilter
// (typically pending). Includes gpl_state, parent gpl_state, and accepted basename.
func ListNodesGPLKeyset(d *db.DB, table string, depth int, afterID, statusFilter string, limit int) ([]db.FetchResult, error) {
	if statusFilter == "" {
		statusFilter = db.GPLStatusPending
	}
	queueType := table
	if queueType != "SRC" && queueType != "DST" {
		queueType = "SRC"
	}
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	isDST := queueType == "DST"
	out := make([]db.FetchResult, 0, limit)
	cursor := afterID
	for len(out) < limit {
		window, err := queryGPLKeysetWindow(ctx, conn, isDST, depth, cursor, db.PullKeysetWindowSize)
		if err != nil {
			return nil, err
		}
		if len(window) == 0 {
			break
		}
		for i := range window {
			st := window[i].State
			if st != nil && st.GPLStatus == statusFilter {
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

func queryGPLKeysetWindow(ctx context.Context, conn *sql.DB, isDST bool, depth int, afterID string, window int) ([]db.FetchResult, error) {
	candWhere := `WHERE n.depth = $1`
	args := []any{depth}
	param := 2
	if afterID != "" {
		candWhere += ` AND n.id > $` + strconv.Itoa(param)
		args = append(args, afterID)
		param++
	}
	limitParam := `$` + strconv.Itoa(param)
	args = append(args, window)

	var q string
	if isDST {
		q = `WITH cand AS (
	SELECT id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, name
	FROM ` + db.TableDstNodes + ` n
	` + candWhere + `
	ORDER BY n.id
	LIMIT ` + limitParam + `
),
trav AS (
	SELECT se.id, arg_max(se.traversal_status, se.event_time) AS traversal_status
	FROM dst_status_events se
	INNER JOIN cand c ON c.id = se.id
	GROUP BY se.id
),
gpl AS (
	SELECT se.id, arg_max(se.gpl_status, se.event_time) AS gpl_status
	FROM dst_status_events se
	INNER JOIN cand c ON c.id = se.id
	WHERE COALESCE(se.gpl_status, '') <> ''
	GROUP BY se.id
)
SELECT c.id, c.service_id, c.parent_id, c.parent_service_id, c.path, c.parent_path, c.type, c.size, c.mtime, c.depth,
	COALESCE(trav.traversal_status,'') AS traversal_status,
	'' AS copy_status,
	'' AS delete_status,
	COALESCE(gpl.gpl_status,'') AS gpl_status,
	0 AS excluded,
	'' AS errors,
	'' AS gpl_state,
	'' AS parent_gpl_state,
	'' AS resolved_dst_path,
	COALESCE(c.name,'') AS name
FROM cand c
LEFT JOIN trav ON trav.id = c.id
LEFT JOIN gpl ON gpl.id = c.id
ORDER BY c.id`
	} else {
		q = `WITH cand AS (
	SELECT id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, COALESCE(gpl_state,'') AS gpl_state, name
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
gpl AS (
	SELECT se.id, arg_max(se.gpl_status, se.event_time) AS gpl_status
	FROM src_status_events se
	INNER JOIN cand c ON c.id = se.id
	WHERE COALESCE(se.gpl_status, '') <> ''
	GROUP BY se.id
)
SELECT c.id, c.service_id, c.parent_id, c.parent_service_id, c.path, c.parent_path, c.type, c.size, c.mtime, c.depth,
	COALESCE(trav.traversal_status,'') AS traversal_status,
	COALESCE(cpy.copy_status,'') AS copy_status,
	COALESCE(del.delete_status,'') AS delete_status,
	COALESCE(gpl.gpl_status,'') AS gpl_status,
	(COALESCE(cpy.copy_status,'') IN ('excluded_explicit','excluded_inherited')) AS excluded,
	'' AS errors,
	c.gpl_state,
	COALESCE(pn.gpl_state,'') AS parent_gpl_state,
	COALESCE(cur.resolved_dst_name,'') AS resolved_dst_path,
	COALESCE(c.name,'') AS name
FROM cand c
LEFT JOIN trav ON trav.id = c.id
LEFT JOIN cpy ON cpy.id = c.id
LEFT JOIN del ON del.id = c.id
LEFT JOIN gpl ON gpl.id = c.id
LEFT JOIN ` + db.TableSrcCurrent + ` cur ON cur.id = c.id
LEFT JOIN ` + db.TableSrcNodes + ` pn ON pn.id = c.parent_id
ORDER BY c.id`
	}
	rows, err := conn.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []db.FetchResult
	for rows.Next() {
		var node db.NodeState
		var size sql.NullInt64
		var parentGPL, resolved string
		if err := rows.Scan(&node.ID, &node.ServiceID, &node.ParentID, &node.ParentServiceID, &node.Path, &node.ParentPath, &node.Type, &size, &node.MTime, &node.Depth,
			&node.TraversalStatus, &node.CopyStatus, &node.DeleteStatus, &node.GPLStatus, &node.Excluded, &node.Errors,
			&node.GPLState, &parentGPL, &resolved, &node.Name); err != nil {
			return nil, err
		}
		if size.Valid {
			node.Size = size.Int64
		}
		node.Status = node.TraversalStatus
		if node.Name == "" {
			node.Name = basenameFromDisplayPath(node.Path)
		}
		out = append(out, db.FetchResult{
			Key:             node.ID,
			State:           &node,
			ParentGPLState:  parentGPL,
			ResolvedDstPath: resolved,
		})
	}
	return out, rows.Err()
}

func basenameFromDisplayPath(p string) string {
	if p == "" || p == "/" {
		return p
	}
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			if i+1 < len(p) {
				return p[i+1:]
			}
			return ""
		}
	}
	return p
}
