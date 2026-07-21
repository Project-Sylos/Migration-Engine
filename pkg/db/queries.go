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
	cteSrcCurrentStatus = `(WITH src_traversal AS (
		SELECT id, arg_max(traversal_status, event_time) AS traversal_status
		FROM src_status_events
		GROUP BY id
	), src_copy AS (
		SELECT id, arg_max(copy_status, event_time) AS copy_status
		FROM src_status_events
		WHERE COALESCE(copy_status, '') <> ''
		GROUP BY id
	), src_delete AS (
		SELECT id, arg_max(delete_status, event_time) AS delete_status
		FROM src_status_events
		WHERE COALESCE(delete_status, '') <> ''
		GROUP BY id
	)
	SELECT COALESCE(t.id, c.id, d.id) AS id,
		COALESCE(t.traversal_status, '') AS traversal_status,
		COALESCE(c.copy_status, '') AS copy_status,
		COALESCE(d.delete_status, '') AS delete_status
	FROM src_traversal t
	FULL OUTER JOIN src_copy c ON t.id = c.id
	FULL OUTER JOIN src_delete d ON COALESCE(t.id, c.id) = d.id)`
	cteDstCurrentStatus = `(SELECT id, arg_max(traversal_status, event_time) AS traversal_status FROM dst_status_events GROUP BY id)`
)

func statusJoinExpr(table string) (nodesAlias, cteAlias, cte string) {
	if table == "DST" {
		return "n", "e", cteDstCurrentStatus
	}
	return "n", "e", cteSrcCurrentStatus
}

// Node columns from joined form: n.* plus e.traversal_status, e.copy_status (SRC only), excluded derived, errors as ”.
func selectNodeColsWithStatus(table string) string {
	t := tableName(table)
	n, e, cte := statusJoinExpr(table)
	if table == "DST" {
		return `SELECT ` + n + `.id, ` + n + `.service_id, ` + n + `.parent_id, ` + n + `.parent_service_id, ` + n + `.path, ` + n + `.parent_path, ` + n + `.type, ` + n + `.size, ` + n + `.mtime, ` + n + `.depth, COALESCE(` + e + `.traversal_status,'') AS traversal_status, '' AS copy_status, '' AS delete_status, 0 AS excluded, '' AS errors FROM ` + t + ` ` + n + ` LEFT JOIN ` + cte + ` ` + e + ` ON ` + n + `.id = ` + e + `.id`
	}
	return `SELECT ` + n + `.id, ` + n + `.service_id, ` + n + `.parent_id, ` + n + `.parent_service_id, ` + n + `.path, ` + n + `.parent_path, ` + n + `.type, ` + n + `.size, ` + n + `.mtime, ` + n + `.depth, COALESCE(` + e + `.traversal_status,'') AS traversal_status, COALESCE(` + e + `.copy_status,'') AS copy_status, COALESCE(` + e + `.delete_status,'') AS delete_status, (COALESCE(` + e + `.copy_status,'') IN ('excluded_explicit','excluded_inherited')) AS excluded, '' AS errors FROM ` + t + ` ` + n + ` LEFT JOIN ` + cte + ` ` + e + ` ON ` + n + `.id = ` + e + `.id`
}

// selectNodeColsRaw returns node columns without joining status events (traversal/copy columns are empty defaults).
func selectNodeColsRaw(table string) string {
	t := tableName(table)
	if table == "DST" {
		return `SELECT n.id, n.service_id, n.parent_id, n.parent_service_id, n.path, n.parent_path, n.type, n.size, n.mtime, n.depth, '' AS traversal_status, '' AS copy_status, '' AS delete_status, 0 AS excluded, '' AS errors FROM ` + t + ` n`
	}
	return `SELECT n.id, n.service_id, n.parent_id, n.parent_service_id, n.path, n.parent_path, n.type, n.size, n.mtime, n.depth, '' AS traversal_status, '' AS copy_status, '' AS delete_status, 0 AS excluded, '' AS errors FROM ` + t + ` n`
}

// pullKeysetWindowSize is how many node rows we scan per round-trip when filtering pulls by event-derived status.
const pullKeysetWindowSize = 50000

// QueryNodesForReview returns nodes from the given table (SRC or DST) with optional depth, status, excluded, pathLike filters. Status from events. Used by review API.
func QueryNodesForReview(d *DB, table string, depth *int, status string, excluded *bool, pathLike string, orderByPath bool, limit, offset int) ([]NodeState, error) {
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	nodeAlias, e, _ := statusJoinExpr(table)
	ctx := context.Background()
	base := selectNodeColsWithStatus(table) + ` WHERE 1=1`
	args := []any{}
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
	// Exclusion is SRC-only (copy_status). DST has no excluded state.
	if excluded != nil && table == "SRC" {
		if *excluded {
			base += ` AND (` + e + `.copy_status = 'excluded_explicit' OR ` + e + `.copy_status = 'excluded_inherited')`
		} else {
			base += ` AND (COALESCE(` + e + `.copy_status,'') NOT IN ('excluded_explicit','excluded_inherited'))`
		}
	}
	if excluded != nil && table == "DST" && *excluded {
		base += ` AND 0=1`
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
		if err := rows.Scan(&n.ID, &n.ServiceID, &n.ParentID, &n.ParentServiceID, &n.Path, &n.ParentPath, &n.Type, &size, &n.MTime, &n.Depth, &n.TraversalStatus, &n.CopyStatus, &n.DeleteStatus, &n.Excluded, &n.Errors); err != nil {
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

const cteActiveIDMap = `(SELECT src_internal_id, arg_max(dst_internal_id, event_time) AS dst_internal_id
	FROM ` + tableIDMap + `
	WHERE status = 'active'
	GROUP BY src_internal_id)`

// cteAcceptedPathRemap is the latest accepted/committed destination basename per SRC node.
const cteAcceptedPathRemap = `(SELECT id, arg_max(proposed_path, event_time) AS proposed_path
	FROM ` + tablePathEvents + `
	WHERE status IN ('accepted', 'committed')
	GROUP BY id)`

// MergedReviewQueryBase returns the WITH clause for the merged src+dst review view (status from events). Use with " SELECT ... FROM merged" + where.
// SRC↔DST pairing uses id_map with path fallback when no active map row exists.
func MergedReviewQueryBase() string {
	return `WITH src_cur AS ` + cteSrcCurrentStatus + `, dst_cur AS ` + cteDstCurrentStatus + `,
idmap AS ` + cteActiveIDMap + `,
path_remap AS ` + cteAcceptedPathRemap + `,
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
	COALESCE(se.delete_status, '') AS delete_status,
	(COALESCE(se.copy_status,'') IN ('excluded_explicit','excluded_inherited')) AS excluded,
	COALESCE(s.size, d.size, 0) AS size,
	COALESCE(s.size, 0) AS src_size,
	COALESCE(d.size, 0) AS dst_size,
	COALESCE(s.parent_path, '') AS src_parent_path,
	COALESCE(d.parent_path, '') AS dst_parent_path,
	CASE
		WHEN COALESCE(s.path, d.path) = '' THEN ''
		WHEN strpos(reverse(COALESCE(s.path, d.path)), '/') = 0 THEN COALESCE(s.path, d.path)
		ELSE right(COALESCE(s.path, d.path), strpos(reverse(COALESCE(s.path, d.path)), '/') - 1)
	END AS name,
	COALESCE(pr.proposed_path, '') AS resolved_dst_name
FROM src_nodes s
LEFT JOIN src_cur se ON s.id = se.id
LEFT JOIN idmap im ON im.src_internal_id = s.id
LEFT JOIN dst_nodes d ON (
	(im.dst_internal_id IS NOT NULL AND d.id = im.dst_internal_id)
	OR (im.dst_internal_id IS NULL AND d.path = s.path)
)
LEFT JOIN dst_cur de ON d.id = de.id
LEFT JOIN path_remap pr ON pr.id = s.id

UNION ALL

SELECT
	d.path,
	d.parent_path,
	COALESCE(d.depth, 0) AS depth,
	COALESCE(d.type, '') AS type,
	'' AS src_node_id,
	COALESCE(d.id, '') AS dst_node_id,
	'' AS src_traversal_status,
	COALESCE(de.traversal_status, '') AS dst_traversal_status,
	'' AS copy_status,
	'' AS delete_status,
	FALSE AS excluded,
	COALESCE(d.size, 0) AS size,
	0 AS src_size,
	COALESCE(d.size, 0) AS dst_size,
	'' AS src_parent_path,
	COALESCE(d.parent_path, '') AS dst_parent_path,
	CASE
		WHEN d.path = '' THEN ''
		WHEN strpos(reverse(d.path), '/') = 0 THEN d.path
		ELSE right(d.path, strpos(reverse(d.path), '/') - 1)
	END AS name,
	'' AS resolved_dst_name
FROM dst_nodes d
LEFT JOIN dst_cur de ON d.id = de.id
WHERE NOT EXISTS (
	SELECT 1 FROM src_nodes s
	LEFT JOIN idmap im ON im.src_internal_id = s.id
	WHERE (im.dst_internal_id IS NOT NULL AND im.dst_internal_id = d.id)
	   OR (im.dst_internal_id IS NULL AND s.path = d.path)
)
)`
}

// mergedReviewQueryBaseParentFolder is the same logical merged view as MergedReviewQueryBase, but restricted to direct
// children of parent_path = $1 (normalized). Status CTEs aggregate events only for those src/dst node ids.
func mergedReviewQueryBaseParentFolder() string {
	return `WITH folder_src AS (
	SELECT * FROM src_nodes WHERE parent_path = $1
),
folder_dst_children AS (
	SELECT * FROM dst_nodes WHERE parent_path = $1
),
idmap AS ` + cteActiveIDMap + `,
path_remap AS (
	SELECT pe.id, arg_max(pe.proposed_path, pe.event_time) AS proposed_path
	FROM ` + tablePathEvents + ` pe
	INNER JOIN folder_src fs ON pe.id = fs.id
	WHERE pe.status IN ('accepted', 'committed')
	GROUP BY pe.id
),
src_cur AS (
	SELECT COALESCE(t.id, c.id, d.id) AS id,
		COALESCE(t.traversal_status, '') AS traversal_status,
		COALESCE(c.copy_status, '') AS copy_status,
		COALESCE(d.delete_status, '') AS delete_status
	FROM (
		SELECT se.id, arg_max(se.traversal_status, se.event_time) AS traversal_status
		FROM src_status_events se
		INNER JOIN folder_src fs ON se.id = fs.id
		GROUP BY se.id
	) t
	FULL OUTER JOIN (
		SELECT se.id, arg_max(se.copy_status, se.event_time) AS copy_status
		FROM src_status_events se
		INNER JOIN folder_src fs ON se.id = fs.id
		WHERE COALESCE(se.copy_status, '') <> ''
		GROUP BY se.id
	) c ON t.id = c.id
	FULL OUTER JOIN (
		SELECT se.id, arg_max(se.delete_status, se.event_time) AS delete_status
		FROM src_status_events se
		INNER JOIN folder_src fs ON se.id = fs.id
		WHERE COALESCE(se.delete_status, '') <> ''
		GROUP BY se.id
	) d ON COALESCE(t.id, c.id) = d.id
),
dst_cur AS (
	SELECT se.id, arg_max(se.traversal_status, se.event_time) AS traversal_status
	FROM dst_status_events se
	INNER JOIN (
		SELECT DISTINCT d.id FROM folder_dst_children d
		UNION
		SELECT im.dst_internal_id FROM folder_src s
		INNER JOIN idmap im ON im.src_internal_id = s.id
		WHERE im.dst_internal_id IS NOT NULL
	) dids ON se.id = dids.id
	GROUP BY se.id
),
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
	COALESCE(se.delete_status, '') AS delete_status,
	(COALESCE(se.copy_status,'') IN ('excluded_explicit','excluded_inherited')) AS excluded,
	COALESCE(s.size, d.size, 0) AS size,
	COALESCE(s.size, 0) AS src_size,
	COALESCE(d.size, 0) AS dst_size,
	COALESCE(s.parent_path, '') AS src_parent_path,
	COALESCE(d.parent_path, '') AS dst_parent_path,
	CASE
		WHEN COALESCE(s.path, d.path) = '' THEN ''
		WHEN strpos(reverse(COALESCE(s.path, d.path)), '/') = 0 THEN COALESCE(s.path, d.path)
		ELSE right(COALESCE(s.path, d.path), strpos(reverse(COALESCE(s.path, d.path)), '/') - 1)
	END AS name,
	COALESCE(pr.proposed_path, '') AS resolved_dst_name
FROM folder_src s
LEFT JOIN src_cur se ON s.id = se.id
LEFT JOIN idmap im ON im.src_internal_id = s.id
LEFT JOIN dst_nodes d ON (
	(im.dst_internal_id IS NOT NULL AND d.id = im.dst_internal_id)
	OR (im.dst_internal_id IS NULL AND d.path = s.path)
)
LEFT JOIN dst_cur de ON d.id = de.id
LEFT JOIN path_remap pr ON pr.id = s.id

UNION ALL

SELECT
	d.path,
	d.parent_path,
	COALESCE(d.depth, 0) AS depth,
	COALESCE(d.type, '') AS type,
	'' AS src_node_id,
	COALESCE(d.id, '') AS dst_node_id,
	'' AS src_traversal_status,
	COALESCE(de.traversal_status, '') AS dst_traversal_status,
	'' AS copy_status,
	'' AS delete_status,
	FALSE AS excluded,
	COALESCE(d.size, 0) AS size,
	0 AS src_size,
	COALESCE(d.size, 0) AS dst_size,
	'' AS src_parent_path,
	COALESCE(d.parent_path, '') AS dst_parent_path,
	CASE
		WHEN d.path = '' THEN ''
		WHEN strpos(reverse(d.path), '/') = 0 THEN d.path
		ELSE right(d.path, strpos(reverse(d.path), '/') - 1)
	END AS name,
	'' AS resolved_dst_name
FROM folder_dst_children d
LEFT JOIN dst_cur de ON d.id = de.id
WHERE NOT EXISTS (
	SELECT 1 FROM folder_src s
	LEFT JOIN idmap im ON im.src_internal_id = s.id
	WHERE (im.dst_internal_id IS NOT NULL AND im.dst_internal_id = d.id)
	   OR (im.dst_internal_id IS NULL AND s.path = d.path)
)
)`
}

func mergedReviewQueryBaseForFilter(f ReviewFilter) string {
	if strings.TrimSpace(f.ParentPath) != "" {
		return mergedReviewQueryBaseParentFolder()
	}
	return MergedReviewQueryBase()
}

// MergedReviewRow is one row from the merged review view (SRC+DST joined via id_map/path, status from events).
type MergedReviewRow struct {
	Path               string
	Name               string
	Depth              int
	Type               string
	SrcNodeID          string
	DstNodeID          string
	SrcTraversalStatus string
	DstTraversalStatus string
	CopyStatus         string
	DeleteStatus       string
	Excluded           bool
	Size               int64
	// ResolvedDstName is the accepted/committed destination basename from path_events (empty if none).
	ResolvedDstName string
}

// ReviewFilter narrows merged review rows for listing, search, and counts.
// ParentPath: if non-empty, only direct children of this path (uses normalized parent_path).
// Query + QueryField: substring match; QueryField "" or unknown = path OR name; "path" = path only; "name" = name only.
// StatusSearchType + TraversalStatus + CopyStatus filter merged review rows ("traversal", "copy", "both").
// TypeFilter: "folder" or "file" (case-insensitive); also FoldersOnly implies folder.
// ExcludeRoot: if true, exclude path = '/' from results (for global search).
// ExcludeDestinationOnly: if true, omit rows with no SRC node (destination-only / not_on_src).
type ReviewFilter struct {
	ParentPath  string
	Query       string
	QueryField  string
	FoldersOnly bool
	ExcludeRoot bool

	StatusSearchType string
	TraversalStatus  string
	CopyStatus       string
	DeleteStatus     string

	TypeFilter string

	DepthOperator string
	DepthValue    *int
	SizeOperator  string
	SizeValue     *int64

	ExcludeDestinationOnly bool

	// PathIssueFilter narrows by destination naming / compatibility review state:
	//   "issues"   — pending/collision suggestions only (excludes manual_review)
	//   "manual"   — needs manual rename (manual_review)
	//   "accepted" — latest path_events accepted/committed (and not ignored)
	//   "rejected" — gpl_status ignored (warnings dismissed)
	//   "none"     — no active/accepted/rejected compat footprint
	PathIssueFilter string
	// PathIssueCategory narrows suggestion path_events (pending/collision) whose gpl_issues
	// JSON mentions this category (e.g. InvalidChar). Empty means no category filter.
	PathIssueCategory string
}

func reviewFilterUsesStructuredStatus(f ReviewFilter) bool {
	if strings.TrimSpace(f.StatusSearchType) != "" {
		return true
	}
	if strings.TrimSpace(f.TraversalStatus) != "" || strings.TrimSpace(f.CopyStatus) != "" {
		return true
	}
	if strings.TrimSpace(f.DeleteStatus) != "" {
		return true
	}
	return false
}

func appendTraversalStatusClause(parts []string, args []any, param int, value string) ([]string, []any, int) {
	v := strings.TrimSpace(value)
	if v == "" {
		return parts, args, param
	}
	if strings.EqualFold(v, "not_on_src") {
		parts = append(parts, `LOWER(dst_traversal_status) = $`+strconv.Itoa(param))
		args = append(args, "not_on_src")
		param++
		return parts, args, param
	}
	parts = append(parts, `(LOWER(src_traversal_status) = LOWER($`+strconv.Itoa(param)+`) OR LOWER(dst_traversal_status) = LOWER($`+strconv.Itoa(param+1)+`))`)
	args = append(args, v, v)
	param += 2
	return parts, args, param
}

func appendCopyStatusClause(parts []string, args []any, param int, value string) ([]string, []any, int) {
	v := strings.TrimSpace(value)
	if v == "" {
		return parts, args, param
	}
	if strings.EqualFold(v, "excluded") {
		parts = append(parts, `(excluded OR LOWER(copy_status) IN ('excluded_explicit','excluded_inherited'))`)
		return parts, args, param
	}
	if strings.EqualFold(v, "successful") {
		// UI "Exists (on both)" / copy-complete filter includes DST matches.
		parts = append(parts, `LOWER(copy_status) IN ('successful','already_existed')`)
		return parts, args, param
	}
	parts = append(parts, `LOWER(copy_status) = LOWER($`+strconv.Itoa(param)+`)`)
	args = append(args, v)
	param++
	return parts, args, param
}

func appendDeleteStatusClause(parts []string, args []any, param int, value string) ([]string, []any, int) {
	v := strings.TrimSpace(value)
	if v == "" {
		return parts, args, param
	}
	if strings.EqualFold(v, "excluded") || strings.EqualFold(v, "skipped") {
		parts = append(parts, `LOWER(delete_status) = 'skipped'`)
		return parts, args, param
	}
	parts = append(parts, `LOWER(delete_status) = LOWER($`+strconv.Itoa(param)+`)`)
	args = append(args, v)
	param++
	return parts, args, param
}

// pathIssueAnyActiveSQL matches any open compatibility queue row (suggestions + manual rename).
const pathIssueAnyActiveSQL = `src_node_id <> '' AND EXISTS (
	SELECT 1
	FROM (
		SELECT id, arg_max(status, event_time) AS status
		FROM path_events
		GROUP BY id
	) pe
	LEFT JOIN (
		SELECT id, arg_max(gpl_status, event_time) AS gpl_status
		FROM src_status_events
		WHERE COALESCE(gpl_status, '') <> ''
		GROUP BY id
	) g ON g.id = pe.id
	WHERE pe.id = src_node_id
	  AND pe.status IN ('pending', 'collision', 'manual_review')
	  AND COALESCE(g.gpl_status, '') <> 'ignored'
)`

// pathIssueSuggestionsSQL matches auto-fixable / collision suggestions only (not manual_review).
const pathIssueSuggestionsSQL = `src_node_id <> '' AND EXISTS (
	SELECT 1
	FROM (
		SELECT id, arg_max(status, event_time) AS status
		FROM path_events
		GROUP BY id
	) pe
	LEFT JOIN (
		SELECT id, arg_max(gpl_status, event_time) AS gpl_status
		FROM src_status_events
		WHERE COALESCE(gpl_status, '') <> ''
		GROUP BY id
	) g ON g.id = pe.id
	WHERE pe.id = src_node_id
	  AND pe.status IN ('pending', 'collision')
	  AND COALESCE(g.gpl_status, '') <> 'ignored'
)`

const pathIssueAcceptedSQL = `src_node_id <> '' AND EXISTS (
	SELECT 1
	FROM (
		SELECT id, arg_max(status, event_time) AS status
		FROM path_events
		GROUP BY id
	) pe
	LEFT JOIN (
		SELECT id, arg_max(gpl_status, event_time) AS gpl_status
		FROM src_status_events
		WHERE COALESCE(gpl_status, '') <> ''
		GROUP BY id
	) g ON g.id = pe.id
	WHERE pe.id = src_node_id
	  AND pe.status IN ('accepted', 'committed')
	  AND COALESCE(g.gpl_status, '') <> 'ignored'
)`

const pathIssueRejectedSQL = `src_node_id <> '' AND EXISTS (
	SELECT 1
	FROM (
		SELECT id, arg_max(gpl_status, event_time) AS gpl_status
		FROM src_status_events
		WHERE COALESCE(gpl_status, '') <> ''
		GROUP BY id
	) g
	WHERE g.id = src_node_id
	  AND COALESCE(g.gpl_status, '') = 'ignored'
)`

const pathIssueManualSQL = `src_node_id <> '' AND EXISTS (
	SELECT 1
	FROM (
		SELECT id, arg_max(status, event_time) AS status
		FROM path_events
		GROUP BY id
	) pe
	LEFT JOIN (
		SELECT id, arg_max(gpl_status, event_time) AS gpl_status
		FROM src_status_events
		WHERE COALESCE(gpl_status, '') <> ''
		GROUP BY id
	) g ON g.id = pe.id
	WHERE pe.id = src_node_id
	  AND pe.status = 'manual_review'
	  AND COALESCE(g.gpl_status, '') <> 'ignored'
)`

func appendPathIssueFilterClause(parts []string, value string) []string {
	v := strings.ToLower(strings.TrimSpace(value))
	switch v {
	case "issues", "active", "pending", "suggestions":
		return append(parts, pathIssueSuggestionsSQL)
	case "accepted":
		return append(parts, pathIssueAcceptedSQL)
	case "rejected", "ignored":
		return append(parts, pathIssueRejectedSQL)
	case "manual", "manual_review":
		return append(parts, pathIssueManualSQL)
	case "none", "clean":
		return append(parts, `NOT (`+pathIssueAnyActiveSQL+`) AND NOT (`+pathIssueAcceptedSQL+`) AND NOT (`+pathIssueRejectedSQL+`)`)
	default:
		return parts
	}
}

// sanitizePathIssueCategory allows only simple category tokens used in gpl_issues JSON.
func sanitizePathIssueCategory(category string) string {
	cat := strings.TrimSpace(category)
	if cat == "" {
		return ""
	}
	for _, r := range cat {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			continue
		}
		return ""
	}
	return cat
}

func appendPathIssueCategoryClause(parts []string, args []any, param int, category string) ([]string, []any, int) {
	cat := sanitizePathIssueCategory(category)
	if cat == "" {
		return parts, args, param
	}
	// Match `"category":"InvalidChar"` (and similar) inside latest path_events.gpl_issues.
	needle := `"category":"` + cat + `"`
	parts = append(parts, `src_node_id <> '' AND EXISTS (
	SELECT 1
	FROM (
		SELECT id,
			arg_max(status, event_time) AS status,
			arg_max(gpl_issues, event_time) AS gpl_issues
		FROM path_events
		GROUP BY id
	) pe
	LEFT JOIN (
		SELECT id, arg_max(gpl_status, event_time) AS gpl_status
		FROM src_status_events
		WHERE COALESCE(gpl_status, '') <> ''
		GROUP BY id
	) g ON g.id = pe.id
	WHERE pe.id = src_node_id
	  AND pe.status IN ('pending', 'collision')
	  AND COALESCE(g.gpl_status, '') <> 'ignored'
	  AND strpos(COALESCE(pe.gpl_issues, ''), $`+strconv.Itoa(param)+`) > 0
)`)
	args = append(args, needle)
	param++
	return parts, args, param
}

// buildMergedReviewWhere returns a WHERE clause and args for the merged CTE. Param placeholders are $1, $2, ...
func buildMergedReviewWhere(f ReviewFilter) (clause string, args []any) {
	var parts []string
	param := 1
	if f.ParentPath != "" {
		parts = append(parts, `parent_path = $`+strconv.Itoa(param))
		args = append(args, NormalizeRootRelativePath(f.ParentPath))
		param++
	}
	if f.Query != "" {
		q := "%" + strings.ToLower(strings.TrimSpace(f.Query)) + "%"
		switch strings.ToLower(strings.TrimSpace(f.QueryField)) {
		case "path":
			parts = append(parts, `LOWER(path) LIKE $`+strconv.Itoa(param))
			args = append(args, q)
			param++
		case "name":
			parts = append(parts, `LOWER(name) LIKE $`+strconv.Itoa(param))
			args = append(args, q)
			param++
		default:
			parts = append(parts, `(LOWER(path) LIKE $`+strconv.Itoa(param)+` OR LOWER(name) LIKE $`+strconv.Itoa(param)+`)`)
			args = append(args, q)
			param++
		}
	}

	structured := reviewFilterUsesStructuredStatus(f)
	if structured {
		st := strings.ToLower(strings.TrimSpace(f.StatusSearchType))
		trav := strings.TrimSpace(f.TraversalStatus)
		copySt := strings.TrimSpace(f.CopyStatus)
		delSt := strings.TrimSpace(f.DeleteStatus)

		needTrav := trav != "" && (st == "traversal" || st == "both")
		needCopy := copySt != "" && (st == "copy" || st == "both")
		needDelete := delSt != "" && (st == "delete" || st == "both")

		if st == "" {
			if trav != "" {
				parts, args, param = appendTraversalStatusClause(parts, args, param, trav)
			}
			if copySt != "" {
				parts, args, param = appendCopyStatusClause(parts, args, param, copySt)
			}
			if delSt != "" {
				parts, args, param = appendDeleteStatusClause(parts, args, param, delSt)
			}
		} else {
			if needTrav {
				parts, args, param = appendTraversalStatusClause(parts, args, param, trav)
			}
			if needCopy {
				parts, args, param = appendCopyStatusClause(parts, args, param, copySt)
			}
			if needDelete {
				parts, args, param = appendDeleteStatusClause(parts, args, param, delSt)
			}
		}
	}

	if f.FoldersOnly || strings.EqualFold(f.TypeFilter, "folder") {
		parts = append(parts, `type = 'folder'`)
	} else if strings.EqualFold(f.TypeFilter, "file") {
		parts = append(parts, `type = 'file'`)
	}
	if f.ExcludeRoot {
		parts = append(parts, `path <> '/'`)
	}
	if f.ExcludeDestinationOnly {
		parts = append(parts, `src_node_id <> ''`)
	}

	if strings.TrimSpace(f.PathIssueFilter) != "" {
		parts = appendPathIssueFilterClause(parts, f.PathIssueFilter)
	}
	if strings.TrimSpace(f.PathIssueCategory) != "" {
		parts, args, param = appendPathIssueCategoryClause(parts, args, param, f.PathIssueCategory)
	}

	if f.DepthValue != nil && f.DepthOperator != "" {
		col := "depth"
		op := "="
		switch strings.ToLower(f.DepthOperator) {
		case "equals", "=":
			op = "="
		case "gt", ">":
			op = ">"
		case "gte", ">=":
			op = ">="
		case "lt", "<":
			op = "<"
		case "lte", "<=":
			op = "<="
		default:
			op = "="
		}
		parts = append(parts, col+` `+op+` $`+strconv.Itoa(param))
		args = append(args, *f.DepthValue)
		param++
	}
	if f.SizeValue != nil && f.SizeOperator != "" {
		col := "size"
		op := "="
		switch strings.ToLower(f.SizeOperator) {
		case "equals", "=":
			op = "="
		case "gt", ">":
			op = ">"
		case "gte", ">=":
			op = ">="
		case "lt", "<":
			op = "<"
		case "lte", "<=":
			op = "<="
		default:
			op = "="
		}
		parts = append(parts, col+` `+op+` $`+strconv.Itoa(param))
		args = append(args, *f.SizeValue)
		param++
	}

	if len(parts) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(parts, " AND "), args
}

// ListMergedReviewDiffs returns merged review rows and total count matching the filter, ordered and paginated.
func ListMergedReviewDiffs(d *DB, f ReviewFilter, orderBy string, limit, offset int) ([]MergedReviewRow, int, error) {
	conn, err := d.GetDB()
	if err != nil {
		return nil, 0, err
	}
	ctx := context.Background()
	where, args := buildMergedReviewWhere(f)
	if orderBy == "" {
		orderBy = "path ASC"
	}
	base := mergedReviewQueryBaseForFilter(f)
	countQ := base + ` SELECT COUNT(*)::INT FROM merged` + where
	var total int
	if err := conn.QueryRowContext(ctx, countQ, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	sel := base + ` SELECT path, name, depth, type, src_node_id, dst_node_id, src_traversal_status, dst_traversal_status, copy_status, delete_status, excluded, size, resolved_dst_name FROM merged` + where +
		` ORDER BY ` + orderBy + ` LIMIT $` + strconv.Itoa(len(args)+1) + ` OFFSET $` + strconv.Itoa(len(args)+2)
	listArgs := append(append([]any{}, args...), limit, offset)
	rows, err := conn.QueryContext(ctx, sel, listArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []MergedReviewRow
	for rows.Next() {
		var r MergedReviewRow
		if err := rows.Scan(&r.Path, &r.Name, &r.Depth, &r.Type, &r.SrcNodeID, &r.DstNodeID, &r.SrcTraversalStatus, &r.DstTraversalStatus, &r.CopyStatus, &r.DeleteStatus, &r.Excluded, &r.Size, &r.ResolvedDstName); err != nil {
			return nil, 0, err
		}
		out = append(out, r)
	}
	return out, total, rows.Err()
}

// CountMergedReviewRows returns the number of merged rows matching the filter.
func CountMergedReviewRows(d *DB, f ReviewFilter) (int, error) {
	conn, err := d.GetDB()
	if err != nil {
		return 0, err
	}
	where, args := buildMergedReviewWhere(f)
	q := mergedReviewQueryBaseForFilter(f) + ` SELECT COUNT(*)::INT FROM merged` + where
	var n int
	err = conn.QueryRowContext(context.Background(), q, args...).Scan(&n)
	return n, err
}

// MergedReviewStats holds aggregate counts over merged review rows (one row per path; same filter semantics as list/count).
type MergedReviewStats struct {
	Total           int
	Folders         int
	Files           int
	MissingOnSource int
	MissingOnDest   int
	Excluded        int
	SizeSrc         int64 // sum of file sizes on SRC (unique by path)
	SizeDst         int64 // sum of file sizes on DST (unique by path)
}

// GetMergedReviewStats returns aggregate counts for rows matching the filter (single query with FILTER). Counts are unique by path (merged view = one row per path).
func GetMergedReviewStats(d *DB, f ReviewFilter) (MergedReviewStats, error) {
	conn, err := d.GetDB()
	if err != nil {
		return MergedReviewStats{}, err
	}
	where, args := buildMergedReviewWhere(f)
	q := mergedReviewQueryBaseForFilter(f) + ` SELECT
		COUNT(*)::INT,
		COUNT(*) FILTER (WHERE type = 'folder')::INT,
		COUNT(*) FILTER (WHERE type = 'file')::INT,
		COUNT(*) FILTER (WHERE src_node_id = '' OR src_node_id IS NULL)::INT,
		COUNT(*) FILTER (WHERE dst_node_id = '' OR dst_node_id IS NULL)::INT,
		COUNT(*) FILTER (WHERE excluded)::INT,
		COALESCE(SUM(src_size) FILTER (WHERE type = 'file'), 0)::BIGINT,
		COALESCE(SUM(dst_size) FILTER (WHERE type = 'file'), 0)::BIGINT
	FROM merged` + where
	var s MergedReviewStats
	err = conn.QueryRowContext(context.Background(), q, args...).Scan(
		&s.Total, &s.Folders, &s.Files, &s.MissingOnSource, &s.MissingOnDest, &s.Excluded, &s.SizeSrc, &s.SizeDst,
	)
	return s, err
}

// GetTotalFileSizes returns the sum of size across src_nodes and dst_nodes (for API totalFileSize).
func GetTotalFileSizes(d *DB) (srcTotal, dstTotal int64, err error) {
	conn, err := d.GetDB()
	if err != nil {
		return 0, 0, err
	}
	ctx := context.Background()
	if err := conn.QueryRowContext(ctx, `SELECT COALESCE(SUM(size), 0) FROM `+tableSrcNodes).Scan(&srcTotal); err != nil {
		return 0, 0, err
	}
	if err := conn.QueryRowContext(ctx, `SELECT COALESCE(SUM(size), 0) FROM `+tableDstNodes).Scan(&dstTotal); err != nil {
		return 0, 0, err
	}
	return srcTotal, dstTotal, nil
}

// GetNodeByID returns the node by id from the given table. Status is derived from latest status event.
func GetNodeByID(d *DB, table, id string) (*NodeState, error) {
	queueType := table
	if queueType != "SRC" && queueType != "DST" {
		queueType = "SRC"
	}
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	var n NodeState
	var size sql.NullInt64
	q := selectNodeColsWithStatus(table) + ` WHERE n.id = $1`
	err = conn.QueryRowContext(ctx, q, id).Scan(&n.ID, &n.ServiceID, &n.ParentID, &n.ParentServiceID, &n.Path, &n.ParentPath, &n.Type, &size, &n.MTime, &n.Depth, &n.TraversalStatus, &n.CopyStatus, &n.DeleteStatus, &n.Excluded, &n.Errors)
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
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	var n NodeState
	var size sql.NullInt64
	q := selectNodeColsWithStatus(table) + ` WHERE n.path = $1`
	err = conn.QueryRowContext(ctx, q, NormalizeRootRelativePath(path)).Scan(&n.ID, &n.ServiceID, &n.ParentID, &n.ParentServiceID, &n.Path, &n.ParentPath, &n.Type, &size, &n.MTime, &n.Depth, &n.TraversalStatus, &n.CopyStatus, &n.DeleteStatus, &n.Excluded, &n.Errors)
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
	rows, err := conn.QueryContext(ctx, q, NormalizeRootRelativePath(parentPath), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*NodeState
	for rows.Next() {
		var n NodeState
		var size sql.NullInt64
		if err := rows.Scan(&n.ID, &n.ServiceID, &n.ParentID, &n.ParentServiceID, &n.Path, &n.ParentPath, &n.Type, &size, &n.MTime, &n.Depth, &n.TraversalStatus, &n.CopyStatus, &n.DeleteStatus, &n.Excluded, &n.Errors); err != nil {
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
		if err := rows.Scan(&n.ID, &n.ServiceID, &n.ParentID, &n.ParentServiceID, &n.Path, &n.ParentPath, &n.Type, &size, &n.MTime, &n.Depth, &n.TraversalStatus, &n.CopyStatus, &n.DeleteStatus, &n.Excluded, &n.Errors); err != nil {
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

func listNodesByDepthKeysetNoStatus(ctx context.Context, conn *sql.DB, table string, depth int, afterID string, limit int) ([]FetchResult, error) {
	base := selectNodeColsRaw(table) + ` WHERE n.depth = $1`
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
func queryDepthKeysetWindowWithStatus(ctx context.Context, conn *sql.DB, table string, isDST bool, depth int, afterID string, window int) ([]FetchResult, error) {
	t := tableName(table)
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
	SELECT id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth
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
	'' AS errors
FROM cand c
LEFT JOIN trav ON trav.id = c.id
ORDER BY c.id`
	} else {
		q = `WITH cand AS (
	SELECT id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth
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
	'' AS errors
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

func scanFetchResults(rows *sql.Rows) ([]FetchResult, error) {
	var out []FetchResult
	for rows.Next() {
		var node NodeState
		var size sql.NullInt64
		if err := rows.Scan(&node.ID, &node.ServiceID, &node.ParentID, &node.ParentServiceID, &node.Path, &node.ParentPath, &node.Type, &size, &node.MTime, &node.Depth, &node.TraversalStatus, &node.CopyStatus, &node.DeleteStatus, &node.Excluded, &node.Errors); err != nil {
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

// ListNodesByDepthKeyset returns nodes at the given depth, ordered by id, after afterID, up to limit rows.
// If statusFilter is empty, returns rows without joining status events (traversal/copy columns empty).
// If statusFilter is non-empty, only rows whose current traversal_status matches are returned; status is derived from events aggregated only for ids in each keyset window (not whole tables).
func ListNodesByDepthKeyset(d *DB, table string, depth int, afterID, statusFilter string, limit int) ([]FetchResult, error) {
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
	out := make([]FetchResult, 0, limit)
	cursor := afterID
	for len(out) < limit {
		window, err := queryDepthKeysetWindowWithStatus(ctx, conn, table, isDST, depth, cursor, pullKeysetWindowSize)
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
		if len(window) < pullKeysetWindowSize {
			break
		}
	}
	return out, nil
}

func queryCopyKeysetWindow(ctx context.Context, conn *sql.DB, depth int, nodeType, afterID string, window int) ([]FetchResult, error) {
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
	SELECT id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth
	FROM ` + tableSrcNodes + ` n
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
path_events_cur AS (
	SELECT pe.id, arg_max(pe.proposed_path, pe.event_time) AS proposed_path
	FROM ` + tablePathEvents + ` pe
	INNER JOIN cand c ON c.id = pe.id
	WHERE pe.status IN ('accepted', 'committed')
	GROUP BY pe.id
),
idmap_self AS (
	SELECT src_internal_id, arg_max(dst_internal_id, event_time) AS dst_internal_id
	FROM ` + tableIDMap + `
	INNER JOIN cand c ON c.id = ` + tableIDMap + `.src_internal_id
	WHERE status = 'active'
	GROUP BY src_internal_id
),
idmap_parent AS (
	SELECT src_internal_id, arg_max(dst_internal_id, event_time) AS dst_internal_id
	FROM ` + tableIDMap + `
	INNER JOIN cand c ON c.parent_id = ` + tableIDMap + `.src_internal_id
	WHERE status = 'active'
	GROUP BY src_internal_id
)
SELECT c.id, c.service_id, c.parent_id, c.parent_service_id, c.path, c.parent_path, c.type, c.size, c.mtime, c.depth,
	COALESCE(trav.traversal_status,'') AS traversal_status,
	COALESCE(cpy.copy_status,'') AS copy_status,
	COALESCE(del.delete_status,'') AS delete_status,
	(COALESCE(cpy.copy_status,'') IN ('excluded_explicit','excluded_inherited')) AS excluded,
	'' AS errors,
	COALESCE(dst_parent.service_id,'') AS dst_parent_service_id,
	COALESCE(dst_parent.id,'') AS dst_parent_internal_id,
	COALESCE(path_events_cur.proposed_path,'') AS resolved_dst_path,
	COALESCE(idmap_self.dst_internal_id,'') AS dst_mapped_id
FROM cand c
LEFT JOIN trav ON trav.id = c.id
LEFT JOIN cpy ON cpy.id = c.id
LEFT JOIN del ON del.id = c.id
LEFT JOIN idmap_parent ON idmap_parent.src_internal_id = c.parent_id
LEFT JOIN ` + tableDstNodes + ` dst_parent ON dst_parent.id = idmap_parent.dst_internal_id
LEFT JOIN path_events_cur ON path_events_cur.id = c.id
LEFT JOIN idmap_self ON idmap_self.src_internal_id = c.id
ORDER BY c.id`
	rows, err := conn.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FetchResult
	for rows.Next() {
		var node NodeState
		var size sql.NullInt64
		var dstParentServiceID, dstParentNodeID, resolvedDstPath, dstMappedID string
		if err := rows.Scan(&node.ID, &node.ServiceID, &node.ParentID, &node.ParentServiceID, &node.Path, &node.ParentPath, &node.Type, &size, &node.MTime, &node.Depth, &node.TraversalStatus, &node.CopyStatus, &node.DeleteStatus, &node.Excluded, &node.Errors, &dstParentServiceID, &dstParentNodeID, &resolvedDstPath, &dstMappedID); err != nil {
			return nil, err
		}
		if size.Valid {
			node.Size = size.Int64
		}
		node.Status = node.TraversalStatus
		node.Name = node.Path
		out = append(out, FetchResult{
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

// ListNodesCopyKeyset returns src_nodes at depth with current copy_status = statusFilter (event-derived), ordered by id. Pass CopyStatusPending for copy phase, CopyStatusFailed for copy-retry.
func ListNodesCopyKeyset(d *DB, depth int, nodeType, afterID string, limit int, statusFilter string) ([]FetchResult, error) {
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	out := make([]FetchResult, 0, limit)
	cursor := afterID
	for len(out) < limit {
		window, err := queryCopyKeysetWindow(ctx, conn, depth, nodeType, cursor, pullKeysetWindowSize)
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
		if len(window) < pullKeysetWindowSize {
			break
		}
	}
	return out, nil
}

// queryDeleteKeysetWindow is like queryCopyKeysetWindow but without dst parent join.
func queryDeleteKeysetWindow(ctx context.Context, conn *sql.DB, depth int, nodeType, afterID string, window int) ([]FetchResult, error) {
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
	SELECT id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth
	FROM ` + tableSrcNodes + ` n
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
	'' AS errors
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
func ListNodesDeleteKeyset(d *DB, depth int, nodeType, afterID string, limit int, statusFilter string) ([]FetchResult, error) {
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	out := make([]FetchResult, 0, limit)
	cursor := afterID
	for len(out) < limit {
		window, err := queryDeleteKeysetWindow(ctx, conn, depth, nodeType, cursor, pullKeysetWindowSize)
		if err != nil {
			return nil, err
		}
		if len(window) == 0 {
			break
		}
		for i := range window {
			st := window[i].State
			if st != nil && !st.Excluded && CopyStatusEligibleForDelete(st.CopyStatus) && st.DeleteStatus == statusFilter {
				out = append(out, window[i])
				if len(out) == limit {
					return out, nil
				}
			}
		}
		cursor = window[len(window)-1].Key
		if len(window) < pullKeysetWindowSize {
			break
		}
	}
	return out, nil
}

// FolderDeleteBlockedIDs returns folder node IDs (subset of parentIDs) that have a direct non-excluded child whose delete_status is not 'deleted'.
func FolderDeleteBlockedIDs(d *DB, parentIDs []string) (map[string]bool, error) {
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
FROM ` + tableSrcNodes + ` p
INNER JOIN ` + tableSrcNodes + ` ch ON ch.parent_id = p.id
LEFT JOIN ` + cteSrcCurrentStatus + ` ce ON ch.id = ce.id
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

// CountExcludedInSubtree returns the number of SRC nodes in the subtree with copy_status in (excluded_explicit, excluded_inherited). Exclusion is SRC-only; returns 0 for DST.
func CountExcludedInSubtree(d *DB, table, rootPath string) (int, error) {
	if table == "DST" {
		return 0, nil
	}
	conn, err := d.GetDB()
	if err != nil {
		return 0, err
	}
	nodeAlias, e, cte := statusJoinExpr("SRC")
	ctx := context.Background()
	base := `SELECT COUNT(*)::INT FROM ` + tableSrcNodes + ` ` + nodeAlias + ` LEFT JOIN ` + cte + ` ` + e + ` ON ` + nodeAlias + `.id = ` + e + `.id WHERE (COALESCE(` + e + `.copy_status,'') IN ('excluded_explicit','excluded_inherited'))`
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
func CountExcluded(d *DB, table string) (int, error) {
	if table == "DST" {
		return 0, nil
	}
	conn, err := d.GetDB()
	if err != nil {
		return 0, err
	}
	nodeAlias, e, cte := statusJoinExpr("SRC")
	ctx := context.Background()
	q := `SELECT COUNT(*)::INT FROM ` + tableSrcNodes + ` ` + nodeAlias + ` LEFT JOIN ` + cte + ` ` + e + ` ON ` + nodeAlias + `.id = ` + e + `.id WHERE (COALESCE(` + e + `.copy_status,'') IN ('excluded_explicit','excluded_inherited'))`
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
	conn, err := d.GetDB()
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

// dstKeysetWindowRow is one DST node row from a keyset window.
type dstKeysetWindowRow struct {
	id, serviceID, parentID, parentServiceID, path, parentPath, typ, mtime string
	size                                                                   sql.NullInt64
	depth                                                                  int
}

func queryDstNodesWindowRaw(ctx context.Context, conn *sql.DB, depth int, afterID string, window int) ([]dstKeysetWindowRow, error) {
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
	q := `SELECT n.id, n.service_id, n.parent_id, n.parent_service_id, n.path, n.parent_path, n.type, n.size, n.mtime, n.depth
FROM ` + tableDstNodes + ` n
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
		if err := rows.Scan(&r.id, &r.serviceID, &r.parentID, &r.parentServiceID, &r.path, &r.parentPath, &r.typ, &r.size, &r.mtime, &r.depth); err != nil {
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
FROM ` + tableIDMap + `
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

func querySrcChildrenByParentIDs(ctx context.Context, conn *sql.DB, parentIDs []string) (map[string][]*NodeState, error) {
	out := make(map[string][]*NodeState)
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
		q := `WITH ch AS (
	SELECT sn.id, sn.service_id, sn.parent_id, sn.parent_service_id, sn.path, sn.parent_path, sn.type, sn.size, sn.mtime, sn.depth, COALESCE(sn.gpl_state,'') AS gpl_state
	FROM ` + tableSrcNodes + ` sn
	WHERE sn.parent_id IN (` + strings.Join(ph, ",") + `)
),
trav AS (
	SELECT se.id, arg_max(se.traversal_status, se.event_time) AS traversal_status
	FROM src_status_events se
	INNER JOIN ch ON ch.id = se.id
	GROUP BY se.id
),
cpy AS (
	SELECT se.id, arg_max(se.copy_status, se.event_time) AS copy_status
	FROM src_status_events se
	INNER JOIN ch ON ch.id = se.id
	WHERE COALESCE(se.copy_status, '') <> ''
	GROUP BY se.id
),
del AS (
	SELECT se.id, arg_max(se.delete_status, se.event_time) AS delete_status
	FROM src_status_events se
	INNER JOIN ch ON ch.id = se.id
	WHERE COALESCE(se.delete_status, '') <> ''
	GROUP BY se.id
)
SELECT ch.id, ch.service_id, ch.parent_id, ch.parent_service_id, ch.path, ch.parent_path, ch.type, ch.size, ch.mtime, ch.depth,
	COALESCE(trav.traversal_status,'') AS traversal_status,
	COALESCE(cpy.copy_status,'') AS copy_status,
	COALESCE(del.delete_status,'') AS delete_status,
	(COALESCE(cpy.copy_status,'') IN ('excluded_explicit','excluded_inherited')) AS excluded,
	'' AS errors,
	ch.parent_id,
	ch.gpl_state
FROM ch
LEFT JOIN trav ON trav.id = ch.id
LEFT JOIN cpy ON cpy.id = ch.id
LEFT JOIN del ON del.id = ch.id
ORDER BY ch.parent_id, ch.id`
		rows, err := conn.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var node NodeState
			var size sql.NullInt64
			var parentID string
			if err := rows.Scan(&node.ID, &node.ServiceID, &node.ParentID, &node.ParentServiceID, &node.Path, &node.ParentPath, &node.Type, &size, &node.MTime, &node.Depth, &node.TraversalStatus, &node.CopyStatus, &node.DeleteStatus, &node.Excluded, &node.Errors, &parentID, &node.GPLState); err != nil {
				rows.Close()
				return nil, err
			}
			if size.Valid {
				node.Size = size.Int64
			}
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
			out[parentID] = append(out[parentID], &node)
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ListDstBatchWithSrcChildren returns the next batch of DST nodes at depth (keyset afterID, limit) and their SRC children (via reverse id_map → parent_id). Status from events, aggregated only for ids in each window / child set (not whole tables).
func ListDstBatchWithSrcChildren(d *DB, depth int, afterID string, limit int, traversalStatus string) ([]FetchResult, map[string][]*NodeState, error) {
	conn, err := d.GetDB()
	if err != nil {
		return nil, nil, err
	}
	ctx := context.Background()

	dstBatch := make([]FetchResult, 0, limit)
	srcParentByDstID := make(map[string]string)
	cursor := afterID

dstGather:
	for len(dstBatch) < limit {
		window, err := queryDstNodesWindowRaw(ctx, conn, depth, cursor, pullKeysetWindowSize)
		if err != nil {
			return nil, nil, err
		}
		if len(window) == 0 {
			break
		}
		ids := make([]string, len(window))
		for i := range window {
			ids[i] = window[i].id
		}
		travByID, err := latestDstTraversalByIDs(ctx, conn, ids)
		if err != nil {
			return nil, nil, err
		}
		reverseMap, err := batchReverseSrcIDFromDstIDs(ctx, conn, ids)
		if err != nil {
			return nil, nil, err
		}
		for i := range window {
			w := window[i]
			st := travByID[w.id]
			if traversalStatus != "" && st != traversalStatus {
				continue
			}
			ns := &NodeState{
				ID: w.id, ServiceID: w.serviceID, ParentID: w.parentID, ParentServiceID: w.parentServiceID,
				Path: w.path, ParentPath: w.parentPath, Type: w.typ, MTime: w.mtime, Depth: w.depth,
				TraversalStatus: st, CopyStatus: "", Excluded: false, Errors: "",
			}
			if w.size.Valid {
				ns.Size = w.size.Int64
			}
			ns.Status = ns.TraversalStatus
			ns.Name = ns.Path
			dstBatch = append(dstBatch, FetchResult{Key: w.id, State: ns})
			if depth == 0 {
				srcParentByDstID[w.id] = RootNodeID("SRC")
			} else if srcID := reverseMap[w.id]; srcID != "" {
				srcParentByDstID[w.id] = srcID
			}
			if len(dstBatch) == limit {
				break dstGather
			}
		}
		cursor = window[len(window)-1].id
		if len(window) < pullKeysetWindowSize {
			break
		}
	}

	childrenByDstID := make(map[string][]*NodeState)
	if len(dstBatch) == 0 {
		return dstBatch, childrenByDstID, nil
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
		return nil, nil, err
	}
	for _, fr := range dstBatch {
		srcParentID := srcParentByDstID[fr.Key]
		if srcParentID == "" {
			continue
		}
		if ch := byParentID[srcParentID]; len(ch) > 0 {
			childrenByDstID[fr.Key] = ch
		}
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
	normalizedPaths := make([]string, len(parentPaths))
	for i, p := range parentPaths {
		normalizedPaths[i] = NormalizeRootRelativePath(p)
	}
	nodeAlias, e, cte := statusJoinExpr("SRC")
	sel := `SELECT ` + nodeAlias + `.parent_path, ` + nodeAlias + `.id, ` + nodeAlias + `.service_id, ` + nodeAlias + `.parent_id, ` + nodeAlias + `.parent_service_id, ` + nodeAlias + `.path, ` + nodeAlias + `.type, ` + nodeAlias + `.size, ` + nodeAlias + `.mtime, ` + nodeAlias + `.depth, COALESCE(` + e + `.traversal_status,'') AS traversal_status, COALESCE(` + e + `.copy_status,'') AS copy_status, COALESCE(` + e + `.delete_status,'') AS delete_status, (COALESCE(` + e + `.copy_status,'') IN ('excluded_explicit','excluded_inherited')) AS excluded, '' AS errors FROM ` + tableSrcNodes + ` ` + nodeAlias + ` LEFT JOIN ` + cte + ` ` + e + ` ON ` + nodeAlias + `.id = ` + e + `.id WHERE ` + nodeAlias + `.parent_path IN (`
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
			var node NodeState
			var parentPath string
			var size sql.NullInt64
			if err := rows.Scan(&parentPath, &node.ID, &node.ServiceID, &node.ParentID, &node.ParentServiceID, &node.Path, &node.Type, &size, &node.MTime, &node.Depth, &node.TraversalStatus, &node.CopyStatus, &node.DeleteStatus, &node.Excluded, &node.Errors); err != nil {
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

// GetDstIDFromSrcID returns the DST node id mapped to the given SRC node id via id_map (path fallback when unmapped).
func GetDstIDFromSrcID(d *DB, srcParentID string) (string, error) {
	conn, err := d.GetDB()
	if err != nil {
		return "", err
	}
	ctx := context.Background()
	var dstID sql.NullString
	err = conn.QueryRowContext(ctx,
		`SELECT arg_max(dst_internal_id, event_time) FROM `+tableIDMap+` WHERE src_internal_id = $1 AND status = 'active'`,
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
	placeholders := make([]string, len(srcIDs))
	args := make([]any, len(srcIDs))
	for i := range srcIDs {
		placeholders[i] = "$" + strconv.Itoa(i+1)
		args[i] = srcIDs[i]
	}
	q := `SELECT src_internal_id, arg_max(dst_internal_id, event_time) AS dst_internal_id
FROM ` + tableIDMap + `
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
	qSrc := "SELECT id, path FROM " + tableSrcNodes + " WHERE id IN (" + strings.Join(placeholders, ",") + ")"
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
	args := make([]any, len(ids))
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
		if err := rows.Scan(&n.ID, &n.ServiceID, &n.ParentID, &n.ParentServiceID, &n.Path, &n.ParentPath, &n.Type, &size, &n.MTime, &n.Depth, &n.TraversalStatus, &n.CopyStatus, &n.DeleteStatus, &n.Excluded, &n.Errors); err != nil {
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
// Asking for CopyStatusSuccessful also returns already_existed (copy-complete for cleanup).
func ListSrcNodesByCopyStatus(d *DB, copyStatus string, limit, offset int) ([]NodeState, error) {
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
	base := selectNodeColsWithStatus("SRC")
	var rows *sql.Rows
	if copyStatus == CopyStatusSuccessful {
		q := base + ` WHERE COALESCE(e.copy_status,'') IN ` + SQLCopyStatusCompleteIN + ` ORDER BY n.path LIMIT $1 OFFSET $2`
		rows, err = conn.QueryContext(ctx, q, limit, offset)
	} else {
		q := base + ` WHERE COALESCE(e.copy_status,'') = $1 ORDER BY n.path LIMIT $2 OFFSET $3`
		rows, err = conn.QueryContext(ctx, q, copyStatus, limit, offset)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []NodeState
	var size sql.NullInt64
	for rows.Next() {
		var n NodeState
		if err := rows.Scan(&n.ID, &n.ServiceID, &n.ParentID, &n.ParentServiceID, &n.Path, &n.ParentPath, &n.Type, &size, &n.MTime, &n.Depth, &n.TraversalStatus, &n.CopyStatus, &n.DeleteStatus, &n.Excluded, &n.Errors); err != nil {
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
