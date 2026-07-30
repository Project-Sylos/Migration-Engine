// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"context"
	"database/sql"
	"strconv"
	"strings"
)

// QueryNodesForReview returns nodes from the given table (SRC or DST) with optional depth, status, excluded, pathLike filters. Status from events. Used by review API.
func QueryNodesForReview(d *db.DB, table string, depth *int, status string, excluded *bool, pathLike string, orderByPath bool, limit, offset int) ([]db.NodeState, error) {
	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	nodeAlias, e, _ := db.StatusJoinExpr(table)
	ctx := context.Background()
	base := db.SelectNodeColsWithStatus(table) + ` WHERE 1=1`
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

const cteActiveIDMap = `(SELECT src_internal_id, dst_internal_id
	FROM ` + db.TableIDMap + `
	WHERE status = 'active')`

// MergedReviewQueryBase returns the WITH clause for the merged src+dst review view.
// Status comes from src_current / dst_current (not event replay). SRC↔DST pairing uses id_map with path fallback.
func MergedReviewQueryBase() string {
	return `WITH idmap AS ` + cteActiveIDMap + `,
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
	COALESCE(NULLIF(s.name, ''), NULLIF(d.name, ''), '') AS name,
	COALESCE(se.resolved_dst_name, '') AS resolved_dst_name
FROM src_nodes s
LEFT JOIN ` + db.TableSrcCurrent + ` se ON s.id = se.id
LEFT JOIN idmap im ON im.src_internal_id = s.id
LEFT JOIN dst_nodes d ON (
	(im.dst_internal_id IS NOT NULL AND d.id = im.dst_internal_id)
	OR (im.dst_internal_id IS NULL AND d.path = s.path)
)
LEFT JOIN ` + db.TableDstCurrent + ` de ON d.id = de.id

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
	COALESCE(d.name, '') AS name,
	'' AS resolved_dst_name
FROM dst_nodes d
LEFT JOIN ` + db.TableDstCurrent + ` de ON d.id = de.id
WHERE NOT EXISTS (
	SELECT 1 FROM src_nodes s
	LEFT JOIN idmap im ON im.src_internal_id = s.id
	WHERE (im.dst_internal_id IS NOT NULL AND im.dst_internal_id = d.id)
	   OR (im.dst_internal_id IS NULL AND s.path = d.path)
)
)`
}

// mergedReviewQueryBaseParentFolder is the same logical merged view as MergedReviewQueryBase, but restricted to direct
// children of parent_path = $1 (normalized).
func mergedReviewQueryBaseParentFolder() string {
	return `WITH folder_src AS (
	SELECT * FROM src_nodes WHERE parent_path = $1
),
folder_dst_children AS (
	SELECT * FROM dst_nodes WHERE parent_path = $1
),
idmap AS ` + cteActiveIDMap + `,
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
	COALESCE(NULLIF(s.name, ''), NULLIF(d.name, ''), '') AS name,
	COALESCE(se.resolved_dst_name, '') AS resolved_dst_name
FROM folder_src s
LEFT JOIN ` + db.TableSrcCurrent + ` se ON s.id = se.id
LEFT JOIN idmap im ON im.src_internal_id = s.id
LEFT JOIN dst_nodes d ON (
	(im.dst_internal_id IS NOT NULL AND d.id = im.dst_internal_id)
	OR (im.dst_internal_id IS NULL AND d.path = s.path)
)
LEFT JOIN ` + db.TableDstCurrent + ` de ON d.id = de.id

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
	COALESCE(d.name, '') AS name,
	'' AS resolved_dst_name
FROM folder_dst_children d
LEFT JOIN ` + db.TableDstCurrent + ` de ON d.id = de.id
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
	//   "issues"   — all active naming issues (pending + manual_review)
	//   "manual"   — needs manual rename (manual_review)
	//   "accepted" — accepted sparse issue rows (and not ignored)
	//   "rejected" — gpl_status ignored (warnings dismissed)
	//   "none"     — no active/accepted/rejected compat footprint
	PathIssueFilter string
	// PathIssueCategory narrows sparse GPL issue JSON by category.
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
	FROM gpl_issues gi
	LEFT JOIN src_current g ON g.id = gi.src_id
	WHERE gi.src_id = src_node_id
	  AND gi.status IN ('pending', 'manual_review')
	  AND COALESCE(g.gpl_status, '') <> 'ignored'
)`

// pathIssueSuggestionsSQL matches auto-fixable / collision suggestions only (not manual_review).
const pathIssueSuggestionsSQL = `src_node_id <> '' AND EXISTS (
	SELECT 1
	FROM gpl_issues gi
	LEFT JOIN src_current g ON g.id = gi.src_id
	WHERE gi.src_id = src_node_id
	  AND gi.status = 'pending'
	  AND COALESCE(g.gpl_status, '') <> 'ignored'
)`

const pathIssueAcceptedSQL = `src_node_id <> '' AND EXISTS (
	SELECT 1
	FROM gpl_issues gi
	LEFT JOIN src_current g ON g.id = gi.src_id
	WHERE gi.src_id = src_node_id
	  AND gi.status = 'accepted'
	  AND COALESCE(g.gpl_status, '') <> 'ignored'
)`

const pathIssueRejectedSQL = `src_node_id <> '' AND EXISTS (
	SELECT 1 FROM src_current g
	WHERE g.id = src_node_id
	  AND COALESCE(g.gpl_status, '') = 'ignored'
)`

const pathIssueManualSQL = `src_node_id <> '' AND EXISTS (
	SELECT 1
	FROM gpl_issues gi
	LEFT JOIN src_current g ON g.id = gi.src_id
	WHERE gi.src_id = src_node_id
	  AND gi.status = 'manual_review'
	  AND COALESCE(g.gpl_status, '') <> 'ignored'
)`

func appendPathIssueFilterClause(parts []string, value string) []string {
	v := strings.ToLower(strings.TrimSpace(value))
	switch v {
	case "issues", "active":
		return append(parts, pathIssueAnyActiveSQL)
	case "pending", "suggestions":
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
	// Match `"category":"InvalidChar"` (and similar) inside sparse issue JSON.
	needle := `"category":"` + cat + `"`
	parts = append(parts, `src_node_id <> '' AND EXISTS (
	SELECT 1
	FROM gpl_issues gi
	LEFT JOIN src_current g ON g.id = gi.src_id
	WHERE gi.src_id = src_node_id
	  AND gi.status IN ('pending', 'manual_review')
	  AND COALESCE(g.gpl_status, '') <> 'ignored'
	  AND strpos(COALESCE(gi.issues_json, ''), $`+strconv.Itoa(param)+`) > 0
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
		args = append(args, db.NormalizeRootRelativePath(f.ParentPath))
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

// ReviewFilterHasSearchPredicate reports whether f carries any user search predicate.
// ParentPath and ExcludeRoot are scoping/defaults for tree vs global search and do not count.
// StatusSearchType alone does not count without a status value.
func ReviewFilterHasSearchPredicate(f ReviewFilter) bool {
	if strings.TrimSpace(f.Query) != "" {
		return true
	}
	if f.FoldersOnly || strings.TrimSpace(f.TypeFilter) != "" {
		return true
	}
	if strings.TrimSpace(f.TraversalStatus) != "" ||
		strings.TrimSpace(f.CopyStatus) != "" ||
		strings.TrimSpace(f.DeleteStatus) != "" {
		return true
	}
	if strings.TrimSpace(f.PathIssueFilter) != "" || strings.TrimSpace(f.PathIssueCategory) != "" {
		return true
	}
	if f.DepthValue != nil && strings.TrimSpace(f.DepthOperator) != "" {
		return true
	}
	if f.SizeValue != nil && strings.TrimSpace(f.SizeOperator) != "" {
		return true
	}
	if f.ExcludeDestinationOnly {
		return true
	}
	return false
}

func scanMergedReviewRow(rows *sql.Rows) (MergedReviewRow, error) {
	var (
		r        MergedReviewRow
		path     sql.NullString
		name     sql.NullString
		depth    sql.NullInt64
		typ      sql.NullString
		srcID    sql.NullString
		dstID    sql.NullString
		srcTrav  sql.NullString
		dstTrav  sql.NullString
		copySt   sql.NullString
		delSt    sql.NullString
		excluded sql.NullBool
		size     sql.NullInt64
		resolved sql.NullString
	)
	if err := rows.Scan(
		&path, &name, &depth, &typ, &srcID, &dstID, &srcTrav, &dstTrav,
		&copySt, &delSt, &excluded, &size, &resolved,
	); err != nil {
		return MergedReviewRow{}, err
	}
	r.Path = path.String
	r.Name = name.String
	if depth.Valid {
		r.Depth = int(depth.Int64)
	}
	r.Type = typ.String
	r.SrcNodeID = srcID.String
	r.DstNodeID = dstID.String
	r.SrcTraversalStatus = srcTrav.String
	r.DstTraversalStatus = dstTrav.String
	r.CopyStatus = copySt.String
	r.DeleteStatus = delSt.String
	r.Excluded = excluded.Valid && excluded.Bool
	if size.Valid {
		r.Size = size.Int64
	}
	r.ResolvedDstName = resolved.String
	return r, nil
}

// ListMergedReviewDiffs returns merged review rows and total count matching the filter, ordered and paginated.
// Runs as one statement: filtered merge is computed once, counted, then LIMITed (avoids rebuilding the CTE twice).
// Use for tree diffs and any caller that needs an exact total. Search uses ListMergedReviewDiffsPage instead.
func ListMergedReviewDiffs(d *db.DB, f ReviewFilter, orderBy string, limit, offset int) ([]MergedReviewRow, int, error) {
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
	limitParam := strconv.Itoa(len(args) + 1)
	offsetParam := strconv.Itoa(len(args) + 2)
	// counted CROSS/LEFT pattern: empty page still returns total_count (offset past end).
	q := base + `,
filtered AS (
	SELECT path, name, depth, type, src_node_id, dst_node_id, src_traversal_status, dst_traversal_status,
		copy_status, delete_status, excluded, size, resolved_dst_name
	FROM merged` + where + `
),
counted AS (
	SELECT COUNT(*)::INT AS total_count FROM filtered
),
page AS (
	SELECT * FROM filtered
	ORDER BY ` + orderBy + `
	LIMIT $` + limitParam + ` OFFSET $` + offsetParam + `
)
SELECT p.path, p.name, p.depth, p.type, p.src_node_id, p.dst_node_id, p.src_traversal_status, p.dst_traversal_status,
	p.copy_status, p.delete_status, p.excluded, p.size, p.resolved_dst_name, c.total_count
FROM counted c
LEFT JOIN page p ON TRUE`
	listArgs := append(append([]any{}, args...), limit, offset)
	rows, err := conn.QueryContext(ctx, q, listArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []MergedReviewRow
	total := 0
	for rows.Next() {
		var (
			r          MergedReviewRow
			path       sql.NullString
			name       sql.NullString
			depth      sql.NullInt64
			typ        sql.NullString
			srcID      sql.NullString
			dstID      sql.NullString
			srcTrav    sql.NullString
			dstTrav    sql.NullString
			copySt     sql.NullString
			delSt      sql.NullString
			excluded   sql.NullBool
			size       sql.NullInt64
			resolved   sql.NullString
			totalCount int
		)
		if err := rows.Scan(
			&path, &name, &depth, &typ, &srcID, &dstID, &srcTrav, &dstTrav,
			&copySt, &delSt, &excluded, &size, &resolved, &totalCount,
		); err != nil {
			return nil, 0, err
		}
		total = totalCount
		if !path.Valid {
			// Sentinel row when the page is empty but counted still has a total.
			continue
		}
		r.Path = path.String
		r.Name = name.String
		if depth.Valid {
			r.Depth = int(depth.Int64)
		}
		r.Type = typ.String
		r.SrcNodeID = srcID.String
		r.DstNodeID = dstID.String
		r.SrcTraversalStatus = srcTrav.String
		r.DstTraversalStatus = dstTrav.String
		r.CopyStatus = copySt.String
		r.DeleteStatus = delSt.String
		r.Excluded = excluded.Valid && excluded.Bool
		if size.Valid {
			r.Size = size.Int64
		}
		r.ResolvedDstName = resolved.String
		out = append(out, r)
	}
	return out, total, rows.Err()
}

// CountMergedReviewRows returns the number of merged rows matching the filter.
func CountMergedReviewRows(d *db.DB, f ReviewFilter) (int, error) {
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
	SizeSelected    int64 // sum of SRC file sizes with copy_status pending (selected to copy)
}

// GetMergedReviewStats returns aggregate counts for rows matching the filter (single query with FILTER). Counts are unique by path (merged view = one row per path).
func GetMergedReviewStats(d *db.DB, f ReviewFilter) (MergedReviewStats, error) {
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
		COALESCE(SUM(dst_size) FILTER (WHERE type = 'file'), 0)::BIGINT,
		COALESCE(SUM(src_size) FILTER (WHERE type = 'file' AND NOT excluded AND LOWER(COALESCE(copy_status,'')) IN ('pending','')), 0)::BIGINT
	FROM merged` + where
	var s MergedReviewStats
	err = conn.QueryRowContext(context.Background(), q, args...).Scan(
		&s.Total, &s.Folders, &s.Files, &s.MissingOnSource, &s.MissingOnDest, &s.Excluded, &s.SizeSrc, &s.SizeDst, &s.SizeSelected,
	)
	return s, err
}

// GetTotalFileSizes returns the sum of size across src_nodes and dst_nodes (for API totalFileSize).
func GetTotalFileSizes(d *db.DB) (srcTotal, dstTotal int64, err error) {
	conn, err := d.GetDB()
	if err != nil {
		return 0, 0, err
	}
	ctx := context.Background()
	if err := conn.QueryRowContext(ctx, `SELECT COALESCE(SUM(size), 0) FROM `+db.TableSrcNodes).Scan(&srcTotal); err != nil {
		return 0, 0, err
	}
	if err := conn.QueryRowContext(ctx, `SELECT COALESCE(SUM(size), 0) FROM `+db.TableDstNodes).Scan(&dstTotal); err != nil {
		return 0, 0, err
	}
	return srcTotal, dstTotal, nil
}

// CountFolderFileSizeTotals returns SRC folder/file counts and SRC/DST file size sums.
// Cheap O(1) aggregates over node tables (no merged join). Used to backfill review stats keys.
func CountFolderFileSizeTotals(d *db.DB) (folders, files, sizeSrc, sizeDst int64, err error) {
	conn, err := d.GetDB()
	if err != nil {
		return 0, 0, 0, 0, err
	}
	ctx := context.Background()
	err = conn.QueryRowContext(ctx, `
SELECT
  COUNT(*) FILTER (WHERE type = 'folder')::BIGINT,
  COUNT(*) FILTER (WHERE type = 'file')::BIGINT,
  COALESCE(SUM(CASE WHEN type = 'file' THEN size ELSE 0 END), 0)::BIGINT
FROM `+db.TableSrcNodes).Scan(&folders, &files, &sizeSrc)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	err = conn.QueryRowContext(ctx, `
SELECT COALESCE(SUM(CASE WHEN type = 'file' THEN size ELSE 0 END), 0)::BIGINT
FROM `+db.TableDstNodes).Scan(&sizeDst)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	return folders, files, sizeSrc, sizeDst, nil
}
