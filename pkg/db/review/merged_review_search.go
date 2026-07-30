// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// ListMergedReviewDiffsPage returns a page of merged review rows without COUNT(*).
// Search hot path: scan SRC and DST independently, late-join via id_map, then
// sorted-merge (zipper) in Go. Avoids building the full SRC⨝DST merge CTE.
// Fetches offset+limit+1 from each side so early pages stay correct; hasMore uses limit+1.
func ListMergedReviewDiffsPage(d *db.DB, f ReviewFilter, orderBy string, limit, offset int) ([]MergedReviewRow, bool, error) {
	conn, err := d.GetDB()
	if err != nil {
		return nil, false, err
	}
	if limit <= 0 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	if orderBy == "" {
		orderBy = "path ASC"
	}
	sortCol, sortDesc := parseReviewOrderBy(orderBy)
	fetch := offset + limit + 1
	ctx := context.Background()

	srcRows, err := queryReviewSearchSRC(ctx, conn, f, sortCol, sortDesc, fetch)
	if err != nil {
		return nil, false, err
	}

	var dstOnly []MergedReviewRow
	if reviewSearchIncludeDSTOnly(f) {
		dstOnly, err = queryReviewSearchDSTOnly(ctx, conn, f, sortCol, sortDesc, fetch)
		if err != nil {
			return nil, false, err
		}
	}

	merged := mergeReviewSearchRows(srcRows, dstOnly, sortCol, sortDesc)
	if offset >= len(merged) {
		return nil, false, nil
	}
	merged = merged[offset:]
	hasMore := len(merged) > limit
	if hasMore {
		merged = merged[:limit]
	}
	return merged, hasMore, nil
}

// reviewSearchIncludeDSTOnly reports whether the DST-only scan can contribute rows.
// Path-issue / copy / delete filters are SRC-native; not_on_src is DST-only (still included).
func reviewSearchIncludeDSTOnly(f ReviewFilter) bool {
	if f.ExcludeDestinationOnly {
		return false
	}
	if strings.TrimSpace(f.PathIssueFilter) != "" || strings.TrimSpace(f.PathIssueCategory) != "" {
		return false
	}
	if strings.TrimSpace(f.CopyStatus) != "" || strings.TrimSpace(f.DeleteStatus) != "" {
		// Copy/delete live on SRC events only; DST-only rows never match.
		st := strings.ToLower(strings.TrimSpace(f.StatusSearchType))
		if st == "" || st == "copy" || st == "delete" || st == "both" {
			if strings.TrimSpace(f.CopyStatus) != "" && (st == "" || st == "copy" || st == "both") {
				return false
			}
			if strings.TrimSpace(f.DeleteStatus) != "" && (st == "" || st == "delete" || st == "both") {
				return false
			}
		}
	}
	return true
}

func reviewSearchSRCImpossible(f ReviewFilter) bool {
	trav := strings.TrimSpace(f.TraversalStatus)
	if trav == "" {
		return false
	}
	st := strings.ToLower(strings.TrimSpace(f.StatusSearchType))
	needTrav := st == "" || st == "traversal" || st == "both"
	return needTrav && strings.EqualFold(trav, "not_on_src")
}

// parseReviewOrderBy reads the primary sort key out of an ORDER BY string such as
// "name DESC, path ASC". Secondary keys are dropped: the merge applies its own fixed
// tie-break, which both side queries mirror so the zipper stays valid.
func parseReviewOrderBy(orderBy string) (col string, desc bool) {
	primary := strings.TrimSpace(orderBy)
	if i := strings.IndexByte(primary, ','); i >= 0 {
		primary = primary[:i]
	}
	parts := strings.Fields(primary)
	col = "path"
	if len(parts) > 0 {
		switch strings.ToLower(parts[0]) {
		case "name", "depth", "size", "type", "path":
			col = strings.ToLower(parts[0])
		case "src_traversal_status", "traversal_status", "traversalstatus", "status":
			col = "src_traversal_status"
		case "copy_status", "copystatus":
			col = "copy_status"
		}
	}
	if len(parts) > 1 && strings.EqualFold(parts[1], "DESC") {
		desc = true
	}
	return col, desc
}

func orderDirection(desc bool) string {
	if desc {
		return " DESC"
	}
	return " ASC"
}

// srcReviewOrderClause and dstReviewOrderClause must both order by the same keys
// reviewRowLess compares, or the zipper cannot produce a globally sorted page.
// Tie-break is path ASC then id ASC on both sides; the SRC-bearing preference in
// reviewRowLess only ever decides cross-side comparisons.
func srcReviewOrderClause(col string, desc bool) string {
	terms := []string{srcReviewOrderExpr(col) + orderDirection(desc)}
	if col != "path" {
		terms = append(terms, `COALESCE(s.path, '') ASC`)
	}
	return strings.Join(append(terms, "s.id ASC"), ", ")
}

func dstReviewOrderClause(col string, desc bool) string {
	var terms []string
	// SRC-side columns project as a constant for DST-only rows, so they impose no order.
	if expr := dstReviewOrderExpr(col); expr != "" {
		terms = append(terms, expr+orderDirection(desc))
	}
	if col != "path" {
		terms = append(terms, `COALESCE(d.path, '') ASC`)
	}
	return strings.Join(append(terms, "d.id ASC"), ", ")
}

func queryReviewSearchSRC(ctx context.Context, conn *sql.DB, f ReviewFilter, sortCol string, sortDesc bool, limit int) ([]MergedReviewRow, error) {
	if reviewSearchSRCImpossible(f) {
		return nil, nil
	}
	where, args := buildSRCReviewSearchWhere(f)
	orderSQL := srcReviewOrderClause(sortCol, sortDesc)

	q := `
SELECT
	COALESCE(s.path, '') AS path,
	COALESCE(NULLIF(s.name, ''), '') AS name,
	COALESCE(s.depth, 0) AS depth,
	COALESCE(s.type, '') AS type,
	COALESCE(s.id, '') AS src_node_id,
	COALESCE(im.dst_internal_id, '') AS dst_node_id,
	COALESCE(se.traversal_status, '') AS src_traversal_status,
	COALESCE(de.traversal_status, '') AS dst_traversal_status,
	COALESCE(se.copy_status, '') AS copy_status,
	COALESCE(se.delete_status, '') AS delete_status,
	(COALESCE(se.copy_status,'') IN ('excluded_explicit','excluded_inherited')) AS excluded,
	COALESCE(s.size, 0) AS size,
	COALESCE(se.resolved_dst_name, '') AS resolved_dst_name
FROM ` + db.TableSrcNodes + ` s
LEFT JOIN ` + db.TableSrcCurrent + ` se ON se.id = s.id
LEFT JOIN (
	SELECT src_internal_id, arg_max(dst_internal_id, event_time) AS dst_internal_id
	FROM ` + db.TableIDMap + `
	WHERE status = 'active'
	GROUP BY src_internal_id
) im ON im.src_internal_id = s.id
LEFT JOIN ` + db.TableDstNodes + ` d ON d.id = im.dst_internal_id
LEFT JOIN ` + db.TableDstCurrent + ` de ON de.id = d.id
` + where + `
ORDER BY ` + orderSQL + `
LIMIT $` + strconv.Itoa(len(args)+1)

	listArgs := append(append([]any{}, args...), limit)
	rows, err := conn.QueryContext(ctx, q, listArgs...)
	if err != nil {
		return nil, fmt.Errorf("review search SRC: %w", err)
	}
	defer rows.Close()
	return scanMergedReviewRows(rows)
}

func queryReviewSearchDSTOnly(ctx context.Context, conn *sql.DB, f ReviewFilter, sortCol string, sortDesc bool, limit int) ([]MergedReviewRow, error) {
	where, args := buildDSTOnlyReviewSearchWhere(f)
	orderSQL := dstReviewOrderClause(sortCol, sortDesc)

	q := `
SELECT
	COALESCE(d.path, '') AS path,
	COALESCE(d.name, '') AS name,
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
	'' AS resolved_dst_name
FROM ` + db.TableDstNodes + ` d
LEFT JOIN ` + db.TableDstCurrent + ` de ON de.id = d.id
` + where + `
ORDER BY ` + orderSQL + `
LIMIT $` + strconv.Itoa(len(args)+1)

	listArgs := append(append([]any{}, args...), limit)
	rows, err := conn.QueryContext(ctx, q, listArgs...)
	if err != nil {
		return nil, fmt.Errorf("review search DST-only: %w", err)
	}
	defer rows.Close()
	return scanMergedReviewRows(rows)
}

func srcReviewOrderExpr(col string) string {
	switch col {
	case "name":
		return "COALESCE(NULLIF(s.name, ''), '')"
	case "depth":
		return "COALESCE(s.depth, 0)"
	case "size":
		return "COALESCE(s.size, 0)"
	case "type":
		return "COALESCE(s.type, '')"
	case "src_traversal_status":
		return "COALESCE(se.traversal_status, '')"
	case "copy_status":
		return "COALESCE(se.copy_status, '')"
	default:
		return "COALESCE(s.path, '')"
	}
}

// dstReviewOrderExpr returns "" when the sort column is constant for DST-only rows.
func dstReviewOrderExpr(col string) string {
	switch col {
	case "name":
		return "COALESCE(d.name, '')"
	case "depth":
		return "COALESCE(d.depth, 0)"
	case "size":
		return "COALESCE(d.size, 0)"
	case "type":
		return "COALESCE(d.type, '')"
	case "src_traversal_status", "copy_status":
		return ""
	default:
		return "COALESCE(d.path, '')"
	}
}

func scanMergedReviewRows(rows *sql.Rows) ([]MergedReviewRow, error) {
	var out []MergedReviewRow
	for rows.Next() {
		r, err := scanMergedReviewRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// mergeReviewSearchRows zipper-merges two pre-sorted streams by sortCol.
func mergeReviewSearchRows(src, dstOnly []MergedReviewRow, sortCol string, sortDesc bool) []MergedReviewRow {
	out := make([]MergedReviewRow, 0, len(src)+len(dstOnly))
	i, j := 0, 0
	for i < len(src) && j < len(dstOnly) {
		if reviewRowLess(src[i], dstOnly[j], sortCol, sortDesc) {
			out = append(out, src[i])
			i++
		} else {
			out = append(out, dstOnly[j])
			j++
		}
	}
	out = append(out, src[i:]...)
	out = append(out, dstOnly[j:]...)
	return out
}

func reviewRowLess(a, b MergedReviewRow, sortCol string, sortDesc bool) bool {
	if cmp := reviewRowCompare(a, b, sortCol); cmp != 0 {
		if sortDesc {
			return cmp > 0
		}
		return cmp < 0
	}
	// Tie-break, always ascending so it matches the side queries regardless of sort
	// direction: path, then SRC-bearing rows first, then ids. Path must outrank the
	// SRC preference so a tie group spanning both sides still reads in path order.
	if a.Path != b.Path {
		return a.Path < b.Path
	}
	if (a.SrcNodeID != "") != (b.SrcNodeID != "") {
		return a.SrcNodeID != ""
	}
	if a.SrcNodeID != b.SrcNodeID {
		return a.SrcNodeID < b.SrcNodeID
	}
	return a.DstNodeID < b.DstNodeID
}

func reviewRowCompare(a, b MergedReviewRow, sortCol string) int {
	switch sortCol {
	case "name":
		return strings.Compare(a.Name, b.Name)
	case "depth":
		return compareOrdered(a.Depth, b.Depth)
	case "size":
		return compareOrdered(a.Size, b.Size)
	case "type":
		return strings.Compare(a.Type, b.Type)
	case "src_traversal_status":
		return strings.Compare(a.SrcTraversalStatus, b.SrcTraversalStatus)
	case "copy_status":
		return strings.Compare(a.CopyStatus, b.CopyStatus)
	default:
		return strings.Compare(a.Path, b.Path)
	}
}

func compareOrdered[T ~int | ~int64](a, b T) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func buildSRCReviewSearchWhere(f ReviewFilter) (clause string, args []any) {
	var parts []string
	param := 1

	if f.ParentPath != "" {
		parts = append(parts, `s.parent_path = $`+strconv.Itoa(param))
		args = append(args, db.NormalizeRootRelativePath(f.ParentPath))
		param++
	}
	if f.Query != "" {
		q := "%" + strings.ToLower(strings.TrimSpace(f.Query)) + "%"
		switch strings.ToLower(strings.TrimSpace(f.QueryField)) {
		case "path":
			parts = append(parts, `LOWER(s.path) LIKE $`+strconv.Itoa(param))
			args = append(args, q)
			param++
		case "name":
			parts = append(parts, `LOWER(COALESCE(s.name, '')) LIKE $`+strconv.Itoa(param))
			args = append(args, q)
			param++
		default:
			parts = append(parts, `(LOWER(s.path) LIKE $`+strconv.Itoa(param)+` OR LOWER(COALESCE(s.name, '')) LIKE $`+strconv.Itoa(param)+`)`)
			args = append(args, q)
			param++
		}
	}

	parts, args, param = appendSRCStatusFilters(parts, args, param, f)

	if f.FoldersOnly || strings.EqualFold(f.TypeFilter, "folder") {
		parts = append(parts, `s.type = 'folder'`)
	} else if strings.EqualFold(f.TypeFilter, "file") {
		parts = append(parts, `s.type = 'file'`)
	}
	if f.ExcludeRoot {
		parts = append(parts, `s.path <> '/'`)
	}

	if strings.TrimSpace(f.PathIssueFilter) != "" {
		parts = appendPathIssueFilterClauseSRC(parts, f.PathIssueFilter)
	}
	if strings.TrimSpace(f.PathIssueCategory) != "" {
		parts, args, param = appendPathIssueCategoryClauseSRC(parts, args, param, f.PathIssueCategory)
	}

	parts, args, param = appendDepthSizeFilters(parts, args, param, f, "s")

	if len(parts) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(parts, " AND "), args
}

func buildDSTOnlyReviewSearchWhere(f ReviewFilter) (clause string, args []any) {
	var parts []string
	param := 1

	// Not actively mapped from any SRC — true destination-only rows.
	parts = append(parts, `NOT EXISTS (
	SELECT 1 FROM `+db.TableIDMap+` im
	WHERE im.dst_internal_id = d.id AND im.status = 'active'
)`)

	if f.ParentPath != "" {
		parts = append(parts, `d.parent_path = $`+strconv.Itoa(param))
		args = append(args, db.NormalizeRootRelativePath(f.ParentPath))
		param++
	}
	if f.Query != "" {
		q := "%" + strings.ToLower(strings.TrimSpace(f.Query)) + "%"
		switch strings.ToLower(strings.TrimSpace(f.QueryField)) {
		case "path":
			parts = append(parts, `LOWER(d.path) LIKE $`+strconv.Itoa(param))
			args = append(args, q)
			param++
		case "name":
			parts = append(parts, `LOWER(COALESCE(d.name, '')) LIKE $`+strconv.Itoa(param))
			args = append(args, q)
			param++
		default:
			parts = append(parts, `(LOWER(d.path) LIKE $`+strconv.Itoa(param)+` OR LOWER(COALESCE(d.name, '')) LIKE $`+strconv.Itoa(param)+`)`)
			args = append(args, q)
			param++
		}
	}

	parts, args, param = appendDSTStatusFilters(parts, args, param, f)

	if f.FoldersOnly || strings.EqualFold(f.TypeFilter, "folder") {
		parts = append(parts, `d.type = 'folder'`)
	} else if strings.EqualFold(f.TypeFilter, "file") {
		parts = append(parts, `d.type = 'file'`)
	}
	if f.ExcludeRoot {
		parts = append(parts, `d.path <> '/'`)
	}

	parts, args, param = appendDepthSizeFilters(parts, args, param, f, "d")

	return " WHERE " + strings.Join(parts, " AND "), args
}

func appendSRCStatusFilters(parts []string, args []any, param int, f ReviewFilter) ([]string, []any, int) {
	if !reviewFilterUsesStructuredStatus(f) {
		return parts, args, param
	}
	st := strings.ToLower(strings.TrimSpace(f.StatusSearchType))
	trav := strings.TrimSpace(f.TraversalStatus)
	copySt := strings.TrimSpace(f.CopyStatus)
	delSt := strings.TrimSpace(f.DeleteStatus)

	needTrav := trav != "" && (st == "" || st == "traversal" || st == "both")
	needCopy := copySt != "" && (st == "" || st == "copy" || st == "both")
	needDelete := delSt != "" && (st == "" || st == "delete" || st == "both")

	if needTrav {
		if strings.EqualFold(trav, "not_on_src") {
			// Handled by skipping SRC query entirely.
			parts = append(parts, `0=1`)
		} else {
			// Match if SRC or paired DST has the traversal status (same as old merged OR).
			parts = append(parts, `(LOWER(COALESCE(se.traversal_status,'')) = LOWER($`+strconv.Itoa(param)+`) OR LOWER(COALESCE(de.traversal_status,'')) = LOWER($`+strconv.Itoa(param+1)+`))`)
			args = append(args, trav, trav)
			param += 2
		}
	}
	if needCopy {
		parts, args, param = appendCopyStatusClauseSide(parts, args, param, copySt, "se")
	}
	if needDelete {
		parts, args, param = appendDeleteStatusClauseSide(parts, args, param, delSt, "se")
	}
	return parts, args, param
}

func appendDSTStatusFilters(parts []string, args []any, param int, f ReviewFilter) ([]string, []any, int) {
	if !reviewFilterUsesStructuredStatus(f) {
		return parts, args, param
	}
	st := strings.ToLower(strings.TrimSpace(f.StatusSearchType))
	trav := strings.TrimSpace(f.TraversalStatus)
	needTrav := trav != "" && (st == "" || st == "traversal" || st == "both")
	if !needTrav {
		return parts, args, param
	}
	parts = append(parts, `LOWER(COALESCE(de.traversal_status,'')) = LOWER($`+strconv.Itoa(param)+`)`)
	args = append(args, trav)
	param++
	return parts, args, param
}

func appendCopyStatusClauseSide(parts []string, args []any, param int, value, alias string) ([]string, []any, int) {
	v := strings.TrimSpace(value)
	if v == "" {
		return parts, args, param
	}
	if strings.EqualFold(v, "excluded") {
		parts = append(parts, `(LOWER(COALESCE(`+alias+`.copy_status,'')) IN ('excluded_explicit','excluded_inherited'))`)
		return parts, args, param
	}
	if strings.EqualFold(v, "successful") {
		parts = append(parts, `LOWER(COALESCE(`+alias+`.copy_status,'')) IN ('successful','already_existed')`)
		return parts, args, param
	}
	parts = append(parts, `LOWER(COALESCE(`+alias+`.copy_status,'')) = LOWER($`+strconv.Itoa(param)+`)`)
	args = append(args, v)
	param++
	return parts, args, param
}

func appendDeleteStatusClauseSide(parts []string, args []any, param int, value, alias string) ([]string, []any, int) {
	v := strings.TrimSpace(value)
	if v == "" {
		return parts, args, param
	}
	if strings.EqualFold(v, "excluded") || strings.EqualFold(v, "skipped") {
		parts = append(parts, `LOWER(COALESCE(`+alias+`.delete_status,'')) = 'skipped'`)
		return parts, args, param
	}
	parts = append(parts, `LOWER(COALESCE(`+alias+`.delete_status,'')) = LOWER($`+strconv.Itoa(param)+`)`)
	args = append(args, v)
	param++
	return parts, args, param
}

func appendDepthSizeFilters(parts []string, args []any, param int, f ReviewFilter, alias string) ([]string, []any, int) {
	if f.DepthValue != nil && f.DepthOperator != "" {
		op := sqlCompareOp(f.DepthOperator)
		parts = append(parts, alias+`.depth `+op+` $`+strconv.Itoa(param))
		args = append(args, *f.DepthValue)
		param++
	}
	if f.SizeValue != nil && f.SizeOperator != "" {
		op := sqlCompareOp(f.SizeOperator)
		parts = append(parts, `COALESCE(`+alias+`.size,0) `+op+` $`+strconv.Itoa(param))
		args = append(args, *f.SizeValue)
		param++
	}
	return parts, args, param
}

func sqlCompareOp(op string) string {
	switch strings.ToLower(strings.TrimSpace(op)) {
	case "gt", ">":
		return ">"
	case "gte", ">=":
		return ">="
	case "lt", "<":
		return "<"
	case "lte", "<=":
		return "<="
	default:
		return "="
	}
}

// Path-issue clauses keyed on s.id (SRC search), not merged src_node_id.
const pathIssueAnyActiveSQLSRC = `EXISTS (
	SELECT 1
	FROM gpl_issues gi
	LEFT JOIN src_current g ON g.id = gi.src_id
	WHERE gi.src_id = s.id
	  AND gi.status IN ('pending', 'manual_review')
	  AND COALESCE(g.gpl_status, '') <> 'ignored'
)`

const pathIssueSuggestionsSQLSRC = `EXISTS (
	SELECT 1
	FROM gpl_issues gi
	LEFT JOIN src_current g ON g.id = gi.src_id
	WHERE gi.src_id = s.id
	  AND gi.status = 'pending'
	  AND COALESCE(g.gpl_status, '') <> 'ignored'
)`

const pathIssueAcceptedSQLSRC = `EXISTS (
	SELECT 1
	FROM gpl_issues gi
	LEFT JOIN src_current g ON g.id = gi.src_id
	WHERE gi.src_id = s.id
	  AND gi.status = 'accepted'
	  AND COALESCE(g.gpl_status, '') <> 'ignored'
)`

const pathIssueRejectedSQLSRC = `EXISTS (
	SELECT 1 FROM src_current g
	WHERE g.id = s.id
	  AND COALESCE(g.gpl_status, '') = 'ignored'
)`

const pathIssueManualSQLSRC = `EXISTS (
	SELECT 1
	FROM gpl_issues gi
	LEFT JOIN src_current g ON g.id = gi.src_id
	WHERE gi.src_id = s.id
	  AND gi.status = 'manual_review'
	  AND COALESCE(g.gpl_status, '') <> 'ignored'
)`

func appendPathIssueFilterClauseSRC(parts []string, value string) []string {
	v := strings.ToLower(strings.TrimSpace(value))
	switch v {
	case "issues", "active":
		return append(parts, pathIssueAnyActiveSQLSRC)
	case "pending", "suggestions":
		return append(parts, pathIssueSuggestionsSQLSRC)
	case "accepted":
		return append(parts, pathIssueAcceptedSQLSRC)
	case "rejected", "ignored":
		return append(parts, pathIssueRejectedSQLSRC)
	case "manual", "manual_review":
		return append(parts, pathIssueManualSQLSRC)
	case "none", "clean":
		return append(parts, `NOT (`+pathIssueAnyActiveSQLSRC+`) AND NOT (`+pathIssueAcceptedSQLSRC+`) AND NOT (`+pathIssueRejectedSQLSRC+`)`)
	default:
		return parts
	}
}

func appendPathIssueCategoryClauseSRC(parts []string, args []any, param int, category string) ([]string, []any, int) {
	cat := sanitizePathIssueCategory(category)
	if cat == "" {
		return parts, args, param
	}
	needle := `"category":"` + cat + `"`
	parts = append(parts, `EXISTS (
	SELECT 1
	FROM gpl_issues gi
	LEFT JOIN src_current g ON g.id = gi.src_id
	WHERE gi.src_id = s.id
	  AND gi.status IN ('pending', 'manual_review')
	  AND COALESCE(g.gpl_status, '') <> 'ignored'
	  AND strpos(COALESCE(gi.issues_json, ''), $`+strconv.Itoa(param)+`) > 0
)`)
	args = append(args, needle)
	param++
	return parts, args, param
}
