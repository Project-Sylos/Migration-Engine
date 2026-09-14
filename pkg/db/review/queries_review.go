// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"context"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/filter"
)

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
	// DstSize is the destination folder's identity child size when HasDstSize is set.
	DstSize            int64
	HasDstSize         bool
	// ResolvedDstName is the accepted/committed destination basename from path_events (empty if none).
	ResolvedDstName string
}

// ReviewFilter narrows merged review rows for listing, search, and counts.
// ParentPath: if non-empty, only direct children of this path (uses normalized parent_path / id_path).
// UnderPath: if non-empty and not "/", the folder and all descendants (path prefix / starts_with).
// Query + QueryField: substring match on basename when QueryField is "name".
// PathSegments: ordered display-name segments for "in path" search (contains, gaps allowed).
// When PathSegments is non-empty, path LIKE on id_path is not used.
// StatusSearchType + TraversalStatus + CopyStatus filter merged review rows ("traversal", "copy", "both").
// TypeFilter: "folder" or "file" (case-insensitive); also FoldersOnly implies folder.
// ExcludeRoot: if true, exclude path = '/' from results (for global search).
// ExcludeDestinationOnly: if true, omit rows with no SRC node (destination-only / not_on_src).
type ReviewFilter struct {
	ParentPath string
	// UnderPath scopes search to a subtree (folder + descendants). Empty or "/" means global.
	UnderPath  string
	Query      string
	QueryField string
	// PathSegments are ordered basename contains-filters for ancestry-aware path search.
	PathSegments []string
	FoldersOnly  bool
	ExcludeRoot  bool

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

	// AfterPath / AfterID are keyset pagination cursors (path ASC, id ASC). When set, OFFSET is ignored.
	AfterPath string
	AfterID   string

	// PathIssueFilter narrows by destination naming / compatibility review state:
	//   "issues"   — all active naming issues (pending + manual_review)
	//   "manual"   — needs manual rename (manual_review)
	//   "accepted" — accepted sparse issue rows (and not ignored)
	//   "rejected" — gpl_status ignored (warnings dismissed)
	//   "none"     — no active/accepted/rejected compat footprint
	PathIssueFilter string
	// PathIssueCategory narrows sparse GPL issue JSON by category.
	PathIssueCategory string

	// FilterMatchedExpr is a SQL boolean ANDed into SRC search (legacy Duck path).
	// FilterArgs are bind parameters for the expression.
	FilterMatchedExpr string
	FilterArgs        []any
	NeedsChildAgg     bool
	// CompiledFilter evaluates rules in Go over Badger candidates (ops path).
	CompiledFilter *filter.CompiledRuleset
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

// normalizeUnderPath returns a non-root underPath scope, or "" when unset/global.
func normalizeUnderPath(p string) string {
	n := db.NormalizeRootRelativePath(strings.TrimSpace(p))
	if n == "" || n == "/" {
		return ""
	}
	return n
}

// ReviewFilterHasSearchPredicate reports whether f carries any user search predicate.
// ParentPath and ExcludeRoot are scoping/defaults for tree vs global search and do not count.
// UnderPath (non-root) counts so "everything under this folder" is a valid list search.
// StatusSearchType alone does not count without a status value.
// CompiledFilter (advanced ruleset) counts as a predicate on its own.
func ReviewFilterHasSearchPredicate(f ReviewFilter) bool {
	if normalizeUnderPath(f.UnderPath) != "" {
		return true
	}
	if strings.TrimSpace(f.Query) != "" {
		return true
	}
	for _, seg := range f.PathSegments {
		if strings.TrimSpace(seg) != "" {
			return true
		}
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
	if strings.TrimSpace(f.FilterMatchedExpr) != "" {
		return true
	}
	if f.CompiledFilter != nil {
		return true
	}
	return false
}

// QueryNodesForReview returns nodes from the given table (SRC or DST) with optional depth, status, excluded, pathLike filters. Status from events. Used by review API.
func QueryNodesForReview(d *db.DB, table string, depth *int, status string, excluded *bool, pathLike string, orderByPath bool, limit, offset int) ([]db.NodeState, error) {
	return queryNodesForReview(d, table, depth, status, excluded, pathLike, orderByPath, limit, offset)
}

// ListMergedReviewDiffs returns merged review rows and total count matching the filter, ordered and paginated.
// Use for tree diffs and any caller that needs an exact total. Search uses ListMergedReviewDiffsPage instead.
func ListMergedReviewDiffs(d *db.DB, f ReviewFilter, orderBy string, limit, offset int) ([]MergedReviewRow, int, error) {
	page, _, err := ListMergedReviewDiffsPage(d, f, orderBy, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	stats, err := GetMergedReviewStats(d, f)
	if err != nil {
		return nil, 0, err
	}
	return page, stats.Total, nil
}

// CountMergedReviewRows returns the number of merged rows matching the filter.
func CountMergedReviewRows(d *db.DB, f ReviewFilter) (int, error) {
	stats, err := getMergedReviewStats(d, f)
	if err != nil {
		return 0, err
	}
	return stats.Total, nil
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
	// Truncated is true when counting stopped early (e.g. query deadline), not when the true total is known.
	Truncated bool
}

// GetMergedReviewStats returns aggregate counts for rows matching the filter. Counts are unique by path (merged view = one row per path).
func GetMergedReviewStats(d *db.DB, f ReviewFilter) (MergedReviewStats, error) {
	return getMergedReviewStats(d, f)
}

// GetMergedReviewStatsCtx is GetMergedReviewStats with a parent context (interactive-read cancel / deadline).
func GetMergedReviewStatsCtx(parent context.Context, d *db.DB, f ReviewFilter) (MergedReviewStats, error) {
	return getMergedReviewStatsCtx(parent, d, f)
}
