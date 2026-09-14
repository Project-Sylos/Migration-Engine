// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

// StatusDrivenSearch reports whether list search should page from the Badger st: overlay (id -> status).
// True when the filter is status-only (no path/name/parent predicates).
func StatusDrivenSearch(f ReviewFilter) bool {
	if strings.TrimSpace(f.ParentPath) != "" {
		return false
	}
	if normalizeUnderPath(f.UnderPath) != "" {
		return false
	}
	if len(pathSegmentsForFilter(f)) > 0 || strings.TrimSpace(f.Query) != "" {
		return false
	}
	if strings.TrimSpace(f.FilterMatchedExpr) != "" {
		return false
	}
	if strings.TrimSpace(f.PathIssueFilter) != "" || strings.TrimSpace(f.PathIssueCategory) != "" {
		return false
	}
	if statusOverlayBlockedByNodeFilter(f) {
		return false
	}
	st := strings.ToLower(strings.TrimSpace(f.StatusSearchType))
	copySt := strings.TrimSpace(f.CopyStatus)
	delSt := strings.TrimSpace(f.DeleteStatus)
	trav := strings.TrimSpace(f.TraversalStatus)
	needCopy := copySt != "" && (st == "" || st == "copy" || st == "both")
	needDelete := delSt != "" && (st == "" || st == "delete" || st == "both")
	needTrav := trav != "" && !strings.EqualFold(trav, "not_on_src") && (st == "" || st == "traversal" || st == "both")
	return needCopy || needDelete || needTrav
}

func statusOverlayBlockedByNodeFilter(f ReviewFilter) bool {
	return f.FoldersOnly || strings.EqualFold(f.TypeFilter, "folder") ||
		strings.EqualFold(f.TypeFilter, "file") || f.DepthValue != nil || f.SizeValue != nil
}

func needsPostHydrateNodeFilter(f ReviewFilter) bool {
	return f.ExcludeRoot || statusOverlayBlockedByNodeFilter(f)
}

func nodeMatchesPostFilter(n srcNodeMeta, f ReviewFilter) bool {
	if f.ExcludeRoot && n.path == "/" {
		return false
	}
	if f.FoldersOnly || strings.EqualFold(f.TypeFilter, "folder") {
		if n.typ != db.NodeTypeFolder {
			return false
		}
	} else if strings.EqualFold(f.TypeFilter, "file") {
		if n.typ != db.NodeTypeFile {
			return false
		}
	}
	if f.DepthValue != nil && f.DepthOperator != "" {
		if !compareInt(n.depth, *f.DepthValue, f.DepthOperator) {
			return false
		}
	}
	if f.SizeValue != nil && f.SizeOperator != "" {
		if !compareInt64(n.size, *f.SizeValue, f.SizeOperator) {
			return false
		}
	}
	return true
}

func compareInt(got, want int, op string) bool {
	switch strings.ToLower(op) {
	case "gt", ">":
		return got > want
	case "gte", ">=":
		return got >= want
	case "lt", "<":
		return got < want
	case "lte", "<=":
		return got <= want
	default:
		return got == want
	}
}

func compareInt64(got, want int64, op string) bool {
	switch strings.ToLower(op) {
	case "gt", ">":
		return got > want
	case "gte", ">=":
		return got >= want
	case "lt", "<":
		return got < want
	case "lte", "<=":
		return got <= want
	default:
		return got == want
	}
}

type srcNodeMeta struct {
	path, name, typ string
	depth           int
	size            int64
}

// CountSrcCurrentMatchingStatus counts status-overlay rows for a status-driven filter.
func CountSrcCurrentMatchingStatus(d *db.DB, f ReviewFilter) (int, error) {
	s, err := statsFromOpsStatusOverlay(d, f)
	if err != nil {
		return 0, err
	}
	return s.Total, nil
}
