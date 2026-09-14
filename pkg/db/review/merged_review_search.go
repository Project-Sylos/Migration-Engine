// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"context"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

// ListMergedReviewDiffsPage returns a page of merged review rows without COUNT(*).
func ListMergedReviewDiffsPage(d *db.DB, f ReviewFilter, orderBy string, limit, offset int) (page []MergedReviewRow, hasMore bool, err error) {
	return ListMergedReviewDiffsPageCtx(context.Background(), d, f, orderBy, limit, offset)
}

// ListMergedReviewDiffsPageCtx is ListMergedReviewDiffsPage with a parent context
// (ReviewQueryContext deadline / cancel). Prefer this for multi-page count and exclude loops.
func ListMergedReviewDiffsPageCtx(parent context.Context, d *db.DB, f ReviewFilter, orderBy string, limit, offset int) (page []MergedReviewRow, hasMore bool, err error) {
	return listMergedReviewDiffsPageCtx(parent, d, f, orderBy, limit, offset)
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
