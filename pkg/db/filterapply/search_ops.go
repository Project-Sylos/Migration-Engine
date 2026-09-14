// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package filterapply

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/review"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func addCopyMut(dst *db.CopyMutationResult, src db.CopyMutationResult) {
	dst.Affected += src.Affected
	dst.Folders += src.Folders
	dst.Files += src.Files
	dst.PendingBytes += src.PendingBytes
}

type searchFileMatch struct {
	id, path string
}

// ErrSearchApplyTimedOut is returned when exclude/unexclude-by-search hits the review query deadline
// mid-apply. Callers must treat this as failure (not partial success).
var ErrSearchApplyTimedOut = errors.New("search apply timed out before all matches were processed")

// ApplySearchExclusionOps writes live copy exclude for search matches (explicit root + inherited descendants).
func ApplySearchExclusionOps(
	database *db.DB,
	f review.ReviewFilter,
	criteriaJSON string,
	exceptIDs []string,
	applicationID string,
	eventTime int64,
) (db.CopyMutationResult, error) {
	return ApplySearchExclusionOpsCtx(context.Background(), database, f, criteriaJSON, exceptIDs, applicationID, eventTime)
}

// ApplySearchExclusionOpsCtx is ApplySearchExclusionOps with a parent context for deadline/cancel.
func ApplySearchExclusionOpsCtx(
	parent context.Context,
	database *db.DB,
	f review.ReviewFilter,
	criteriaJSON string,
	exceptIDs []string,
	applicationID string,
	eventTime int64,
) (db.CopyMutationResult, error) {
	var out db.CopyMutationResult
	if database == nil || database.Ops() == nil {
		return out, fmt.Errorf("ops store required")
	}
	if !review.ReviewFilterHasSearchPredicate(f) {
		return out, fmt.Errorf("search predicate required")
	}
	if applicationID == "" {
		return out, fmt.Errorf("filter application id required")
	}
	if err := database.Ops().PutFilterApplication(opsdb.FilterApplicationRecord{
		ID: applicationID, CriteriaJSON: criteriaJSON, AppliedAt: eventTime,
	}); err != nil {
		return out, fmt.Errorf("insert filter application: %w", err)
	}
	protected, err := loadProtectedNodesOps(database, exceptIDs)
	if err != nil {
		return out, err
	}
	skip := func(id, path, typ string) bool {
		return isProtected(id, path, typ, protected)
	}
	matchedFolderPaths := make(map[string]struct{})
	var matchedFiles []searchFileMatch
	var matchedExplicit int64
	ctx, cancel := database.ReviewQueryContext(parent)
	defer cancel()
	afterPath := strings.TrimSpace(f.AfterPath)
	afterID := strings.TrimSpace(f.AfterID)
	for {
		if err := ctx.Err(); err != nil {
			return out, fmt.Errorf("%w: %v", ErrSearchApplyTimedOut, err)
		}
		pageFilter := f
		pageFilter.AfterPath = afterPath
		pageFilter.AfterID = afterID
		rows, hasMore, err := review.ListMergedReviewDiffsPageCtx(ctx, database, pageFilter, "path ASC", 500, 0)
		if err != nil {
			if ctx.Err() != nil {
				return out, fmt.Errorf("%w: %v", ErrSearchApplyTimedOut, err)
			}
			return out, err
		}
		for _, r := range rows {
			if r.SrcNodeID == "" || isProtected(r.SrcNodeID, r.Path, r.Type, protected) {
				continue
			}
			st, ok, err := database.Ops().GetStatus(opsdb.SideSRC, r.SrcNodeID)
			if err != nil {
				return out, err
			}
			if !ok {
				continue
			}
			if st.CopyStatus != "" && st.CopyStatus != db.CopyStatusPending {
				continue
			}
			matchedExplicit++
			if r.Type == db.NodeTypeFolder {
				matchedFolderPaths[r.Path] = struct{}{}
				continue
			}
			matchedFiles = append(matchedFiles, searchFileMatch{id: r.SrcNodeID, path: r.Path})
		}
		if !hasMore || len(rows) == 0 {
			break
		}
		last := rows[len(rows)-1]
		afterPath = last.Path
		afterID = last.SrcNodeID
	}
	topFolders := collapseTopLevelFolderPaths(matchedFolderPaths)
	for _, folderPath := range topFolders {
		if err := ctx.Err(); err != nil {
			return out, fmt.Errorf("%w: %v", ErrSearchApplyTimedOut, err)
		}
		mut, err := database.ApplySubtreeCopyExclusion(folderPath, true, skip)
		if err != nil {
			return out, err
		}
		addCopyMut(&out, mut)
	}
	for _, fm := range matchedFiles {
		if err := ctx.Err(); err != nil {
			return out, fmt.Errorf("%w: %v", ErrSearchApplyTimedOut, err)
		}
		if pathUnderAnyFolder(fm.path, topFolders) {
			continue
		}
		mut, err := database.ApplyNodeCopyExclusion(fm.id, true)
		if err != nil {
			return out, err
		}
		addCopyMut(&out, mut)
	}
	if err := database.Ops().UpdateFilterApplicationCounts(applicationID, matchedExplicit, out.Affected); err != nil {
		return out, fmt.Errorf("update filter application counts: %w", err)
	}
	return out, nil
}

// ApplySearchUnexclusionOps restores pending copy status and pend:copy for excluded search matches.
func ApplySearchUnexclusionOps(
	database *db.DB,
	f review.ReviewFilter,
	exceptIDs []string,
	eventTime int64,
) (db.CopyMutationResult, error) {
	return ApplySearchUnexclusionOpsCtx(context.Background(), database, f, exceptIDs, eventTime)
}

// ApplySearchUnexclusionOpsCtx is ApplySearchUnexclusionOps with a parent context for deadline/cancel.
func ApplySearchUnexclusionOpsCtx(
	parent context.Context,
	database *db.DB,
	f review.ReviewFilter,
	exceptIDs []string,
	_ int64,
) (db.CopyMutationResult, error) {
	var out db.CopyMutationResult
	if database == nil || database.Ops() == nil {
		return out, fmt.Errorf("ops store required")
	}
	if !review.ReviewFilterHasSearchPredicate(f) {
		return out, fmt.Errorf("search predicate required")
	}
	protected, err := loadProtectedNodesOps(database, exceptIDs)
	if err != nil {
		return out, err
	}
	skip := func(id, path, typ string) bool {
		return isProtected(id, path, typ, protected)
	}
	matchedFolderPaths := make(map[string]struct{})
	var matchedFiles []searchFileMatch
	ctx, cancel := database.ReviewQueryContext(parent)
	defer cancel()
	afterPath := strings.TrimSpace(f.AfterPath)
	afterID := strings.TrimSpace(f.AfterID)
	for {
		if err := ctx.Err(); err != nil {
			return out, fmt.Errorf("%w: %v", ErrSearchApplyTimedOut, err)
		}
		pageFilter := f
		pageFilter.AfterPath = afterPath
		pageFilter.AfterID = afterID
		rows, hasMore, err := review.ListMergedReviewDiffsPageCtx(ctx, database, pageFilter, "path ASC", 500, 0)
		if err != nil {
			if ctx.Err() != nil {
				return out, fmt.Errorf("%w: %v", ErrSearchApplyTimedOut, err)
			}
			return out, err
		}
		for _, r := range rows {
			if r.SrcNodeID == "" || isProtected(r.SrcNodeID, r.Path, r.Type, protected) {
				continue
			}
			if !r.Excluded {
				continue
			}
			if r.Type == db.NodeTypeFolder {
				matchedFolderPaths[r.Path] = struct{}{}
				continue
			}
			matchedFiles = append(matchedFiles, searchFileMatch{id: r.SrcNodeID, path: r.Path})
		}
		if !hasMore || len(rows) == 0 {
			break
		}
		last := rows[len(rows)-1]
		afterPath = last.Path
		afterID = last.SrcNodeID
	}
	topFolders := collapseTopLevelFolderPaths(matchedFolderPaths)
	for _, folderPath := range topFolders {
		if err := ctx.Err(); err != nil {
			return out, fmt.Errorf("%w: %v", ErrSearchApplyTimedOut, err)
		}
		mut, err := database.ApplySubtreeCopyExclusion(folderPath, false, skip)
		if err != nil {
			return out, err
		}
		addCopyMut(&out, mut)
	}
	for _, fm := range matchedFiles {
		if err := ctx.Err(); err != nil {
			return out, fmt.Errorf("%w: %v", ErrSearchApplyTimedOut, err)
		}
		if pathUnderAnyFolder(fm.path, topFolders) {
			continue
		}
		mut, err := database.ApplyNodeCopyExclusion(fm.id, false)
		if err != nil {
			return out, err
		}
		addCopyMut(&out, mut)
	}
	return out, nil
}

func collapseTopLevelFolderPaths(paths map[string]struct{}) []string {
	if len(paths) == 0 {
		return nil
	}
	list := make([]string, 0, len(paths))
	for p := range paths {
		list = append(list, db.NormalizeSubtreeRootPathForPropagation(p))
	}
	sort.Strings(list)
	var out []string
	for _, p := range list {
		keep := true
		for _, root := range out {
			if p == root || strings.HasPrefix(p, root+"/") {
				keep = false
				break
			}
		}
		if keep {
			out = append(out, p)
		}
	}
	return out
}

func pathUnderAnyFolder(path string, folders []string) bool {
	for _, root := range folders {
		if path == root || strings.HasPrefix(path, root+"/") {
			return true
		}
	}
	return false
}

type protectedSet struct {
	ids   map[string]struct{}
	paths map[string]struct{}
}

func loadProtectedNodesOps(database *db.DB, exceptIDs []string) (protectedSet, error) {
	out := protectedSet{ids: make(map[string]struct{}), paths: make(map[string]struct{})}
	if len(exceptIDs) == 0 {
		return out, nil
	}
	ops := database.Ops()
	nodes, err := ops.BatchGetNode(opsdb.SideSRC, exceptIDs)
	if err != nil {
		return out, err
	}
	for _, id := range exceptIDs {
		out.ids[id] = struct{}{}
		if n, ok := nodes[id]; ok && n.Path != "" {
			out.paths[n.Path] = struct{}{}
		}
	}
	return out, nil
}

func isProtected(id, path, typ string, protected protectedSet) bool {
	if _, ok := protected.ids[id]; ok {
		return true
	}
	if path == "" {
		return false
	}
	if _, ok := protected.paths[path]; ok {
		return true
	}
	for p := range protected.paths {
		if strings.HasPrefix(path, p+"/") {
			return true
		}
	}
	_ = typ
	return false
}
