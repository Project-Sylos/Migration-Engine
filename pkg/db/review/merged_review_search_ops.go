// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"context"
	"fmt"
	"strings"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func listMergedReviewDiffsPageCtx(parent context.Context, d *db.DB, f ReviewFilter, orderBy string, limit, offset int) (page []MergedReviewRow, hasMore bool, err error) {
	start := time.Now()
	defer func() { d.RecordOp(db.OpReviewSearchPage, "", int64(len(page)), time.Since(start), err) }()
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
	useKeyset := strings.TrimSpace(f.AfterPath) != "" || strings.TrimSpace(f.AfterID) != ""
	fetch := limit + 1
	if !useKeyset {
		fetch = offset + limit + 1
	}
	ctx, cancel := d.ReviewQueryContext(parent)
	defer cancel()

	if StatusDrivenSearch(f) {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		ob := strings.ToLower(strings.TrimSpace(orderBy))
		idDesc := strings.HasPrefix(ob, "id") && strings.Contains(ob, "desc")
		srcRows, err := queryReviewSearchSRCByStatusOps(ctx, d, f, fetch, idDesc)
		if err != nil {
			return nil, false, err
		}
		if !useKeyset {
			if offset >= len(srcRows) {
				return nil, false, nil
			}
			srcRows = srcRows[offset:]
		}
		hasMore = len(srcRows) > limit
		if hasMore {
			srcRows = srcRows[:limit]
		}
		return srcRows, hasMore, nil
	}

	if reviewSearchSRCImpossible(f) {
		srcRows := []MergedReviewRow{}
		var dstOnly []MergedReviewRow
		if reviewSearchIncludeDSTOnly(f) {
			dstOnly, err = queryReviewSearchDSTOnlyOps(ctx, d, f, sortCol, sortDesc, fetch)
			if err != nil {
				return nil, false, err
			}
		}
		merged := mergeReviewSearchRows(srcRows, dstOnly, sortCol, sortDesc)
		return sliceMergedPage(merged, limit, offset, useKeyset)
	}

	srcRows, err := queryReviewSearchSRCOps(ctx, d, f, sortCol, sortDesc, fetch)
	if err != nil {
		return nil, false, err
	}
	var dstOnly []MergedReviewRow
	if reviewSearchIncludeDSTOnly(f) {
		dstOnly, err = queryReviewSearchDSTOnlyOps(ctx, d, f, sortCol, sortDesc, fetch)
		if err != nil {
			return nil, false, err
		}
	}
	merged := mergeReviewSearchRows(srcRows, dstOnly, sortCol, sortDesc)
	return sliceMergedPage(merged, limit, offset, useKeyset)
}

func sliceMergedPage(merged []MergedReviewRow, limit, offset int, useKeyset bool) ([]MergedReviewRow, bool, error) {
	if !useKeyset {
		if offset >= len(merged) {
			return nil, false, nil
		}
		merged = merged[offset:]
	}
	hasMore := len(merged) > limit
	if hasMore {
		merged = merged[:limit]
	}
	return merged, hasMore, nil
}

func queryReviewSearchSRCOps(ctx context.Context, d *db.DB, f ReviewFilter, sortCol string, sortDesc bool, limit int) ([]MergedReviewRow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ops := d.Ops()
	if ops == nil {
		return nil, fmt.Errorf("review search SRC: ops store required")
	}
	// Path-index streaming resumes via AfterPath so pages are not stuck in the first
	// ~100K–200K candidates. Selective size/name/seg plans still use the planner.
	if reviewSearchShouldStreamPathIndex(f, sortCol) {
		return queryReviewSearchSRCStreamPath(ctx, d, f, limit)
	}
	overFetch := limit * 4
	if overFetch < limit+50 {
		overFetch = limit + 50
	}
	plan, err := planReviewCandidateIDs(ops, opsdb.SideSRC, f, overFetch)
	if err != nil {
		return nil, err
	}
	ids := plan.ids
	if len(ids) == 0 {
		return nil, nil
	}
	nodes, err := ops.BatchGetNode(opsdb.SideSRC, ids)
	if err != nil {
		return nil, err
	}
	filtered := make([]string, 0, len(ids))
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, ok := nodes[id]
		if !ok || !nodeMatchesMetaFilter(ops, opsdb.SideSRC, n, f) {
			continue
		}
		if !nodePassesTrigramVerify(n, f) {
			continue
		}
		okMatch, err := nodeMatchesFilterOrExpr(ops, opsdb.SideSRC, n, f)
		if err != nil {
			return nil, err
		}
		if !okMatch {
			continue
		}
		filtered = append(filtered, id)
	}
	sortCandidateIDsByPath(nodes, filtered, sortCol, sortDesc)
	if len(filtered) > overFetch {
		filtered = filtered[:overFetch]
	}
	return hydrateSRCReviewRows(ctx, d, ops, f, filtered, nodes, limit)
}

// reviewSearchShouldStreamPathIndex is true when candidates come from a full/under
// path walk (or ruleset-only) and results are path-ordered so AfterPath resume works.
func reviewSearchShouldStreamPathIndex(f ReviewFilter, sortCol string) bool {
	if strings.TrimSpace(sortCol) != "" && !strings.EqualFold(sortCol, "path") {
		return false
	}
	if f.SizeValue != nil && f.SizeOperator != "" {
		return false
	}
	if bestContainsNeedle(containsNeedlesFromFilter(f)) != "" {
		return false
	}
	nameQ := strings.TrimSpace(f.Query)
	nameField := strings.TrimSpace(f.QueryField)
	if nameQ != "" && strings.EqualFold(nameField, "name") && !strings.Contains(nameQ, "%") && !strings.HasPrefix(nameQ, "*") {
		return false
	}
	segments := pathSegmentsForFilter(f)
	if len(segments) > 0 {
		first := strings.ToLower(strings.TrimSpace(segments[0]))
		if first != "" && !strings.ContainsAny(first, "*%") {
			return false
		}
	}
	return true
}

func queryReviewSearchSRCStreamPath(ctx context.Context, d *db.DB, f ReviewFilter, limit int) ([]MergedReviewRow, error) {
	ops := d.Ops()
	if ops == nil {
		return nil, fmt.Errorf("review search SRC stream: ops store required")
	}
	if limit <= 0 {
		limit = 100
	}
	root := strings.TrimSpace(f.UnderPath)
	if root == "" {
		root = "/"
	}
	afterPath := strings.TrimSpace(f.AfterPath)
	const chunk = 2000
	matchedIDs := make([]string, 0, limit)
	nodes := make(map[string]opsdb.NodeRecord)
	for len(matchedIDs) < limit {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		need := (limit - len(matchedIDs)) * 4
		if need < chunk {
			need = chunk
		}
		ids, nextAfter, done, err := ops.ScanSubtreeIDs(opsdb.SideSRC, root, afterPath, need, opsdb.SubtreeScanOpts{})
		if err != nil {
			return nil, err
		}
		if len(ids) == 0 {
			break
		}
		batch, err := ops.BatchGetNode(opsdb.SideSRC, ids)
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			n, ok := batch[id]
			if !ok {
				continue
			}
			nodes[id] = n
			if !nodeMatchesMetaFilter(ops, opsdb.SideSRC, n, f) {
				continue
			}
			okMatch, err := nodeMatchesFilterOrExpr(ops, opsdb.SideSRC, n, f)
			if err != nil {
				return nil, err
			}
			if !okMatch {
				continue
			}
			matchedIDs = append(matchedIDs, id)
			if len(matchedIDs) >= limit {
				break
			}
		}
		if done {
			break
		}
		afterPath = nextAfter
	}
	return hydrateSRCReviewRows(ctx, d, ops, f, matchedIDs, nodes, limit)
}

func hydrateSRCReviewRows(
	ctx context.Context,
	d *db.DB,
	ops *opsdb.Store,
	f ReviewFilter,
	filtered []string,
	nodes map[string]opsdb.NodeRecord,
	limit int,
) ([]MergedReviewRow, error) {
	if len(filtered) == 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stMap, err := ops.BatchGetStatus(opsdb.SideSRC, filtered)
	if err != nil {
		return nil, err
	}
	dstBySrc := make(map[string]string, len(filtered))
	dstIDs := make([]string, 0, len(filtered))
	for _, id := range filtered {
		if m, ok, _ := ops.GetMapBySrc(id); ok {
			dstBySrc[id] = m.DstID
			dstIDs = append(dstIDs, m.DstID)
		}
	}
	dstTrav, err := effectiveDSTTraversalByID(d, dstIDs)
	if err != nil {
		return nil, err
	}
	out := make([]MergedReviewRow, 0, limit)
	for _, id := range filtered {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n := nodes[id]
		st := stMap[id]
		r := MergedReviewRow{
			SrcNodeID:          id,
			Path:               n.Path,
			Name:               n.Name,
			Depth:              n.Depth,
			Type:               n.Type,
			Size:               opsdb.DisplayBytes(n.Type, n.Size, st.ChildSize),
			SrcTraversalStatus: st.TraversalStatus,
			CopyStatus:         st.CopyStatus,
			DeleteStatus:       st.DeleteStatus,
			Excluded:           st.CopyStatus == db.CopyStatusExcludedExplicit || st.CopyStatus == db.CopyStatusExcludedInherited,
		}
		if dstID := dstBySrc[id]; dstID != "" {
			r.DstNodeID = dstID
			r.DstTraversalStatus = dstTrav[dstID]
		}
		if !rowMatchesSRCStatusFilter(r, f) {
			continue
		}
		if strings.TrimSpace(f.PathIssueFilter) != "" || strings.TrimSpace(f.PathIssueCategory) != "" {
			gpl, ok, err := ops.GetGPL(id)
			if err != nil {
				return nil, err
			}
			ignored := st.GPLStatus == db.GPLStatusIgnored
			switch f.PathIssueFilter {
			case "rejected", "ignored":
				if !ignored {
					continue
				}
			case "none":
				if ok || ignored {
					continue
				}
			default:
				if ignored {
					continue
				}
				if !ok || !opsdb.GPLMatchesFilter(gpl, f.PathIssueFilter, f.PathIssueCategory) {
					continue
				}
			}
		}
		if gpl, ok, _ := ops.GetGPL(id); ok && gpl.Status == db.GPLIssueStatusAccepted {
			r.ResolvedDstName = gpl.ProposedName
		}
		out = append(out, r)
		if len(out) >= limit {
			break
		}
	}
	if err := attachPairedDSTTraversal(d, out); err != nil {
		return nil, err
	}
	return out, nil
}

func queryReviewSearchDSTOnlyOps(ctx context.Context, d *db.DB, f ReviewFilter, sortCol string, sortDesc bool, limit int) ([]MergedReviewRow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ops := d.Ops()
	if ops == nil {
		return nil, fmt.Errorf("review search DST-only: ops store required")
	}
	if strings.TrimSpace(f.FilterMatchedExpr) != "" && f.CompiledFilter == nil {
		return nil, nil
	}
	overFetch := limit * 2
	if overFetch < limit+50 {
		overFetch = limit + 50
	}
	plan, err := planReviewCandidateIDs(ops, opsdb.SideDST, f, overFetch)
	if err != nil {
		return nil, err
	}
	ids := plan.ids
	if len(ids) == 0 {
		return nil, nil
	}
	nodes, err := ops.BatchGetNode(opsdb.SideDST, ids)
	if err != nil {
		return nil, err
	}
	maps, err := ops.BatchGetMap(ids, true)
	if err != nil {
		return nil, err
	}
	filtered := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, mapped := maps[id]; mapped {
			continue // has SRC mapping
		}
		n, ok := nodes[id]
		if !ok || !nodeMatchesMetaFilter(ops, opsdb.SideDST, n, f) {
			continue
		}
		filtered = append(filtered, id)
	}
	sortCandidateIDsByPath(nodes, filtered, sortCol, sortDesc)
	dstTrav, err := effectiveDSTTraversalByID(d, filtered)
	if err != nil {
		return nil, err
	}
	stMap, err := ops.BatchGetStatus(opsdb.SideDST, filtered)
	if err != nil {
		return nil, err
	}
	out := make([]MergedReviewRow, 0, limit)
	for _, id := range filtered {
		n := nodes[id]
		r := MergedReviewRow{
			DstNodeID:          id,
			Path:               n.Path,
			Name:               n.Name,
			Depth:              n.Depth,
			Type:               n.Type,
			Size:               opsdb.DisplayBytes(n.Type, n.Size, stMap[id].ChildSize),
			DstTraversalStatus: dstTrav[id],
		}
		if !rowMatchesDSTStatusFilter(r, f) {
			continue
		}
		out = append(out, r)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func queryReviewSearchSRCByStatusOps(ctx context.Context, d *db.DB, f ReviewFilter, limit int, idDesc bool) ([]MergedReviewRow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ops := d.Ops()
	if ops == nil {
		return nil, fmt.Errorf("review status overlay: ops store required")
	}
	if limit <= 0 {
		limit = 100
	}
	after := strings.TrimSpace(f.AfterID)
	type hit struct {
		id string
		st opsdb.StatusRecord
	}
	collectLimit := limit + 1
	if needsPostHydrateNodeFilter(f) {
		collectLimit = limit*4 + 1
		if collectLimit < limit+51 {
			collectLimit = limit + 51
		}
	}
	hits := make([]hit, 0, collectLimit)
	err := walkReviewSRCStatus(d, f, after, idDesc, func(id string, st opsdb.StatusRecord) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if !statusRecordMatchesSRC(st, f) {
			return true, nil
		}
		hits = append(hits, hit{id: id, st: st})
		return len(hits) < collectLimit, nil
	})
	if err != nil {
		return nil, err
	}
	if len(hits) == 0 {
		return nil, nil
	}
	ids := make([]string, len(hits))
	for i, h := range hits {
		ids[i] = h.id
	}
	nodes, err := ops.BatchGetNode(opsdb.SideSRC, ids)
	if err != nil {
		return nil, err
	}
	out := make([]MergedReviewRow, 0, limit)
	for _, h := range hits {
		n, ok := nodes[h.id]
		if !ok {
			continue
		}
		if !nodeRecordMatchesPostFilter(n, f) {
			continue
		}
		r := mergedRowFromOps(h.id, n, h.st)
		if m, ok, _ := ops.GetMapBySrc(h.id); ok {
			r.DstNodeID = m.DstID
		}
		out = append(out, r)
		if len(out) >= limit {
			break
		}
	}
	if err := attachPairedDSTTraversal(d, out); err != nil {
		return nil, err
	}
	return out, nil
}

func nodeRecordMatchesPostFilter(n opsdb.NodeRecord, f ReviewFilter) bool {
	return nodeMatchesPostFilter(srcNodeMeta{
		path: n.Path, name: n.Name, typ: n.Type, depth: n.Depth, size: n.Size,
	}, f)
}

func mergedRowFromOps(id string, n opsdb.NodeRecord, st opsdb.StatusRecord) MergedReviewRow {
	r := MergedReviewRow{
		SrcNodeID:          id,
		Path:               n.Path,
		Name:               n.Name,
		Depth:              n.Depth,
		Type:               n.Type,
		Size:               opsdb.DisplayBytes(n.Type, n.Size, st.ChildSize),
		SrcTraversalStatus: st.TraversalStatus,
		CopyStatus:         st.CopyStatus,
		DeleteStatus:       st.DeleteStatus,
		Excluded:           st.CopyStatus == db.CopyStatusExcludedExplicit || st.CopyStatus == db.CopyStatusExcludedInherited,
	}
	if r.Name == "" {
		r.Name = id
	}
	return r
}

func reviewStatusWalkPhase(f ReviewFilter) (phase string, ok bool) {
	st := strings.ToLower(strings.TrimSpace(f.StatusSearchType))
	trav := strings.TrimSpace(f.TraversalStatus)
	copySt := strings.TrimSpace(f.CopyStatus)
	delSt := strings.TrimSpace(f.DeleteStatus)
	needTrav := trav != "" && !strings.EqualFold(trav, "not_on_src") && (st == "" || st == "traversal" || st == "both")
	needCopy := copySt != "" && (st == "" || st == "copy" || st == "both")
	needDel := delSt != "" && (st == "" || st == "delete" || st == "both")
	if needCopy && (strings.EqualFold(copySt, db.CopyStatusExcluded) || strings.EqualFold(copySt, "excluded")) {
		return "", false
	}
	if needCopy && !needTrav && !needDel {
		return opsdb.PhaseCopy, true
	}
	if needDel && !needTrav && !needCopy {
		return opsdb.PhaseDel, true
	}
	if needTrav && !needCopy && !needDel {
		return opsdb.PhaseTrav, true
	}
	return "", false
}

func walkReviewSRCStatus(d *db.DB, f ReviewFilter, after string, idDesc bool, fn func(id string, st opsdb.StatusRecord) (bool, error)) error {
	ops := d.Ops()
	if ops == nil {
		return fmt.Errorf("review status overlay: ops store required")
	}
	phase, usePhase := reviewStatusWalkPhase(f)
	if usePhase {
		return ops.WalkStatusPhase(opsdb.SideSRC, phase, after, idDesc, fn)
	}
	return ops.WalkStatus(opsdb.SideSRC, after, idDesc, fn)
}

func statusRecordMatchesSRC(st opsdb.StatusRecord, f ReviewFilter) bool {
	if !reviewFilterUsesStructuredStatus(f) {
		return true
	}
	searchType := strings.ToLower(strings.TrimSpace(f.StatusSearchType))
	trav := strings.TrimSpace(f.TraversalStatus)
	copySt := strings.TrimSpace(f.CopyStatus)
	delSt := strings.TrimSpace(f.DeleteStatus)
	if trav != "" && (searchType == "" || searchType == "traversal" || searchType == "both") {
		if !traversalStatusMatches(st.TraversalStatus, trav) {
			return false
		}
	}
	if copySt != "" && (searchType == "" || searchType == "copy" || searchType == "both") {
		if !copyOrTraversalExclusionMatches(st.CopyStatus, st.TraversalStatus, copySt) {
			return false
		}
	}
	if delSt != "" && (searchType == "" || searchType == "delete" || searchType == "both") {
		if !deleteStatusMatches(st.DeleteStatus, delSt) {
			return false
		}
	}
	return true
}

func rowMatchesSRCStatusFilter(r MergedReviewRow, f ReviewFilter) bool {
	if !reviewFilterUsesStructuredStatus(f) {
		return true
	}
	st := strings.ToLower(strings.TrimSpace(f.StatusSearchType))
	trav := strings.TrimSpace(f.TraversalStatus)
	copySt := strings.TrimSpace(f.CopyStatus)
	delSt := strings.TrimSpace(f.DeleteStatus)
	if trav != "" && (st == "" || st == "traversal" || st == "both") {
		srcT := r.SrcTraversalStatus
		dstT := r.DstTraversalStatus
		if reviewSearchSRCNeedsDSTBeforeLimit(f) {
			if !traversalStatusMatches(srcT, trav) && !traversalStatusMatches(dstT, trav) {
				return false
			}
		} else if !traversalStatusMatches(srcT, trav) {
			return false
		}
	}
	if copySt != "" && (st == "" || st == "copy" || st == "both") {
		if !copyOrTraversalExclusionMatches(r.CopyStatus, r.SrcTraversalStatus, copySt) {
			return false
		}
	}
	if delSt != "" && (st == "" || st == "delete" || st == "both") {
		if !deleteStatusMatches(r.DeleteStatus, delSt) {
			return false
		}
	}
	return true
}

func rowMatchesDSTStatusFilter(r MergedReviewRow, f ReviewFilter) bool {
	trav := strings.TrimSpace(f.TraversalStatus)
	if trav == "" {
		return true
	}
	st := strings.ToLower(strings.TrimSpace(f.StatusSearchType))
	if st != "" && st != "traversal" && st != "both" {
		return true
	}
	return traversalStatusMatches(r.DstTraversalStatus, trav)
}

func traversalStatusMatches(have, want string) bool {
	if strings.EqualFold(strings.TrimSpace(want), "not_failed") {
		return !strings.EqualFold(strings.TrimSpace(have), db.StatusFailed)
	}
	return strings.EqualFold(have, want)
}

func copyStatusMatches(have, want string) bool {
	v := strings.ToLower(strings.TrimSpace(want))
	h := strings.ToLower(strings.TrimSpace(have))
	if v == "pending" {
		return db.CopyStatusIsPending(h)
	}
	if v == "excluded" {
		return h == db.CopyStatusExcludedExplicit || h == db.CopyStatusExcludedInherited
	}
	if v == "successful" {
		return h == db.CopyStatusSuccessful || h == db.CopyStatusAlreadyExisted
	}
	return h == v
}

func traversalExcludedMatches(trav string) bool {
	t := strings.ToLower(strings.TrimSpace(trav))
	return t == db.StatusExcluded || t == db.StatusExclusionInherited
}

func copyOrTraversalExclusionMatches(copySt, travSt, want string) bool {
	if !strings.EqualFold(strings.TrimSpace(want), db.CopyStatusExcluded) &&
		!strings.EqualFold(strings.TrimSpace(want), "excluded") {
		return copyStatusMatches(copySt, want)
	}
	return copyStatusMatches(copySt, want) || traversalExcludedMatches(travSt)
}

func deleteStatusMatches(have, want string) bool {
	v := strings.ToLower(strings.TrimSpace(want))
	h := strings.ToLower(strings.TrimSpace(have))
	switch v {
	case "pending":
		return db.DeleteStatusIsPending(h) || h == "pending"
	case "excluded", "skipped":
		return h == db.DeleteStatusSkipped
	default:
		return h == v
	}
}

func reviewSearchSRCNeedsDSTBeforeLimit(f ReviewFilter) bool {
	trav := strings.TrimSpace(f.TraversalStatus)
	if trav == "" || strings.EqualFold(trav, "not_on_src") {
		return false
	}
	st := strings.ToLower(strings.TrimSpace(f.StatusSearchType))
	return st == "" || st == "traversal" || st == "both"
}

func getMergedReviewStats(d *db.DB, f ReviewFilter) (MergedReviewStats, error) {
	return getMergedReviewStatsCtx(context.Background(), d, f)
}

func getMergedReviewStatsCtx(parent context.Context, d *db.DB, f ReviewFilter) (MergedReviewStats, error) {
	// Unfiltered footer/stats must stay O(1) ++/-- counters. Never fall back to a
	// full merged-row scan when the tree is empty or counters happen to be zero.
	if reviewFilterUnfiltered(f) {
		return mergedReviewStatsFromCanonical(d)
	}
	if StatusDrivenSearch(f) && !needsPostHydrateNodeFilter(f) {
		return statsFromOpsStatusOverlay(d, f)
	}
	ctx, cancel := d.ReviewQueryContext(parent)
	defer cancel()

	const pageSize = 1000
	var s MergedReviewStats
	afterPath := strings.TrimSpace(f.AfterPath)
	afterID := strings.TrimSpace(f.AfterID)
	for {
		if err := ctx.Err(); err != nil {
			s.Truncated = true
			return s, nil
		}
		pageFilter := f
		pageFilter.AfterPath = afterPath
		pageFilter.AfterID = afterID
		rows, hasMore, err := listMergedReviewDiffsPageCtx(ctx, d, pageFilter, "path ASC", pageSize, 0)
		if err != nil {
			if ctx.Err() != nil {
				s.Truncated = true
				return s, nil
			}
			return MergedReviewStats{}, err
		}
		for _, r := range rows {
			s.Total++
			if r.Type == db.NodeTypeFolder {
				s.Folders++
			}
			if r.Type == db.NodeTypeFile {
				s.Files++
			}
			if r.SrcNodeID == "" {
				s.MissingOnSource++
			}
			if r.DstNodeID == "" {
				s.MissingOnDest++
			}
			if r.Excluded {
				s.Excluded++
			}
			if r.Type == db.NodeTypeFile {
				s.SizeSrc += r.Size
				if !r.Excluded && (r.CopyStatus == "" || r.CopyStatus == db.CopyStatusPending) {
					s.SizeSelected += r.Size
				}
			}
		}
		if !hasMore || len(rows) == 0 {
			return s, nil
		}
		last := rows[len(rows)-1]
		afterPath = last.Path
		afterID = last.SrcNodeID
		if afterID == "" {
			afterID = last.DstNodeID
		}
	}
}

func statsFromOpsStatusOverlay(d *db.DB, f ReviewFilter) (MergedReviewStats, error) {
	ops := d.Ops()
	if ops == nil {
		return MergedReviewStats{}, fmt.Errorf("review status overlay: ops store required")
	}
	var s MergedReviewStats
	ids := make([]string, 0, 512)
	sts := make([]opsdb.StatusRecord, 0, 512)
	flush := func() error {
		if len(ids) == 0 {
			return nil
		}
		nodes, err := ops.BatchGetNode(opsdb.SideSRC, ids)
		if err != nil {
			return err
		}
		for i, id := range ids {
			st := sts[i]
			n := nodes[id]
			if !nodeRecordMatchesPostFilter(n, f) {
				continue
			}
			s.Total++
			if n.Type == db.NodeTypeFolder {
				s.Folders++
			}
			if n.Type == db.NodeTypeFile {
				s.Files++
				s.SizeSrc += n.Size
				if st.CopyStatus != db.CopyStatusExcludedExplicit && st.CopyStatus != db.CopyStatusExcludedInherited &&
					(st.CopyStatus == "" || st.CopyStatus == db.CopyStatusPending) {
					s.SizeSelected += n.Size
				}
			}
			if st.CopyStatus == db.CopyStatusExcludedExplicit || st.CopyStatus == db.CopyStatusExcludedInherited {
				s.Excluded++
			}
		}
		ids = ids[:0]
		sts = sts[:0]
		return nil
	}
	err := walkReviewSRCStatus(d, f, "", false, func(id string, st opsdb.StatusRecord) (bool, error) {
		if !statusRecordMatchesSRC(st, f) {
			return true, nil
		}
		ids = append(ids, id)
		sts = append(sts, st)
		if len(ids) >= 512 {
			if err := flush(); err != nil {
				return false, err
			}
		}
		return true, nil
	})
	if err != nil {
		return MergedReviewStats{}, err
	}
	if err := flush(); err != nil {
		return MergedReviewStats{}, err
	}
	return s, nil
}

func reviewFilterUnfiltered(f ReviewFilter) bool {
	return f.ParentPath == "" && strings.TrimSpace(f.UnderPath) == "" && f.Query == "" &&
		len(f.PathSegments) == 0 && !f.FoldersOnly && !f.ExcludeRoot &&
		f.StatusSearchType == "" && f.TraversalStatus == "" && f.CopyStatus == "" && f.DeleteStatus == "" &&
		f.TypeFilter == "" && f.DepthOperator == "" && f.DepthValue == nil &&
		f.SizeOperator == "" && f.SizeValue == nil && !f.ExcludeDestinationOnly &&
		f.AfterPath == "" && f.AfterID == "" && f.PathIssueFilter == "" && f.PathIssueCategory == "" &&
		f.FilterMatchedExpr == "" && len(f.FilterArgs) == 0 && !f.NeedsChildAgg
}

func mergedReviewStatsFromCanonical(d *db.DB) (MergedReviewStats, error) {
	read := func(key string) (int64, error) {
		return d.Ops().GetStat(key)
	}
	var s MergedReviewStats
	var folders, files, excluded int64
	var copyPending, copySuccessful, copyFailed int64
	var err error
	if folders, err = read(db.ReviewKeyFolders); err != nil {
		return MergedReviewStats{}, err
	}
	if files, err = read(db.ReviewKeyFiles); err != nil {
		return MergedReviewStats{}, err
	}
	if excluded, err = read(db.ReviewKeyExcluded); err != nil {
		return MergedReviewStats{}, err
	}
	if copyPending, err = read(db.ReviewKeyCopyPending); err != nil {
		return MergedReviewStats{}, err
	}
	if copySuccessful, err = read(db.ReviewKeyCopySuccessful); err != nil {
		return MergedReviewStats{}, err
	}
	if copyFailed, err = read(db.ReviewKeyCopyFailed); err != nil {
		return MergedReviewStats{}, err
	}
	s.Folders = int(folders)
	s.Files = int(files)
	s.Excluded = int(excluded)
	if s.SizeSrc, err = read(db.ReviewKeySizeSrc); err != nil {
		return MergedReviewStats{}, err
	}
	if s.SizeDst, err = read(db.ReviewKeySizeDst); err != nil {
		return MergedReviewStats{}, err
	}
	if s.SizeSelected, err = read(db.ReviewKeySizeSelected); err != nil {
		return MergedReviewStats{}, err
	}
	// Total is SRC paths by copy outcome + excluded (not pending-only folders/files).
	s.Total = int(copyPending + copySuccessful + copyFailed + excluded)
	return s, nil
}
