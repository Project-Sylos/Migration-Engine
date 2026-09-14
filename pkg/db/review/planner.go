// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"fmt"
	"sort"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

const reviewPlannerScanCap = 200_000

type plannerPlan struct {
	kind   string // path, size, mtime, name, seg, all
	ids    []string
	est    int64
	reason string
}

// planReviewCandidateIDs picks the cheapest Badger secondary index and returns candidate ids.
// Path segments vs trigrams: docs/search_indexes.md.
func planReviewCandidateIDs(ops *opsdb.Store, side string, f ReviewFilter, limit int) (plannerPlan, error) {
	if ops == nil {
		return plannerPlan{}, fmt.Errorf("review planner: nil ops")
	}
	scanLimit := limit * 8
	if scanLimit < 5000 {
		scanLimit = 5000
	}
	if scanLimit > reviewPlannerScanCap {
		scanLimit = reviewPlannerScanCap
	}

	under := strings.TrimSpace(f.UnderPath)
	if under != "" && under != "/" {
		ids, err := ops.ListSubtreeIDs(side, under, scanLimit)
		return plannerPlan{kind: "path", ids: ids, est: int64(len(ids)), reason: "under_path"}, err
	}

	var sizeEst, mtimeEst int64 = -1, -1
	var sizeLo, sizeHi *int64
	if f.SizeValue != nil && f.SizeOperator != "" {
		sizeLo, sizeHi = sizeBounds(f.SizeOperator, *f.SizeValue)
		if sizeLo != nil && sizeHi != nil {
			var err error
			sizeEst, err = ops.EstimateSizeBucketSum(side, *sizeLo, *sizeHi)
			if err != nil {
				return plannerPlan{}, err
			}
		}
	}

	nameQ := strings.TrimSpace(f.Query)
	nameField := strings.TrimSpace(f.QueryField)
	segments := pathSegmentsForFilter(f)

	if sizeEst >= 0 && sizeLo != nil && sizeHi != nil && (mtimeEst < 0 || sizeEst <= mtimeEst) {
		ids, err := ops.ScanSizeRange(side, *sizeLo, *sizeHi, scanLimit)
		return plannerPlan{kind: "size", ids: ids, est: sizeEst, reason: "size_range"}, err
	}

	if nameQ != "" && strings.EqualFold(nameField, "name") && !strings.Contains(nameQ, "%") && !strings.HasPrefix(nameQ, "*") {
		tok := strings.ToLower(nameQ)
		if plan, ok, err := planSegOrNameCandidates(ops, side, tok, scanLimit); err != nil {
			return plannerPlan{}, err
		} else if ok {
			return plan, nil
		}
		// Fall through: mid-string contains may still use trigrams below.
	}

	if len(segments) > 0 {
		first := strings.ToLower(strings.TrimSpace(segments[0]))
		if first != "" && !strings.ContainsAny(first, "*%") {
			anchorIDs, err := ops.ScanSegToken(side, first, scanLimit)
			if err != nil {
				return plannerPlan{}, err
			}
			if len(anchorIDs) == 0 {
				anchorIDs, err = ops.ScanNamePrefix(side, first, scanLimit)
				if err != nil {
					return plannerPlan{}, err
				}
			}
			if len(anchorIDs) > 0 {
				ids, err := expandPathSegmentAnchorIDs(ops, side, anchorIDs, scanLimit)
				return plannerPlan{kind: "path_seg", ids: ids, est: int64(len(ids)), reason: "path_seg_anchors"}, err
			}
		}
		// Substring segment with no index hit: prefer trigram, else capped path scan.
	}

	if plan, ok, err := planTrigramContains(ops, side, f, scanLimit); err != nil {
		return plannerPlan{}, err
	} else if ok {
		return plan, nil
	}

	if len(segments) > 0 {
		ids, err := ops.ListSubtreeIDs(side, "/", scanLimit)
		return plannerPlan{kind: "all", ids: ids, est: int64(len(ids)), reason: "path_segments_scan"}, err
	}

	if under == "/" || under == "" {
		ids, err := ops.ListSubtreeIDs(side, "/", scanLimit)
		return plannerPlan{kind: "all", ids: ids, est: int64(len(ids)), reason: "full_path_scan"}, err
	}
	ids, err := ops.ListSubtreeIDs(side, "/", scanLimit)
	return plannerPlan{kind: "all", ids: ids, est: int64(len(ids)), reason: "fallback"}, err
}

func planSegOrNameCandidates(ops *opsdb.Store, side, tok string, scanLimit int) (plannerPlan, bool, error) {
	if tok == "" || strings.ContainsAny(tok, "*%") {
		return plannerPlan{}, false, nil
	}
	ids, err := ops.ScanSegToken(side, tok, scanLimit)
	if err != nil {
		return plannerPlan{}, false, err
	}
	if len(ids) > 0 {
		return plannerPlan{kind: "seg", ids: ids, est: int64(len(ids)), reason: "seg"}, true, nil
	}
	ids, err = ops.ScanNamePrefix(side, tok, scanLimit)
	if err != nil {
		return plannerPlan{}, false, err
	}
	if len(ids) > 0 {
		return plannerPlan{kind: "name", ids: ids, est: int64(len(ids)), reason: "name_prefix"}, true, nil
	}
	return plannerPlan{}, false, nil
}

// expandPathSegmentAnchorIDs unions each anchor with its path subtree so "in path"
// segment search includes descendants (Duck EXISTS underCand semantics).
func expandPathSegmentAnchorIDs(ops *opsdb.Store, side string, anchorIDs []string, scanLimit int) ([]string, error) {
	nodes, err := ops.BatchGetNode(side, anchorIDs)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(anchorIDs)*8)
	out := make([]string, 0, len(anchorIDs)*8)
	add := func(id string) bool {
		if _, ok := seen[id]; ok {
			return false
		}
		seen[id] = struct{}{}
		out = append(out, id)
		return len(out) >= scanLimit
	}
	perAnchor := scanLimit
	if n := len(anchorIDs); n > 0 {
		perAnchor = scanLimit / n
		if perAnchor < 100 {
			perAnchor = 100
		}
	}
	for _, id := range anchorIDs {
		if add(id) {
			return out, nil
		}
		n, ok := nodes[id]
		if !ok {
			continue
		}
		sub, err := ops.ListSubtreeIDs(side, n.Path, perAnchor)
		if err != nil {
			return nil, err
		}
		for _, sid := range sub {
			if add(sid) {
				return out, nil
			}
		}
	}
	return out, nil
}

func sizeBounds(op string, v int64) (lo, hi *int64) {
	min := int64(0)
	max := int64(^uint64(0) >> 1)
	switch strings.ToLower(strings.TrimSpace(op)) {
	case "eq", "=", "equals":
		return &v, &v
	case "gt", ">":
		x := v + 1
		return &x, &max
	case "gte", ">=":
		return &v, &max
	case "lt", "<":
		x := v - 1
		if x < 0 {
			x = 0
		}
		return &min, &x
	case "lte", "<=":
		return &min, &v
	default:
		return nil, nil
	}
}

func nodeMatchesMetaFilter(ops *opsdb.Store, side string, n opsdb.NodeRecord, f ReviewFilter) bool {
	path := opsdb.NormalizeIndexPath(n.Path)
	if f.ParentPath != "" && opsdb.NormalizeIndexPath(n.ParentPath) != db.NormalizeRootRelativePath(f.ParentPath) {
		return false
	}
	under := strings.TrimSpace(f.UnderPath)
	if under != "" && under != "/" {
		root := opsdb.NormalizeIndexPath(under)
		if path != root && !strings.HasPrefix(path, root+"/") {
			return false
		}
	}
	if f.ExcludeRoot && path == "/" {
		return false
	}
	if f.FoldersOnly || strings.EqualFold(f.TypeFilter, "folder") {
		if n.Type != db.NodeTypeFolder && n.Type != opsdb.NodeTypeFolder {
			return false
		}
	} else if strings.EqualFold(f.TypeFilter, "file") {
		if n.Type != db.NodeTypeFile && n.Type != opsdb.NodeTypeFile {
			return false
		}
	}
	if f.DepthValue != nil && f.DepthOperator != "" {
		if !compareInt(n.Depth, *f.DepthValue, f.DepthOperator) {
			return false
		}
	}
	if f.SizeValue != nil && f.SizeOperator != "" {
		if !compareInt64(n.Size, *f.SizeValue, f.SizeOperator) {
			return false
		}
	}
	name := strings.ToLower(n.Name)
	q := strings.ToLower(strings.TrimSpace(f.Query))
	qf := strings.TrimSpace(f.QueryField)
	segments := pathSegmentsForFilter(f)
	if q != "" && strings.EqualFold(qf, "name") {
		if !strings.Contains(name, q) {
			return false
		}
	} else if q != "" && len(segments) == 0 && !strings.EqualFold(qf, "path") {
		if !strings.Contains(name, q) && !strings.Contains(strings.ToLower(path), q) {
			return false
		}
	}
	if len(segments) > 0 {
		if !pathSegmentsMatchAncestry(ops, side, n, segments) {
			return false
		}
	}
	if ap := strings.TrimSpace(f.AfterPath); ap != "" || strings.TrimSpace(f.AfterID) != "" {
		ap = db.NormalizeRootRelativePath(ap)
		aid := strings.TrimSpace(f.AfterID)
		if path < ap || (path == ap && n.ID <= aid) {
			return false
		}
	}
	return true
}

// pathSegmentsMatchAncestry mirrors Duck EXISTS ancestry: ordered name-contains
// matches along the root→node chain (gaps allowed).
func pathSegmentsMatchAncestry(ops *opsdb.Store, side string, n opsdb.NodeRecord, segments []string) bool {
	segments = NormalizePathSegments(segments)
	if len(segments) == 0 {
		return true
	}
	if ops == nil {
		return false
	}
	chain := []opsdb.NodeRecord{n}
	cur := n
	for i := 0; i < 64 && cur.ParentID != ""; i++ {
		p, ok, err := ops.GetNode(side, cur.ParentID)
		if err != nil || !ok {
			break
		}
		chain = append(chain, p)
		cur = p
		if opsdb.NormalizeIndexPath(cur.Path) == "/" {
			break
		}
	}
	// chain is leaf→root; reverse to root→leaf
	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}
	si := 0
	for _, node := range chain {
		if si >= len(segments) {
			break
		}
		if strings.Contains(strings.ToLower(node.Name), strings.ToLower(segments[si])) {
			si++
		}
	}
	return si == len(segments)
}

func sortCandidateIDsByPath(nodes map[string]opsdb.NodeRecord, ids []string, sortCol string, sortDesc bool) {
	sort.SliceStable(ids, func(i, j int) bool {
		a, b := nodes[ids[i]], nodes[ids[j]]
		var less bool
		switch sortCol {
		case "name":
			less = strings.ToLower(a.Name) < strings.ToLower(b.Name)
			if a.Name == b.Name {
				less = a.Path < b.Path
			}
		case "size":
			less = a.Size < b.Size
			if a.Size == b.Size {
				less = a.Path < b.Path
			}
		case "depth":
			less = a.Depth < b.Depth
			if a.Depth == b.Depth {
				less = a.Path < b.Path
			}
		default: // path
			less = a.Path < b.Path
			if a.Path == b.Path {
				less = a.ID < b.ID
			}
		}
		if sortDesc {
			return !less
		}
		return less
	})
}
