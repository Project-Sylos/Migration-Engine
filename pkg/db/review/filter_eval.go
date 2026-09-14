// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"strings"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/filter"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func nodeInputFromRecord(n opsdb.NodeRecord, childCount *int) filter.NodeInput {
	var nt string
	switch n.Type {
	case opsdb.NodeTypeFolder:
		nt = filter.NodeFolder
	case opsdb.NodeTypeFile:
		nt = filter.NodeFile
	default:
		nt = n.Type
	}
	in := filter.NodeInput{
		Name:        n.Name,
		DisplayPath: n.DisplayPath,
		Size:        n.Size,
		MTime:       n.MTime,
		Depth:       n.Depth,
		NodeType:    nt,
		ChildCount:  childCount,
	}
	if in.DisplayPath == "" {
		in.DisplayPath = n.Path
	}
	if childCount != nil {
		empty := *childCount == 0
		in.IsEmpty = &empty
	}
	return in
}

func enrichNodeInputReview(ops *opsdb.Store, side string, n opsdb.NodeRecord, in *filter.NodeInput) error {
	if ops == nil || in == nil || n.ID == "" {
		return nil
	}
	st, _, err := ops.GetStatus(side, n.ID)
	if err != nil {
		return err
	}
	in.ReviewStatus = effectiveReviewStatus(st)
	if side != opsdb.SideSRC {
		return nil
	}
	gpl, ok, err := ops.GetGPL(n.ID)
	if err != nil {
		return err
	}
	if !ok {
		in.PathIssueStatus = "none"
		return nil
	}
	switch gpl.Status {
	case db.GPLIssueStatusPending:
		in.PathIssueStatus = "issues"
	case "manual_review":
		in.PathIssueStatus = "manual"
	case db.GPLIssueStatusAccepted:
		in.PathIssueStatus = "accepted"
	default:
		if st.GPLStatus == db.GPLStatusIgnored {
			in.PathIssueStatus = "rejected"
		} else {
			in.PathIssueStatus = gpl.Status
		}
	}
	in.PathIssueCategory = gpl.IssuesJSON
	return nil
}

// effectiveReviewStatus maps sealed overlay fields to UI review_status tokens.
// Prefers copy-phase meaning for "pending" (will be copied).
func effectiveReviewStatus(st opsdb.StatusRecord) string {
	if strings.EqualFold(st.TraversalStatus, db.StatusNotOnSrc) || strings.EqualFold(st.TraversalStatus, "not_on_src") {
		return "not_on_src"
	}
	if st.CopyStatus == db.CopyStatusExcludedExplicit || st.CopyStatus == db.CopyStatusExcludedInherited ||
		st.TraversalStatus == db.StatusExcluded || st.TraversalStatus == db.StatusExclusionInherited {
		return "excluded"
	}
	if strings.EqualFold(st.CopyStatus, db.StatusFailed) || strings.EqualFold(st.TraversalStatus, db.StatusFailed) {
		return "failed"
	}
	if db.CopyStatusIsPending(st.CopyStatus) || st.CopyStatus == "" {
		if strings.EqualFold(st.TraversalStatus, db.StatusPending) {
			return "pending_retry"
		}
		return "pending"
	}
	if st.CopyStatus == db.CopyStatusSuccessful || st.CopyStatus == db.CopyStatusAlreadyExisted {
		return "successful"
	}
	if strings.EqualFold(st.TraversalStatus, db.StatusPending) {
		return "pending_retry"
	}
	if strings.EqualFold(st.TraversalStatus, db.StatusSuccessful) {
		return "successful"
	}
	return strings.TrimSpace(st.CopyStatus)
}

func nodeMatchesCompiledFilter(ops *opsdb.Store, side string, n opsdb.NodeRecord, compiled *filter.CompiledRuleset) (bool, error) {
	if compiled == nil {
		return true, nil
	}
	var cc *int
	if n.Type == opsdb.NodeTypeFolder || n.Type == "folder" {
		packs, err := ops.BatchGetKids(side, []string{n.ID})
		if err != nil {
			return false, err
		}
		c := len(packs[n.ID])
		if c == 0 {
			childIDs, err := ops.ListChildren(side, n.ID, "", 100_000)
			if err != nil {
				return false, err
			}
			c = len(childIDs)
		}
		cc = &c
	}
	in := nodeInputFromRecord(n, cc)
	if err := enrichNodeInputReview(ops, side, n, &in); err != nil {
		return false, err
	}
	out := compiled.EvaluateDetermining(in, time.Now().UTC())
	return out.Outcome == filter.OutcomeExcluded, nil
}

func nodeMatchesFilterOrExpr(ops *opsdb.Store, side string, n opsdb.NodeRecord, f ReviewFilter) (bool, error) {
	if f.CompiledFilter != nil {
		return nodeMatchesCompiledFilter(ops, side, n, f.CompiledFilter)
	}
	if strings.TrimSpace(f.FilterMatchedExpr) != "" {
		// Legacy SQL expr without CompiledFilter: cannot evaluate on Badger path.
		return false, nil
	}
	return true, nil
}
