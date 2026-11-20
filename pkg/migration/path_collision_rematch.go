// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
	queuegpl "codeberg.org/Sylos/Migration-Engine/pkg/queue/gpl"
	"codeberg.org/Sylos/go-path-linter/pkg/gpl"
	"codeberg.org/Sylos/go-path-linter/pkg/issue"
)

// RematchPendingGPLCollisions upgrades soft pending proposals whose cleaned /
// proposed basename already exists as a DST sibling under the mapped parent.
// Safe to call from path-issue listing so review UI offers Merge without a
// full rediscovery when DST compare missed the collision mark.
func (m *Migration) RematchPendingGPLCollisions() (int, error) {
	if m == nil || m.DB == nil {
		return 0, fmt.Errorf("migration db not open")
	}
	if !m.PathChecksEnabled() {
		return 0, nil
	}
	pending, err := m.DB.Ops().ListGPLByStatus(db.GPLIssueStatusPending, 10000)
	if err != nil {
		return 0, err
	}
	if len(pending) == 0 {
		return 0, nil
	}
	recs, err := m.DB.Ops().BatchGetGPL(pending)
	if err != nil {
		return 0, err
	}
	nodes, err := m.DB.Ops().BatchGetNode(opsdb.SideSRC, pending)
	if err != nil {
		return 0, err
	}

	target := m.dstGPLTarget()
	fold := gpl.CaseInsensitiveSiblingTarget(target) || m.WindowsCompat()
	now := time.Now().UnixNano()
	upgraded := 0
	dstSiblingsByParent := map[string]map[string]string{}

	for _, id := range pending {
		rec, ok := recs[id]
		if !ok || rec.Status != db.GPLIssueStatusPending {
			continue
		}
		n, ok := nodes[id]
		if !ok || n.ParentID == "" {
			continue
		}
		if _, mappedAlready, _ := m.DB.Ops().GetMapBySrc(id); mappedAlready {
			continue
		}
		proposed := strings.TrimSpace(rec.ProposedName)
		if proposed == "" {
			proposed = queuegpl.ParseGPLProposedClean(n.GPLState)
		}
		if proposed == "" {
			continue
		}
		mapped, ok, err := m.DB.Ops().GetMapBySrc(n.ParentID)
		if err != nil {
			return upgraded, err
		}
		if !ok || mapped.DstID == "" || mapped.Status != db.IDMapStatusActive {
			continue
		}
		sibs, err := m.dstSiblingNameMap(mapped.DstID, fold, dstSiblingsByParent)
		if err != nil {
			return upgraded, err
		}
		dstName, hit := sibs[rematchFoldKey(proposed, fold)]
		if !hit {
			continue
		}
		issuesJSON := rematchEnsureSiblingCollisionJSON(rec.IssuesJSON, dstName)
		if err := m.DB.PutGPLIssue(id, db.GPLIssueStatusManualReview, dstName, issuesJSON, "", now); err != nil {
			return upgraded, err
		}
		upgraded++
	}
	return upgraded, nil
}

func (m *Migration) dstSiblingNameMap(
	dstParentID string,
	fold bool,
	cache map[string]map[string]string,
) (map[string]string, error) {
	if cached, ok := cache[dstParentID]; ok {
		return cached, nil
	}
	ids, err := m.DB.Ops().ListChildren(opsdb.SideDST, dstParentID, "", 100_000)
	if err != nil {
		return nil, err
	}
	nodes, err := m.DB.Ops().BatchGetNode(opsdb.SideDST, ids)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(ids))
	for _, id := range ids {
		name := db.NormalizeNodeBasename(nodes[id].Name)
		if name == "" {
			continue
		}
		key := rematchFoldKey(name, fold)
		if _, ok := out[key]; !ok {
			out[key] = name
		}
	}
	cache[dstParentID] = out
	return out, nil
}

func rematchFoldKey(name string, fold bool) string {
	if fold {
		return strings.ToLower(name)
	}
	return name
}

func rematchEnsureSiblingCollisionJSON(existing, dstName string) string {
	dstName = strings.TrimSpace(dstName)
	if dstName == "" {
		return existing
	}
	var issues []issue.Issue
	if existing != "" {
		_ = json.Unmarshal([]byte(existing), &issues)
	}
	for i := range issues {
		if issues[i].Category == issue.CategorySiblingCollision {
			if issues[i].Detail == "" {
				issues[i].Detail = dstName
			}
			if issues[i].Part == "" {
				issues[i].Part = dstName
			}
			b, err := json.Marshal(issues)
			if err != nil {
				return existing
			}
			return string(b)
		}
	}
	issues = append([]issue.Issue{{
		Category:    issue.CategorySiblingCollision,
		Part:        dstName,
		Detail:      dstName,
		Message:     fmt.Sprintf("collides with sibling %q", dstName),
		UserMessage: "Name already used in this folder.",
	}}, issues...)
	b, err := json.Marshal(issues)
	if err != nil {
		return existing
	}
	return string(b)
}
