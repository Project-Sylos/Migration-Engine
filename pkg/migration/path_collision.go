// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"encoding/json"
	"fmt"
	"path"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/go-path-linter/pkg/gpl"
	"codeberg.org/Sylos/go-path-linter/pkg/issue"
)

// PreviewUniquePathName returns the next free destination basename (base, base (1), …)
// without writing. Used so review UI can label “Accept {name}”.
func (m *Migration) PreviewUniquePathName(nodeID string) (string, error) {
	if m == nil || m.DB == nil {
		return "", fmt.Errorf("migration db not open")
	}
	if !m.PathChecksEnabled() {
		return "", fmt.Errorf("%s", PathChecksNotApplicableMessage)
	}
	node, err := pull.GetNodeByID(m.DB, "SRC", nodeID)
	if err != nil {
		return "", err
	}
	if node == nil {
		return "", fmt.Errorf("node %s not found", nodeID)
	}
	base := preferredUniqueBase(node)
	return m.allocateUniqueSiblingName(nodeID, base, node.Type == db.NodeTypeFile)
}

// AcceptUniquePathName allocates base, base (1), … under Windows-aware sibling rules, then AcceptPathChange.
// When proposedName is non-empty it must pass sibling validation (same rules as remap) before accept.
func (m *Migration) AcceptUniquePathName(nodeID, proposedName string) (string, error) {
	if m == nil || m.DB == nil {
		return "", fmt.Errorf("migration db not open")
	}
	if !m.PathChecksEnabled() {
		return "", fmt.Errorf("%s", PathChecksNotApplicableMessage)
	}
	node, err := pull.GetNodeByID(m.DB, "SRC", nodeID)
	if err != nil {
		return "", err
	}
	if node == nil {
		return "", fmt.Errorf("node %s not found", nodeID)
	}
	unique := strings.TrimSpace(proposedName)
	if unique != "" {
		unique = db.NormalizeNodeBasename(unique)
	}
	if unique == "" || unique == "/" {
		base := preferredUniqueBase(node)
		unique, err = m.allocateUniqueSiblingName(nodeID, base, node.Type == db.NodeTypeFile)
		if err != nil {
			return "", err
		}
	}
	if err := m.AcceptPathChange(nodeID, unique, false); err != nil {
		return "", err
	}
	return unique, nil
}

func preferredUniqueBase(node *db.NodeState) string {
	base := strings.TrimSpace(db.NormalizeNodeBasename(node.Name))
	if node.GPLState == "" {
		return base
	}
	var payload db.GPLStatePayload
	if err := json.Unmarshal([]byte(node.GPLState), &payload); err != nil {
		return base
	}
	if clean := strings.TrimSpace(payload.Part.ProposedClean); clean != "" {
		return clean
	}
	return base
}

func (m *Migration) allocateUniqueSiblingName(nodeID, base string, isFile bool) (string, error) {
	base = db.NormalizeNodeBasename(base)
	if base == "" {
		return "", fmt.Errorf("empty basename for unique name")
	}
	parentPathLen, siblings, _, err := m.loadPathChangeContext(nodeID)
	if err != nil {
		return "", err
	}
	target := m.dstGPLTarget()
	fold := gpl.CaseInsensitiveSiblingTarget(target) || m.WindowsCompat()
	stem, ext := splitNameExt(base, isFile)
	for i := 0; i < 10000; i++ {
		candidate := stem
		if i > 0 {
			candidate = fmt.Sprintf("%s (%d)", stem, i)
		}
		if ext != "" {
			candidate += ext
		}
		taken := false
		for _, s := range siblings {
			if gpl.SiblingNamesEqual(s, candidate, fold) {
				taken = true
				break
			}
		}
		if taken {
			continue
		}
		payload, issues, err := validateProposedPart(target, parentPathLen, candidate, siblings, isFile, m.WindowsCompat())
		if err != nil {
			return "", err
		}
		if payload.Valid && len(issues) == 0 {
			return candidate, nil
		}
		collides := false
		for _, iss := range issues {
			if iss.Category == issue.CategorySiblingCollision {
				collides = true
				break
			}
		}
		if !collides && payload.Valid {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("could not allocate a unique name for %q", base)
}

func splitNameExt(name string, isFile bool) (stem, ext string) {
	if !isFile {
		return name, ""
	}
	ext = path.Ext(name)
	if ext == "" || ext == name {
		return name, ""
	}
	return strings.TrimSuffix(name, ext), ext
}

func collisionSiblingFromIssuesJSON(raw string) string {
	if raw == "" {
		return ""
	}
	var issues []issue.Issue
	if err := json.Unmarshal([]byte(raw), &issues); err != nil {
		return ""
	}
	for _, iss := range issues {
		if iss.Category == issue.CategorySiblingCollision && iss.Detail != "" {
			return iss.Detail
		}
	}
	return ""
}

// PathCollisionSibling returns the colliding DST basename from a GPL IssuesJSON blob.
func PathCollisionSibling(issuesJSON string) string {
	return collisionSiblingFromIssuesJSON(issuesJSON)
}
