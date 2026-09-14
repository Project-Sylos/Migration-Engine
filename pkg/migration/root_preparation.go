// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import "strings"

// RootChildSeed is one child under a prepared parent (depth-1 under root, or nested).
// Captured during root-pick review and applied at AddRoots (not earlier).
// Excluded children are omitted from the DB (silent allowlist gap), not counted excluded.
type RootChildSeed struct {
	ServiceID string `json:"serviceId"`
	Name      string `json:"name"`
	Type      string `json:"type"` // folder | file
	Size      int64  `json:"size,omitempty"`
	MTime     string `json:"mtime,omitempty"`
	Excluded  bool   `json:"excluded,omitempty"` // SRC only: omit from seed / allowlist
	DstOnly   bool   `json:"dstOnly,omitempty"`  // DST only
	// Children are nested included/excluded siblings when the UI visited this folder.
	// Empty Children on an included folder means unrestricted subtree (pending traversal).
	Children []RootChildSeed `json:"children,omitempty"`
	// IncludeOnly lists child service IDs when this folder is partial; optional — also
	// inferred from Excluded siblings in Children.
	IncludeOnly []string `json:"includeOnly,omitempty"`
}

// RootPreparation carries UI-reviewed children for one or both sides.
// Empty / unprepared sides keep classic engine round 0.
type RootPreparation struct {
	SourcePrepared bool           `json:"sourcePrepared,omitempty"`
	DestPrepared   bool           `json:"destPrepared,omitempty"`
	SourceChildren []RootChildSeed `json:"sourceChildren,omitempty"`
	DestChildren   []RootChildSeed `json:"destChildren,omitempty"`
}

// SourceStartRound returns the BFS round SRC queues should start at after seeding.
func (p RootPreparation) SourceStartRound() int {
	if !p.SourcePrepared {
		return 0
	}
	return ComputeSourceStartRound(p.SourceChildren)
}

// DestStartRound returns the BFS round DST queues should start at after seeding.
func (p RootPreparation) DestStartRound() int {
	if p.DestPrepared {
		return 1
	}
	return 0
}

// ComputeSourceStartRound is the minimum depth of included folders that still need
// ListChildren (no nested Children in the prep payload). Defaults to 1.
func ComputeSourceStartRound(children []RootChildSeed) int {
	minDepth := -1
	var walk func([]RootChildSeed, int)
	walk = func(kids []RootChildSeed, depth int) {
		for _, c := range kids {
			if c.Excluded || c.Name == "" {
				continue
			}
			if strings.EqualFold(c.Type, "file") {
				continue
			}
			if len(c.Children) > 0 {
				walk(c.Children, depth+1)
				continue
			}
			if minDepth < 0 || depth < minDepth {
				minDepth = depth
			}
		}
	}
	walk(children, 1)
	if minDepth < 0 {
		return 1
	}
	return minDepth
}
