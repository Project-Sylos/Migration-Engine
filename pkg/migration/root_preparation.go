// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

// RootChildSeed is one immediate child under the migration root, captured during
// root-pick review and applied at AddRoots (not earlier).
type RootChildSeed struct {
	ServiceID string `json:"serviceId"`
	Name      string `json:"name"`
	Type      string `json:"type"` // folder | file
	Size      int64  `json:"size,omitempty"`
	MTime     string `json:"mtime,omitempty"`
	Excluded  bool   `json:"excluded,omitempty"` // SRC only
	DstOnly   bool   `json:"dstOnly,omitempty"`  // DST only
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
	if p.SourcePrepared {
		return 1
	}
	return 0
}

// DestStartRound returns the BFS round DST queues should start at after seeding.
func (p RootPreparation) DestStartRound() int {
	if p.DestPrepared {
		return 1
	}
	return 0
}
