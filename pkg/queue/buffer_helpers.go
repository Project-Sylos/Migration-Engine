// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"github.com/Project-Sylos/Sylos-DB/pkg/store"
)

// Helper functions for queueing write operations using Store API.

// queueStatusUpdate transitions node status using Store API.
func queueStatusUpdate(s *store.Store, queueType string, level int, oldStatus, newStatus, nodeID string) error {
	return s.TransitionNodeStatus(queueType, level, oldStatus, newStatus, nodeID)
}

// queueExclusionUpdate marks nodes as excluded using Store API.
func queueExclusionUpdate(s *store.Store, queueType string, nodeID string, inherited bool) error {
	return s.MarkExcluded(queueType, nodeID, inherited)
}

// queuePathToULIDMapping creates a path-to-ULID mapping using Store API.
func queuePathToULIDMapping(s *store.Store, queueType string, path string, nodeID string) error {
	return s.SetPathMapping(queueType, path, nodeID)
}

// queueLookupMapping creates a join mapping (SRC<->DST) using Store API.
func queueLookupMapping(s *store.Store, srcID string, dstID string) error {
	return s.SetJoinMapping(srcID, dstID)
}
