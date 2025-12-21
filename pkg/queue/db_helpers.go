// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"

	"github.com/Project-Sylos/Sylos-DB/pkg/bolt"
	"github.com/Project-Sylos/Sylos-DB/pkg/store"
)

// Helper functions for loading data from the database using Store API.

// LoadRootFolders loads root folders (depth 0) from the database.
func LoadRootFolders(s *store.Store, queueType string) ([]*bolt.NodeState, error) {
	return s.ListPendingAtLevel(queueType, 0, 0) // 0 limit = no limit
}

// LoadPendingFolders loads pending folders at a specific level.
func LoadPendingFolders(s *store.Store, queueType string, level int, limit int) ([]*bolt.NodeState, error) {
	return s.ListPendingAtLevel(queueType, level, limit)
}

// LoadExpectedChildren loads expected children for a given parent node.
func LoadExpectedChildren(s *store.Store, queueType string, parentID string) ([]string, error) {
	result, err := s.GetChildren(queueType, parentID, "ids")
	if err != nil {
		return nil, err
	}
	childIDs, ok := result.([]string)
	if !ok {
		return nil, fmt.Errorf("failed to convert children result to []string")
	}
	return childIDs, nil
}

// BatchLoadExpectedChildrenByDSTIDs loads expected children for multiple DST parent IDs.
// Returns a map of DST ID -> list of child IDs.
func BatchLoadExpectedChildrenByDSTIDs(s *store.Store, dstIDs []string) (map[string][]string, error) {
	result := make(map[string][]string)
	
	for _, dstID := range dstIDs {
		childResult, err := s.GetChildren("DST", dstID, "ids")
		if err != nil {
			// If children not found, just skip
			continue
		}
		childIDs, ok := childResult.([]string)
		if !ok {
			continue
		}
		result[dstID] = childIDs
	}
	
	return result, nil
}
