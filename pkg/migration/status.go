// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
)

// MigrationStatus summarizes the current state of a migration in the database.
type MigrationStatus struct {
	SrcTotal int
	DstTotal int

	SrcPending int
	DstPending int

	SrcFailed int
	DstFailed int

	MinPendingDepthSrc *int
	MinPendingDepthDst *int
}

// IsEmpty returns true if no nodes have been discovered yet.
func (s MigrationStatus) IsEmpty() bool {
	return s.SrcTotal == 0 && s.DstTotal == 0
}

// HasPending returns true if any src or dst nodes are still pending.
func (s MigrationStatus) HasPending() bool {
	return s.SrcPending > 0 || s.DstPending > 0
}

// HasFailures returns true if any src or dst nodes failed traversal.
func (s MigrationStatus) HasFailures() bool {
	return s.SrcFailed > 0 || s.DstFailed > 0
}

// IsComplete returns true when there are nodes and no pending or failed work.
func (s MigrationStatus) IsComplete() bool {
	if s.IsEmpty() {
		return false
	}
	return !s.HasPending() && !s.HasFailures()
}

// InspectMigrationStatus inspects the BadgerDB node data and returns a MigrationStatus.
func InspectMigrationStatus(badgerDB *db.DB) (MigrationStatus, error) {
	if badgerDB == nil {
		return MigrationStatus{}, fmt.Errorf("badgerDB cannot be nil")
	}

	status := MigrationStatus{}

	// Count all src nodes
	srcPrefix := db.PrefixForAllLevels("src")
	srcTotal, err := badgerDB.CountByPrefix(srcPrefix)
	if err != nil {
		return MigrationStatus{}, fmt.Errorf("failed to count src nodes: %w", err)
	}
	status.SrcTotal = srcTotal

	// Count all dst nodes
	dstPrefix := db.PrefixForAllLevels("dst")
	dstTotal, err := badgerDB.CountByPrefix(dstPrefix)
	if err != nil {
		return MigrationStatus{}, fmt.Errorf("failed to count dst nodes: %w", err)
	}
	status.DstTotal = dstTotal

	// Count pending src nodes
	srcPendingPrefix := db.PrefixForAllLevels("src")
	var srcPendingCount int
	err = badgerDB.IterateKeys(db.IteratorOptions{
		Prefix: srcPendingPrefix,
	}, func(key []byte) error {
		keyStr := string(key)
		// Check if key contains pending status
		parts := splitKeyParts(keyStr)
		if len(parts) >= 3 && parts[2] == db.TraversalStatusPending {
			srcPendingCount++
		}
		return nil
	})
	if err != nil {
		return MigrationStatus{}, fmt.Errorf("failed to count pending src nodes: %w", err)
	}
	status.SrcPending = srcPendingCount

	// Count pending dst nodes
	dstPendingPrefix := db.PrefixForAllLevels("dst")
	var dstPendingCount int
	err = badgerDB.IterateKeys(db.IteratorOptions{
		Prefix: dstPendingPrefix,
	}, func(key []byte) error {
		keyStr := string(key)
		parts := splitKeyParts(keyStr)
		if len(parts) >= 3 && parts[2] == db.TraversalStatusPending {
			dstPendingCount++
		}
		return nil
	})
	if err != nil {
		return MigrationStatus{}, fmt.Errorf("failed to count pending dst nodes: %w", err)
	}
	status.DstPending = dstPendingCount

	// Count failed src nodes
	var srcFailedCount int
	err = badgerDB.IterateKeys(db.IteratorOptions{
		Prefix: srcPrefix,
	}, func(key []byte) error {
		keyStr := string(key)
		parts := splitKeyParts(keyStr)
		if len(parts) >= 3 && parts[2] == db.TraversalStatusFailed {
			srcFailedCount++
		}
		return nil
	})
	if err != nil {
		return MigrationStatus{}, fmt.Errorf("failed to count failed src nodes: %w", err)
	}
	status.SrcFailed = srcFailedCount

	// Count failed dst nodes
	var dstFailedCount int
	err = badgerDB.IterateKeys(db.IteratorOptions{
		Prefix: dstPrefix,
	}, func(key []byte) error {
		keyStr := string(key)
		parts := splitKeyParts(keyStr)
		if len(parts) >= 3 && parts[2] == db.TraversalStatusFailed {
			dstFailedCount++
		}
		return nil
	})
	if err != nil {
		return MigrationStatus{}, fmt.Errorf("failed to count failed dst nodes: %w", err)
	}
	status.DstFailed = dstFailedCount

	// Find minimum pending depth for src
	var minSrcDepth *int
	err = badgerDB.IterateNodeStates(db.IteratorOptions{
		Prefix: srcPrefix,
	}, func(key []byte, state *db.NodeState) error {
		keyStr := string(key)
		parts := splitKeyParts(keyStr)
		if len(parts) >= 3 && parts[2] == db.TraversalStatusPending {
			if minSrcDepth == nil || state.Depth < *minSrcDepth {
				d := state.Depth
				minSrcDepth = &d
			}
		}
		return nil
	})
	if err == nil && minSrcDepth != nil {
		status.MinPendingDepthSrc = minSrcDepth
	}

	// Find minimum pending depth for dst
	var minDstDepth *int
	err = badgerDB.IterateNodeStates(db.IteratorOptions{
		Prefix: dstPrefix,
	}, func(key []byte, state *db.NodeState) error {
		keyStr := string(key)
		parts := splitKeyParts(keyStr)
		if len(parts) >= 3 && parts[2] == db.TraversalStatusPending {
			if minDstDepth == nil || state.Depth < *minDstDepth {
				d := state.Depth
				minDstDepth = &d
			}
		}
		return nil
	})
	if err == nil && minDstDepth != nil {
		status.MinPendingDepthDst = minDstDepth
	}

	return status, nil
}

// splitKeyParts splits a BadgerDB key by colons.
func splitKeyParts(key string) []string {
	var parts []string
	lastIdx := 0
	for i, r := range key {
		if r == ':' {
			parts = append(parts, key[lastIdx:i])
			lastIdx = i + 1
		}
	}
	if lastIdx < len(key) {
		parts = append(parts, key[lastIdx:])
	}
	return parts
}
