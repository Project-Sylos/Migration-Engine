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
	return !s.HasPending()
}

// InspectMigrationStatus inspects the BoltDB node data and returns a MigrationStatus.
func InspectMigrationStatus(boltDB *db.DB) (MigrationStatus, error) {
	if boltDB == nil {
		return MigrationStatus{}, fmt.Errorf("boltDB cannot be nil")
	}

	status := MigrationStatus{}

	// Count all src nodes
	srcTotal, err := boltDB.CountNodes("SRC")
	if err != nil {
		return MigrationStatus{}, fmt.Errorf("failed to count src nodes: %w", err)
	}
	status.SrcTotal = srcTotal

	// Count all dst nodes
	dstTotal, err := boltDB.CountNodes("DST")
	if err != nil {
		return MigrationStatus{}, fmt.Errorf("failed to count dst nodes: %w", err)
	}
	status.DstTotal = dstTotal

	// Count pending and failed nodes for src across all levels
	srcLevels, err := boltDB.GetAllLevels("SRC")
	if err != nil {
		return MigrationStatus{}, fmt.Errorf("failed to get src levels: %w", err)
	}

	var srcPendingCount, srcFailedCount int
	var minSrcDepth *int

	for _, level := range srcLevels {
		pendingCount, err := boltDB.CountStatusBucket("SRC", level, db.StatusPending)
		if err == nil {
			srcPendingCount += pendingCount
			if pendingCount > 0 && (minSrcDepth == nil || level < *minSrcDepth) {
				d := level
				minSrcDepth = &d
			}
		}

		failedCount, err := boltDB.CountStatusBucket("SRC", level, db.StatusFailed)
		if err == nil {
			srcFailedCount += failedCount
		}
	}

	status.SrcPending = srcPendingCount
	status.SrcFailed = srcFailedCount
	status.MinPendingDepthSrc = minSrcDepth

	// Count pending and failed nodes for dst across all levels
	dstLevels, err := boltDB.GetAllLevels("DST")
	if err != nil {
		return MigrationStatus{}, fmt.Errorf("failed to get dst levels: %w", err)
	}

	var dstPendingCount, dstFailedCount int
	var minDstDepth *int

	for _, level := range dstLevels {
		pendingCount, err := boltDB.CountStatusBucket("DST", level, db.StatusPending)
		if err == nil {
			dstPendingCount += pendingCount
			if pendingCount > 0 && (minDstDepth == nil || level < *minDstDepth) {
				d := level
				minDstDepth = &d
			}
		}

		failedCount, err := boltDB.CountStatusBucket("DST", level, db.StatusFailed)
		if err == nil {
			dstFailedCount += failedCount
		}
	}

	status.DstPending = dstPendingCount
	status.DstFailed = dstFailedCount
	status.MinPendingDepthDst = minDstDepth

	return status, nil
}
