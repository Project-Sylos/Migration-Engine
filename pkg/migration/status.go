// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
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

// InspectMigrationStatus reads delta-maintained traversal counters from src_stats/dst_stats.
func InspectMigrationStatus(database *db.DB) (MigrationStatus, error) {
	if database == nil {
		return MigrationStatus{}, fmt.Errorf("database cannot be nil")
	}

	status := MigrationStatus{}

	srcCounts, err := stats.GetTraversalStatusCounts(database, "SRC")
	if err != nil {
		return MigrationStatus{}, fmt.Errorf("failed to get SRC traversal counts: %w", err)
	}
	status.SrcPending = int(srcCounts.Pending)
	status.SrcFailed = int(srcCounts.Failed)
	status.SrcTotal = int(srcCounts.Pending + srcCounts.Successful + srcCounts.Failed + srcCounts.NotOnSrc + srcCounts.Excluded)

	dstCounts, err := stats.GetTraversalStatusCounts(database, "DST")
	if err != nil {
		return MigrationStatus{}, fmt.Errorf("failed to get DST traversal counts: %w", err)
	}
	status.DstPending = int(dstCounts.Pending)
	status.DstFailed = int(dstCounts.Failed)
	status.DstTotal = int(dstCounts.Pending + dstCounts.Successful + dstCounts.Failed + dstCounts.NotOnSrc + dstCounts.Excluded)

	if d, err := stats.MinPendingTraversalDepth(database, "SRC"); err == nil {
		status.MinPendingDepthSrc = d
	}
	if d, err := stats.MinPendingTraversalDepth(database, "DST"); err == nil {
		status.MinPendingDepthDst = d
	}

	return status, nil
}
