// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
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

// InspectMigrationStatus inspects the DuckDB node data and stats tables and returns a MigrationStatus.
func InspectMigrationStatus(database *db.DB) (MigrationStatus, error) {
	if database == nil {
		return MigrationStatus{}, fmt.Errorf("database cannot be nil")
	}

	status := MigrationStatus{}

	srcTotal, err := db.CountNodes(database, "SRC")
	if err != nil {
		return MigrationStatus{}, fmt.Errorf("failed to count src nodes: %w", err)
	}
	status.SrcTotal = srcTotal

	dstTotal, err := db.CountNodes(database, "DST")
	if err != nil {
		return MigrationStatus{}, fmt.Errorf("failed to count dst nodes: %w", err)
	}
	status.DstTotal = dstTotal

	// Pending/failed totals from src_stats
	c, _ := database.GetStatsCount("SRC", db.StatsKeyTraversalStatus(db.StatusPending))
	status.SrcPending = int(c)
	c, _ = database.GetStatsCount("SRC", db.StatsKeyTraversalStatus(db.StatusFailed))
	status.SrcFailed = int(c)

	// Min pending depth for SRC from stats breakdown
	breakdown, _ := database.GetStatsBreakdown("SRC")
	for _, row := range breakdown {
		if row.Key == db.StatsKeyTraversalStatus(db.StatusPending) && row.Count > 0 {
			if status.MinPendingDepthSrc == nil || row.Depth < *status.MinPendingDepthSrc {
				d := row.Depth
				status.MinPendingDepthSrc = &d
			}
		}
	}

	// Pending/failed totals from dst_stats
	c, _ = database.GetStatsCount("DST", db.StatsKeyTraversalStatus(db.StatusPending))
	status.DstPending = int(c)
	c, _ = database.GetStatsCount("DST", db.StatsKeyTraversalStatus(db.StatusFailed))
	status.DstFailed = int(c)

	// Min pending depth for DST from stats breakdown
	breakdown, _ = database.GetStatsBreakdown("DST")
	for _, row := range breakdown {
		if row.Key == db.StatsKeyTraversalStatus(db.StatusPending) && row.Count > 0 {
			if status.MinPendingDepthDst == nil || row.Depth < *status.MinPendingDepthDst {
				d := row.Depth
				status.MinPendingDepthDst = &d
			}
		}
	}

	return status, nil
}
