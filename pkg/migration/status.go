// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"

	"github.com/Project-Sylos/Sylos-DB/pkg/store"
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

// InspectMigrationStatus inspects the Store and returns a MigrationStatus.
func InspectMigrationStatus(storeInstance *store.Store) (MigrationStatus, error) {
	if storeInstance == nil {
		return MigrationStatus{}, fmt.Errorf("storeInstance cannot be nil")
	}

	dbReport, err := storeInstance.InspectDatabase(store.InspectionModeStats)
	if err != nil {
		return MigrationStatus{}, err
	}

	status := MigrationStatus{
		SrcTotal: dbReport.Src.TotalNodes,
		DstTotal: dbReport.Dst.TotalNodes,

		SrcPending: dbReport.Src.TotalPending,
		DstPending: dbReport.Dst.TotalPending,

		SrcFailed: dbReport.Src.TotalFailed,
		DstFailed: dbReport.Dst.TotalFailed,

		MinPendingDepthSrc: dbReport.Src.MinPendingLevel,
		MinPendingDepthDst: dbReport.Dst.MinPendingLevel,
	}

	// Optional: treat "not_on_src" as a failure signal for DST in UI/status reporting.
	// (We keep it separate in VerifyMigration where it is explicitly reported.)
	if status.DstFailed == 0 && dbReport.Dst.TotalNotOnSrc > 0 {
		status.DstFailed = dbReport.Dst.TotalNotOnSrc
	}

	return status, nil
}
