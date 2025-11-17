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

// InspectMigrationStatus inspects the node tables and returns a MigrationStatus.
// Callers should validate schema beforehand (e.g. via db.ValidateCoreSchema).
func InspectMigrationStatus(database *db.DB) (MigrationStatus, error) {
	if database == nil {
		return MigrationStatus{}, fmt.Errorf("database cannot be nil")
	}

	status := MigrationStatus{}

	if err := singleCount(database, "SELECT COUNT(*) FROM src_nodes", &status.SrcTotal); err != nil {
		return MigrationStatus{}, err
	}
	if err := singleCount(database, "SELECT COUNT(*) FROM dst_nodes", &status.DstTotal); err != nil {
		return MigrationStatus{}, err
	}

	// When counting Pending, if 'initial' is true, only count those where depth_level != 0.
	srcPendingQuery := "SELECT COUNT(*) FROM src_nodes WHERE traversal_status = 'Pending'"
	dstPendingQuery := "SELECT COUNT(*) FROM dst_nodes WHERE traversal_status = 'Pending'"

	if err := singleCount(database, srcPendingQuery, &status.SrcPending); err != nil {
		return MigrationStatus{}, err
	}
	if err := singleCount(database, dstPendingQuery, &status.DstPending); err != nil {
		return MigrationStatus{}, err
	}

	if err := singleCount(database, "SELECT COUNT(*) FROM src_nodes WHERE traversal_status = 'Failed'", &status.SrcFailed); err != nil {
		return MigrationStatus{}, err
	}
	if err := singleCount(database, "SELECT COUNT(*) FROM dst_nodes WHERE traversal_status = 'Failed'", &status.DstFailed); err != nil {
		return MigrationStatus{}, err
	}

	if depth, ok, err := singleMinInt(database, "SELECT MIN(depth_level) FROM src_nodes WHERE traversal_status = 'Pending'"); err != nil {
		return MigrationStatus{}, err
	} else if ok {
		status.MinPendingDepthSrc = &depth
	}

	if depth, ok, err := singleMinInt(database, "SELECT MIN(depth_level) FROM dst_nodes WHERE traversal_status = 'Pending'"); err != nil {
		return MigrationStatus{}, err
	} else if ok {
		status.MinPendingDepthDst = &depth
	}

	return status, nil
}

// singleMinInt runs a MIN() query that may return NULL.
// It returns (value, true, nil) when a non-null value exists,
// (0, false, nil) when the result is NULL, or an error.
func singleMinInt(database *db.DB, query string, args ...any) (int, bool, error) {
	rows, err := database.Query(query, args...)
	if err != nil {
		return 0, false, err
	}
	defer rows.Close()

	if !rows.Next() {
		return 0, false, nil
	}

	var value *int
	if err := rows.Scan(&value); err != nil {
		return 0, false, err
	}
	if value == nil {
		return 0, false, nil
	}
	return *value, true, nil
}
