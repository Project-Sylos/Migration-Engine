// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

// Traversal status values (event-derived). Stats keys use: pending, successful, failed, not_on_src (DST only).
const (
	StatusPending           = "pending"
	StatusSuccessful        = "successful"
	StatusFailed            = "failed"
	StatusNotOnSrc          = "not_on_src"          // DST only
	StatusExcluded          = "excluded"
	StatusExclusionInherited = "exclusion_inherited" // bulk subtree exclusion
)

// Copy status values (src_nodes only). Stats keys use: pending, successful, failed (no in_progress in stats).
const (
	CopyStatusPending           = "pending"
	CopyStatusInProgress        = "in_progress" // transient; not stored in stats
	CopyStatusSuccessful        = "successful"
	CopyStatusFailed            = "failed"
	CopyStatusSkipped           = "skipped"
	CopyStatusExcludedExplicit  = "excluded_explicit"  // user excluded this node from copy
	CopyStatusExcludedInherited = "excluded_inherited" // excluded by parent folder
	CopyStatusExcluded          = "excluded"          // display value for UI (both explicit and inherited)
)

// Delete status values (src_status_events only). Stats keys use: pending, deleted, failed.
const (
	DeleteStatusPending = "pending"
	DeleteStatusDeleted = "deleted"
	DeleteStatusFailed  = "failed"
	DeleteStatusSkipped = "skipped" // opted out of source removal during cleanup planning
)

// DeletePendingIfCopySuccessful returns delete_status=pending when copy becomes successful and delete is not yet set.
func DeletePendingIfCopySuccessful(copyStatus, currentDeleteStatus string) string {
	if copyStatus != CopyStatusSuccessful {
		return ""
	}
	switch currentDeleteStatus {
	case DeleteStatusPending, DeleteStatusDeleted, DeleteStatusFailed, DeleteStatusSkipped:
		return ""
	}
	return DeleteStatusPending
}

// CopyStatusForDisplay returns the copy_status to show in the UI. Internal excluded_explicit and excluded_inherited both become "excluded".
func CopyStatusForDisplay(status string) string {
	switch status {
	case CopyStatusExcludedExplicit, CopyStatusExcludedInherited:
		return CopyStatusExcluded
	}
	return status
}

// Node type values.
const (
	NodeTypeFolder = "folder"
	NodeTypeFile   = "file"
)
