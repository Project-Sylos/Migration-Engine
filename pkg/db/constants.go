// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

// Traversal status values (event-derived). Stats keys use: pending, successful, failed, not_on_src (DST only).
const (
	StatusPending            = "pending"
	StatusSuccessful         = "successful"
	StatusFailed             = "failed"
	StatusNotOnSrc           = "not_on_src"          // DST only
	StatusExcluded           = "excluded"
	StatusExclusionInherited = "exclusion_inherited" // bulk subtree exclusion
)

// Copy status values (src_nodes only). Stats keys use: pending, successful, failed (no in_progress in stats).
const (
	CopyStatusPending           = "pending"
	CopyStatusInProgress        = "in_progress" // transient; not stored in stats
	CopyStatusSuccessful        = "successful"
	CopyStatusAlreadyExisted    = "already_existed" // SRC root or DST traversal match; never copied by this migration's copy phase
	CopyStatusFailed            = "failed"
	CopyStatusSkipped           = "skipped"
	CopyStatusExcludedExplicit  = "excluded_explicit"  // user excluded this node from copy
	CopyStatusExcludedInherited = "excluded_inherited" // excluded by parent folder
	CopyStatusExcluded          = "excluded"           // display value for UI (both explicit and inherited)
)

// SQLCopyStatusCompleteIN is the SQL IN-list for copy-complete statuses (progress / delete eligibility).
const SQLCopyStatusCompleteIN = `('successful','already_existed')`

// SQLCopyStatusExcludedIN is the SQL IN-list for copy exclusion statuses.
const SQLCopyStatusExcludedIN = `('excluded_explicit','excluded_inherited')`

// CopyStatusIsComplete reports whether copy work is satisfied (actual copy or already on DST).
func CopyStatusIsComplete(s string) bool {
	switch s {
	case CopyStatusSuccessful, CopyStatusAlreadyExisted:
		return true
	default:
		return false
	}
}

// CopyStatusEligibleForDelete reports whether the node may enter source cleanup (copy satisfied).
func CopyStatusEligibleForDelete(s string) bool {
	return CopyStatusIsComplete(s)
}

// CopyStatusIsActualCopy reports whether this migration's copy phase wrote the item.
func CopyStatusIsActualCopy(s string) bool {
	return s == CopyStatusSuccessful
}

// Delete status values (src_status_events only). Stats keys use: pending, deleted, failed.
const (
	DeleteStatusPending = "pending"
	DeleteStatusDeleted = "deleted"
	DeleteStatusFailed  = "failed"
	DeleteStatusSkipped = "skipped" // opted out of source removal during cleanup planning
)

// DeletePendingIfCopySuccessful returns delete_status=pending when copy becomes complete and delete is not yet set.
func DeletePendingIfCopySuccessful(copyStatus, currentDeleteStatus string) string {
	if !CopyStatusIsComplete(copyStatus) {
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

// GPL status values (src_status_events / dst_status_events). Path-scoped cascade revalidation.
const (
	GPLStatusPending    = "pending"
	GPLStatusSuccessful = "successful"
	GPLStatusFailed     = "failed"
	GPLStatusIgnored    = "ignored" // user dismissed warnings for this node + subtree
)
