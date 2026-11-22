// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"path/filepath"
	"strings"
)

// Traversal status values (event-derived). Stats keys use: pending, successful, failed, not_on_src (DST only), excluded (SRC).
const (
	StatusPending            = "pending"
	StatusSuccessful         = "successful"
	StatusFailed             = "failed"
	StatusNotOnSrc           = "not_on_src"          // Legacy DST-only marker; new discovery uses pending/successful and walks DST-only folders
	StatusExcluded           = "excluded"            // SRC only: user excluded; do not traverse subtree (counts as 1 item)
	StatusExclusionInherited = "exclusion_inherited" // SRC only: excluded because an ancestor was excluded
	StatusSilentExcluded     = "silent_excluded"     // SRC only: off-path / prep allowlist skip; not counted in stats
)

// Copy status values.
// SRC: hot-path copy queue / progress (pending, successful, already_existed, failed, excluded_*).
// DST: bookkeeping + review search only (destination_only, already_existed, needs_overwrite, successful).
// Stats keys use SRC: pending, successful, failed (no in_progress in stats).
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

	// DST-only bookkeeping (st:copy:dst). Not used on the SRC copy frontier.
	CopyStatusDestinationOnly = "destination_only" // unpaired DEST; never copied
	CopyStatusNeedsOverwrite  = "needs_overwrite"  // paired; SRC will replace this DEST object
)

// SQLCopyStatusCompleteIN is the SQL IN-list for copy-complete statuses (progress / delete eligibility).
const SQLCopyStatusCompleteIN = `('successful','already_existed')`

// SQLCopyStatusExcludedIN is the SQL IN-list for copy exclusion statuses.
const SQLCopyStatusExcludedIN = `('excluded_explicit','excluded_inherited')`

const ExclusionSourceManual = "manual"

// Delete-subtree eligibility (copy-complete SRC nodes under a path).
const successfulCopyEligibleForDelete = `COALESCE(cur.copy_status,'') IN ` + SQLCopyStatusCompleteIN

const (
	// Skip-eligible: unset or any pending* delete status (will become skipped).
	SQLDeleteSubtreeSkipEligible = successfulCopyEligibleForDelete + ` AND COALESCE(cur.delete_status,'') IN ('pending_explicit','pending_inherited','')`
	// Unskip-eligible: currently skipped (will become pending*).
	SQLDeleteSubtreeUnskipEligible = successfulCopyEligibleForDelete + ` AND COALESCE(cur.delete_status,'') = 'skipped'`
)

// CopyStatusIsComplete reports whether copy work is satisfied (actual copy or already on DST).
func CopyStatusIsComplete(s string) bool {
	switch s {
	case CopyStatusSuccessful, CopyStatusAlreadyExisted:
		return true
	default:
		return false
	}
}

// CopyStatusIsPending reports whether copy work is still selected to copy.
func CopyStatusIsPending(s string) bool {
	return s == "" || s == CopyStatusPending
}

// Delete status values (src_status_events only).
// Planning uses pending_explicit (execute recursive/single delete) and pending_inherited
// (covered by an ancestor explicit; not pulled as a task). Outcomes: deleted / failed / skipped.
const (
	DeleteStatusPendingExplicit  = "pending_explicit"  // delete-forest root; worker executes DeleteNode
	DeleteStatusPendingInherited = "pending_inherited" // covered by ancestor pending_explicit
	DeleteStatusDeleted          = "deleted"
	DeleteStatusFailed           = "failed"
	DeleteStatusSkipped          = "skipped" // opted out of source removal during cleanup planning
)

// SQLDeleteStatusPendingIN matches any "will be removed" planning status.
const SQLDeleteStatusPendingIN = `('pending_explicit','pending_inherited')`

// DeleteStatusIsPending reports whether delete_status means scheduled for source removal.
func DeleteStatusIsPending(s string) bool {
	return s == DeleteStatusPendingExplicit || s == DeleteStatusPendingInherited
}

// DeleteStatusOnFrontier reports whether delete_status belongs on pend:del
// (executable roots only: pending_explicit or failed). pending_inherited is covered
// by an ancestor recursive delete and must not be leased.
func DeleteStatusOnFrontier(s string) bool {
	return s == DeleteStatusPendingExplicit || s == DeleteStatusFailed
}

// DeleteStatusAfterCopyComplete returns the delete_status to write when SRC becomes
// copy-complete (successful or already_existed). Empty means leave delete_status unchanged.
// Parent coverage: if a strict parent is already pending*, write pending_inherited; else pending_explicit.
func DeleteStatusAfterCopyComplete(parentDeleteStatus, currentDeleteStatus string) string {
	switch currentDeleteStatus {
	case DeleteStatusPendingExplicit, DeleteStatusPendingInherited,
		DeleteStatusDeleted, DeleteStatusFailed, DeleteStatusSkipped:
		return ""
	}
	if DeleteStatusIsPending(parentDeleteStatus) {
		return DeleteStatusPendingInherited
	}
	return DeleteStatusPendingExplicit
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

// GPLDisabled gates path verifier / GPL queue. False = enabled for new migrations.
const GPLDisabled = false

// MigrationOpsPath returns the Badger ops directory for a migration DB file path.
func MigrationOpsPath(dbPath string) string {
	if dbPath == ":memory:" {
		return ""
	}
	base := strings.TrimSuffix(filepath.Base(dbPath), ".db")
	dir := filepath.Dir(dbPath)
	return filepath.Join(dir, base+".ops")
}

// MigrationLogsPath returns the Badger logs sidecar directory for a migration DB file path.
func MigrationLogsPath(dbPath string) string {
	if dbPath == ":memory:" {
		return ""
	}
	base := strings.TrimSuffix(filepath.Base(dbPath), ".db")
	dir := filepath.Dir(dbPath)
	return filepath.Join(dir, base+".logs")
}

// PullKeysetWindowSize is how many node rows we scan per round-trip when filtering pulls by event-derived status
// (SRC depth keyset, copy/delete/GPL pulls).
const PullKeysetWindowSize = 50000
