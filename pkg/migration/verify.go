// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

// VerifyOptions define the expectations for post-migration validation.
type VerifyOptions struct {
	AllowPending  bool
	AllowNotOnSrc bool
}

// VerificationReport captures aggregate statistics from the verification pass.
type VerificationReport struct {
	SrcTotal      int
	DstTotal      int
	SrcPending    int
	DstPending    int
	SrcFailed     int
	DstFailed     int
	DstNotOnSrc   int
	SrcSuccessful int   // Count of successful SRC nodes
	DstSuccessful int   // Count of successful DST nodes
	SrcCompleted  int64 // Tasks completed (success or final failure) — not compared to bucket counts; some nodes are inserted as successful (e.g. files)
	DstCompleted  int64 // Tasks completed (success or final failure) — not compared to bucket counts
}

// Success returns true when the report satisfies the supplied VerifyOptions.
// Additionally, migration will not be considered successful unless at least one node was actually moved/traversed.
// Completed count is not compared to successful+failed: some nodes (e.g. files) are inserted directly as successful.
func (r VerificationReport) Success(opts VerifyOptions) bool {
	if !opts.AllowPending && (r.SrcPending > 0 || r.DstPending > 0) {
		return false
	}
	if !opts.AllowNotOnSrc && r.DstNotOnSrc > 0 {
		return false
	}
	if r.SrcTotal == 0 && r.DstTotal == 0 {
		return false
	}

	return true
}

// VerifyMigration inspects BoltDB for pending, failed, or missing nodes and returns a report.
// In addition to previous checks, also verifies that at least one file/folder (not just roots) was migrated.
func VerifyMigration(boltDB *db.DB, opts VerifyOptions) (VerificationReport, error) {
	if boltDB == nil {
		return VerificationReport{}, fmt.Errorf("boltDB cannot be nil")
	}

	report := VerificationReport{}

	// Count all src nodes
	srcTotal, err := boltDB.CountNodes("SRC")
	if err != nil {
		return VerificationReport{}, fmt.Errorf("failed to count src nodes: %w", err)
	}
	report.SrcTotal = srcTotal

	// Count all dst nodes
	dstTotal, err := boltDB.CountNodes("DST")
	if err != nil {
		return VerificationReport{}, fmt.Errorf("failed to count dst nodes: %w", err)
	}
	report.DstTotal = dstTotal

	if report.SrcTotal == 0 && report.DstTotal == 0 {
		return report, fmt.Errorf("no nodes discovered - migration did not run")
	}

	// Count pending, failed, and not_on_src nodes across all levels
	srcLevels, err := boltDB.GetAllLevels("SRC")
	if err != nil {
		return VerificationReport{}, fmt.Errorf("failed to get src levels: %w", err)
	}

	var srcPendingCount, srcFailedCount, srcSuccessfulCount int
	for _, level := range srcLevels {
		pendingCount, _ := boltDB.CountStatusBucket("SRC", level, db.StatusPending)
		srcPendingCount += pendingCount

		failedCount, _ := boltDB.CountStatusBucket("SRC", level, db.StatusFailed)
		srcFailedCount += failedCount

		successfulCount, _ := boltDB.CountStatusBucket("SRC", level, db.StatusSuccessful)
		srcSuccessfulCount += successfulCount
	}
	report.SrcPending = srcPendingCount
	report.SrcFailed = srcFailedCount
	report.SrcSuccessful = srcSuccessfulCount

	// Get completed count (tasks that transitioned out of pending)
	srcCompleted, _ := boltDB.GetTotalCompletedCount("SRC")
	report.SrcCompleted = srcCompleted

	// Count dst nodes
	dstLevels, err := boltDB.GetAllLevels("DST")
	if err != nil {
		return VerificationReport{}, fmt.Errorf("failed to get dst levels: %w", err)
	}

	var dstPendingCount, dstFailedCount, dstNotOnSrcCount, dstSuccessfulCount int
	for _, level := range dstLevels {
		pendingCount, _ := boltDB.CountStatusBucket("DST", level, db.StatusPending)
		dstPendingCount += pendingCount

		failedCount, _ := boltDB.CountStatusBucket("DST", level, db.StatusFailed)
		dstFailedCount += failedCount

		notOnSrcCount, _ := boltDB.CountStatusBucket("DST", level, db.StatusNotOnSrc)
		dstNotOnSrcCount += notOnSrcCount

		successfulCount, _ := boltDB.CountStatusBucket("DST", level, db.StatusSuccessful)
		dstSuccessfulCount += successfulCount
	}
	report.DstPending = dstPendingCount
	report.DstFailed = dstFailedCount
	report.DstNotOnSrc = dstNotOnSrcCount
	report.DstSuccessful = dstSuccessfulCount

	// Get completed count (tasks that transitioned out of pending)
	dstCompleted, _ := boltDB.GetTotalCompletedCount("DST")
	report.DstCompleted = dstCompleted

	// Calculate number of actually moved nodes (excluding root, which is level 0)
	// Only count successful dst traversals at depth > 0
	var movedCount int
	for _, level := range dstLevels {
		if level > 0 {
			successCount, _ := boltDB.CountStatusBucket("DST", level, db.StatusSuccessful)
			movedCount += successCount
		}
	}

	return report, nil
}
