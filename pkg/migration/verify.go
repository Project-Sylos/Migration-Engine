// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
)

// VerifyOptions define the expectations for post-migration validation.
type VerifyOptions struct {
	AllowPending  bool
	AllowNotOnSrc bool
}

// VerificationReport captures aggregate statistics from the verification pass.
type VerificationReport struct {
	SrcTotal    int
	DstTotal    int
	SrcPending  int
	DstPending  int
	SrcFailed   int
	DstFailed   int
	DstNotOnSrc int
}

// Success returns true when the report satisfies the supplied VerifyOptions.
// Additionally, migration will not be considered successful unless at least one node was actually moved/traversed.
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

	var srcPendingCount, srcFailedCount int
	for _, level := range srcLevels {
		pendingCount, _ := boltDB.CountStatusBucket("SRC", level, db.StatusPending)
		srcPendingCount += pendingCount

		failedCount, _ := boltDB.CountStatusBucket("SRC", level, db.StatusFailed)
		srcFailedCount += failedCount
	}
	report.SrcPending = srcPendingCount
	report.SrcFailed = srcFailedCount

	// Count dst nodes
	dstLevels, err := boltDB.GetAllLevels("DST")
	if err != nil {
		return VerificationReport{}, fmt.Errorf("failed to get dst levels: %w", err)
	}

	var dstPendingCount, dstFailedCount, dstNotOnSrcCount int
	for _, level := range dstLevels {
		pendingCount, _ := boltDB.CountStatusBucket("DST", level, db.StatusPending)
		dstPendingCount += pendingCount

		failedCount, _ := boltDB.CountStatusBucket("DST", level, db.StatusFailed)
		dstFailedCount += failedCount

		notOnSrcCount, _ := boltDB.CountStatusBucket("DST", level, db.StatusNotOnSrc)
		dstNotOnSrcCount += notOnSrcCount
	}
	report.DstPending = dstPendingCount
	report.DstFailed = dstFailedCount
	report.DstNotOnSrc = dstNotOnSrcCount

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
