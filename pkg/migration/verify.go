// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"

	"github.com/Project-Sylos/Sylos-DB/pkg/store"
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
func VerifyMigration(storeInstance *store.Store, opts VerifyOptions) (VerificationReport, error) {
	if storeInstance == nil {
		return VerificationReport{}, fmt.Errorf("storeInstance cannot be nil")
	}

	// For verification, prefer scan mode (accurate even if stats drifted).
	dbReport, err := storeInstance.InspectDatabase(store.InspectionModeScan)
	if err != nil {
		return VerificationReport{}, err
	}

	report := VerificationReport{
		SrcTotal: dbReport.Src.TotalNodes,
		DstTotal: dbReport.Dst.TotalNodes,

		SrcPending: dbReport.Src.TotalPending,
		DstPending: dbReport.Dst.TotalPending,

		SrcFailed: dbReport.Src.TotalFailed,
		DstFailed: dbReport.Dst.TotalFailed,

		DstNotOnSrc: dbReport.Dst.TotalNotOnSrc,
	}

	if report.SrcTotal == 0 && report.DstTotal == 0 {
		return report, fmt.Errorf("no nodes discovered - migration did not run")
	}

	// Must have actually migrated/traversed more than just roots.
	// We treat "moved" as any successful DST node at depth > 0.
	movedCount := 0
	for _, lvl := range dbReport.Dst.Levels {
		if lvl.Level > 0 {
			movedCount += lvl.Successful
		}
	}
	if movedCount == 0 {
		return report, fmt.Errorf("no migrated nodes detected beyond roots (dst successful at depth > 0 == 0)")
	}

	// Preserve existing semantics: only enforce via Success(opts) at call site,
	// but also fail fast here if policy disallows known bad states.
	if !opts.AllowPending && (report.SrcPending > 0 || report.DstPending > 0) {
		return report, fmt.Errorf("verification failed: pending nodes remain (src=%d dst=%d)", report.SrcPending, report.DstPending)
	}
	if !opts.AllowNotOnSrc && report.DstNotOnSrc > 0 {
		return report, fmt.Errorf("verification failed: dst nodes not on src (count=%d)", report.DstNotOnSrc)
	}

	return report, nil
}
