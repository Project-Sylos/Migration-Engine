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
	AllowFailed   bool
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
	NumMovedNodes int // Number of dst_nodes that were actually traversed/moved
}

// Success returns true when the report satisfies the supplied VerifyOptions.
// Additionally, migration will not be considered successful unless at least one node was actually moved/traversed.
func (r VerificationReport) Success(opts VerifyOptions) bool {
	if !opts.AllowPending && (r.SrcPending > 0 || r.DstPending > 0) {
		return false
	}
	if !opts.AllowFailed && (r.SrcFailed > 0 || r.DstFailed > 0) {
		return false
	}
	if !opts.AllowNotOnSrc && r.DstNotOnSrc > 0 {
		return false
	}
	if r.SrcTotal == 0 && r.DstTotal == 0 {
		return false
	}
	// Require at least one node in dst_nodes actually traversed/moved (not just roots inserted)
	if r.NumMovedNodes == 0 {
		return false
	}
	return true
}

// VerifyMigration inspects BadgerDB for pending, failed, or missing nodes and returns a report.
// In addition to previous checks, also verifies that at least one file/folder (not just roots) was migrated.
func VerifyMigration(badgerDB *db.DB, opts VerifyOptions) (VerificationReport, error) {
	if badgerDB == nil {
		return VerificationReport{}, fmt.Errorf("badgerDB cannot be nil")
	}

	report := VerificationReport{}

	// Count all src nodes
	srcPrefix := db.PrefixForAllLevels("src")
	srcTotal, err := badgerDB.CountByPrefix(srcPrefix)
	if err != nil {
		return VerificationReport{}, fmt.Errorf("failed to count src nodes: %w", err)
	}
	report.SrcTotal = srcTotal

	// Count all dst nodes
	dstPrefix := db.PrefixForAllLevels("dst")
	dstTotal, err := badgerDB.CountByPrefix(dstPrefix)
	if err != nil {
		return VerificationReport{}, fmt.Errorf("failed to count dst nodes: %w", err)
	}
	report.DstTotal = dstTotal

	if report.SrcTotal == 0 && report.DstTotal == 0 {
		return report, fmt.Errorf("no nodes discovered - migration did not run")
	}

	// Count pending src nodes
	var srcPendingCount int
	err = badgerDB.IterateKeys(db.IteratorOptions{
		Prefix: srcPrefix,
	}, func(key []byte) error {
		keyStr := string(key)
		parts := splitKeyParts(keyStr)
		if len(parts) >= 3 && parts[2] == db.TraversalStatusPending {
			srcPendingCount++
		}
		return nil
	})
	if err != nil {
		return VerificationReport{}, fmt.Errorf("failed to count pending src nodes: %w", err)
	}
	report.SrcPending = srcPendingCount

	// Count pending dst nodes
	var dstPendingCount int
	err = badgerDB.IterateKeys(db.IteratorOptions{
		Prefix: dstPrefix,
	}, func(key []byte) error {
		keyStr := string(key)
		parts := splitKeyParts(keyStr)
		if len(parts) >= 3 && parts[2] == db.TraversalStatusPending {
			dstPendingCount++
		}
		return nil
	})
	if err != nil {
		return VerificationReport{}, fmt.Errorf("failed to count pending dst nodes: %w", err)
	}
	report.DstPending = dstPendingCount

	if !opts.AllowPending && (report.SrcPending > 0 || report.DstPending > 0) {
		return report, fmt.Errorf("incomplete migration: %d src and %d dst nodes still pending", report.SrcPending, report.DstPending)
	}

	// Count failed src nodes
	var srcFailedCount int
	err = badgerDB.IterateKeys(db.IteratorOptions{
		Prefix: srcPrefix,
	}, func(key []byte) error {
		keyStr := string(key)
		parts := splitKeyParts(keyStr)
		if len(parts) >= 3 && parts[2] == db.TraversalStatusFailed {
			srcFailedCount++
		}
		return nil
	})
	if err != nil {
		return VerificationReport{}, fmt.Errorf("failed to count failed src nodes: %w", err)
	}
	report.SrcFailed = srcFailedCount

	// Count failed dst nodes
	var dstFailedCount int
	err = badgerDB.IterateKeys(db.IteratorOptions{
		Prefix: dstPrefix,
	}, func(key []byte) error {
		keyStr := string(key)
		parts := splitKeyParts(keyStr)
		if len(parts) >= 3 && parts[2] == db.TraversalStatusFailed {
			dstFailedCount++
		}
		return nil
	})
	if err != nil {
		return VerificationReport{}, fmt.Errorf("failed to count failed dst nodes: %w", err)
	}
	report.DstFailed = dstFailedCount

	if !opts.AllowFailed && (report.SrcFailed > 0 || report.DstFailed > 0) {
		return report, fmt.Errorf("migration reported failures: %d src and %d dst nodes failed", report.SrcFailed, report.DstFailed)
	}

	// Count dst nodes with NotOnSrc status
	if !opts.AllowNotOnSrc {
		var dstNotOnSrcCount int
		err = badgerDB.IterateKeys(db.IteratorOptions{
			Prefix: dstPrefix,
		}, func(key []byte) error {
			keyStr := string(key)
			parts := splitKeyParts(keyStr)
			if len(parts) >= 3 && parts[2] == db.TraversalStatusNotOnSrc {
				dstNotOnSrcCount++
			}
			return nil
		})
		if err != nil {
			return VerificationReport{}, fmt.Errorf("failed to count NotOnSrc dst nodes: %w", err)
		}
		report.DstNotOnSrc = dstNotOnSrcCount

		if report.DstNotOnSrc > 0 {
			return report, fmt.Errorf("incomplete migration: %d dst nodes marked as NotOnSrc", report.DstNotOnSrc)
		}
	}

	// Count dst nodes that were actually traversed/moved (successful status, depth > 0)
	var numMovedNodes int
	err = badgerDB.IterateNodeStates(db.IteratorOptions{
		Prefix: dstPrefix,
	}, func(key []byte, state *db.NodeState) error {
		keyStr := string(key)
		parts := splitKeyParts(keyStr)
		// Check if status is successful and depth > 0
		if len(parts) >= 3 && parts[2] == db.TraversalStatusSuccessful && state.Depth > 0 {
			numMovedNodes++
		}
		return nil
	})
	if err != nil {
		return report, fmt.Errorf("failed to count migrated nodes: %w", err)
	}
	report.NumMovedNodes = numMovedNodes

	if report.NumMovedNodes == 0 {
		return report, fmt.Errorf("no nodes were migrated: no dst nodes (other than roots) were traversed/moved")
	}

	return report, nil
}
