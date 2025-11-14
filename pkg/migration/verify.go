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
	SrcTotal    int
	DstTotal    int
	SrcPending  int
	DstPending  int
	SrcFailed   int
	DstFailed   int
	DstNotOnSrc int
}

// Success returns true when the report satisfies the supplied VerifyOptions.
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
	return true
}

// VerifyMigration inspects the database for pending, failed, or missing nodes and returns a report.
func VerifyMigration(database *db.DB, opts VerifyOptions) (VerificationReport, error) {
	if database == nil {
		return VerificationReport{}, fmt.Errorf("database cannot be nil")
	}

	report := VerificationReport{}

	if err := singleCount(database, "SELECT COUNT(*) FROM src_nodes", &report.SrcTotal); err != nil {
		return VerificationReport{}, err
	}
	if err := singleCount(database, "SELECT COUNT(*) FROM dst_nodes", &report.DstTotal); err != nil {
		return VerificationReport{}, err
	}

	if report.SrcTotal == 0 && report.DstTotal == 0 {
		return report, fmt.Errorf("no nodes discovered - migration did not run")
	}

	if err := singleCount(database, "SELECT COUNT(*) FROM src_nodes WHERE traversal_status = 'Pending'", &report.SrcPending); err != nil {
		return VerificationReport{}, err
	}
	if err := singleCount(database, "SELECT COUNT(*) FROM dst_nodes WHERE traversal_status = 'Pending'", &report.DstPending); err != nil {
		return VerificationReport{}, err
	}

	if !opts.AllowPending && (report.SrcPending > 0 || report.DstPending > 0) {
		return report, fmt.Errorf("incomplete migration: %d src and %d dst nodes still pending", report.SrcPending, report.DstPending)
	}

	if err := singleCount(database, "SELECT COUNT(*) FROM src_nodes WHERE traversal_status = 'Failed'", &report.SrcFailed); err != nil {
		return VerificationReport{}, err
	}
	if err := singleCount(database, "SELECT COUNT(*) FROM dst_nodes WHERE traversal_status = 'Failed'", &report.DstFailed); err != nil {
		return VerificationReport{}, err
	}

	if !opts.AllowFailed && (report.SrcFailed > 0 || report.DstFailed > 0) {
		return report, fmt.Errorf("migration reported failures: %d src and %d dst nodes failed", report.SrcFailed, report.DstFailed)
	}

	if !opts.AllowNotOnSrc {
		if err := singleCount(database, "SELECT COUNT(*) FROM dst_nodes WHERE traversal_status = 'NotOnSrc'", &report.DstNotOnSrc); err != nil {
			return VerificationReport{}, err
		}
		if report.DstNotOnSrc > 0 {
			return report, fmt.Errorf("incomplete migration: %d dst nodes marked as NotOnSrc", report.DstNotOnSrc)
		}
	}

	return report, nil
}

func singleCount(database *db.DB, query string, dest *int) error {
	rows, err := database.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	if rows.Next() {
		if err := rows.Scan(dest); err != nil {
			return err
		}
	}

	return nil
}
