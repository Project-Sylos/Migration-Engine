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
	SrcSuccessful int // Count of successful SRC nodes
	DstSuccessful int // Count of successful DST nodes
}

// Success returns true when the report satisfies the supplied VerifyOptions.
// Migration is not considered successful unless at least one node was actually moved/traversed.
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

// VerifyMigration inspects the DuckDB node tables and stats for pending, failed, or missing nodes and returns a report.
func VerifyMigration(database *db.DB, opts VerifyOptions) (VerificationReport, error) {
	if database == nil {
		return VerificationReport{}, fmt.Errorf("database cannot be nil")
	}

	report := VerificationReport{}

	srcTotal, err := db.CountNodes(database, "SRC")
	if err != nil {
		return VerificationReport{}, fmt.Errorf("failed to count src nodes: %w", err)
	}
	report.SrcTotal = srcTotal

	dstTotal, err := db.CountNodes(database, "DST")
	if err != nil {
		return VerificationReport{}, fmt.Errorf("failed to count dst nodes: %w", err)
	}
	report.DstTotal = dstTotal

	if report.SrcTotal == 0 && report.DstTotal == 0 {
		return report, fmt.Errorf("no nodes discovered - migration did not run")
	}

	// Status totals from src_stats
	c, err := database.GetStatsCount("SRC", db.StatsKeyTraversalStatus(db.StatusPending))
	if err != nil {
		return VerificationReport{}, fmt.Errorf("failed to get stats count: %w", err)
	}
	report.SrcPending = int(c)
	c, err = database.GetStatsCount("SRC", db.StatsKeyTraversalStatus(db.StatusFailed))
	if err != nil {
		return VerificationReport{}, fmt.Errorf("failed to get stats count: %w", err)
	}
	report.SrcFailed = int(c)
	c, err = database.GetStatsCount("SRC", db.StatsKeyTraversalStatus(db.StatusSuccessful))
	if err != nil {
		return VerificationReport{}, fmt.Errorf("failed to get stats count: %w", err)
	}
	report.SrcSuccessful = int(c)

	// Status totals from dst_stats
	c, err = database.GetStatsCount("DST", db.StatsKeyTraversalStatus(db.StatusPending))
	if err != nil {
		return VerificationReport{}, fmt.Errorf("failed to get stats count: %w", err)
	}
	report.DstPending = int(c)
	c, err = database.GetStatsCount("DST", db.StatsKeyTraversalStatus(db.StatusFailed))
	if err != nil {
		return VerificationReport{}, fmt.Errorf("failed to get stats count: %w", err)
	}
	report.DstFailed = int(c)
	c, err = database.GetStatsCount("DST", db.StatsKeyTraversalStatus(db.StatusNotOnSrc))
	if err != nil {
		return VerificationReport{}, fmt.Errorf("failed to get stats count: %w", err)
	}
	report.DstNotOnSrc = int(c)
	c, err = database.GetStatsCount("DST", db.StatsKeyTraversalStatus(db.StatusSuccessful))
	if err != nil {
		return VerificationReport{}, fmt.Errorf("failed to get stats count: %w", err)
	}
	report.DstSuccessful = int(c)

	return report, nil
}
