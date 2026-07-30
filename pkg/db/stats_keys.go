// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

// StatsKind identifies the per-depth stats key family in src_stats/dst_stats.
type StatsKind string

const (
	StatsKindTraversal StatsKind = "traversal"
	StatsKindCopy      StatsKind = "copy"
	StatsKindDelete    StatsKind = "delete"
)

// StatsKey returns the src_stats/dst_stats key for the given kind and status.
// Traversal valid statuses: pending, successful, failed, not_on_src (DST only).
// Copy valid statuses: pending, successful, failed (src only).
func StatsKey(kind StatsKind, status string) string {
	return string(kind) + "/" + status
}

// StatsKeyExpected is the stats key for expected count at a depth (set at round start).
const StatsKeyExpected = "expected"

// StatsKeyCompleted is the stats key for completed count at a depth (written at seal).
const StatsKeyCompleted = "completed"

// Universal stats table keys for canonical review stats (tableStats). Namespaced as traversal/*, copy/*, and flat aggregates.
const (
	ReviewKeyTraversalPending      = "traversal/pending"
	ReviewKeyTraversalPendingRetry = "traversal/pending_retry"
	ReviewKeyTraversalSuccessful   = "traversal/successful"
	ReviewKeyTraversalFailed       = "traversal/failed"
	ReviewKeyCopyPending           = "copy/pending"
	ReviewKeyCopySuccessful        = "copy/successful"
	ReviewKeyCopyFailed            = "copy/failed"
	ReviewKeyDeletePending         = "delete/pending"
	ReviewKeyDeleteDeleted         = "delete/deleted"
	ReviewKeyDeleteFailed          = "delete/failed"
	ReviewKeyExcluded              = "excluded"
	ReviewKeyFolders               = "folders"
	ReviewKeyFiles                 = "files"
	ReviewKeySizeSrc               = "size_src"
	ReviewKeySizeDst               = "size_dst"
	ReviewKeySizeSelected          = "size_selected"
	// ReviewKeySizeDeleteSelected is Σ file bytes with copy-complete + delete_status=pending.
	// Updated on seal flush (not round seal) so source-cleanup Selected survives mid-round stop.
	ReviewKeySizeDeleteSelected = "size_delete_selected"
)

var CanonicalReviewKeys = []string{
	ReviewKeyTraversalPending,
	ReviewKeyTraversalPendingRetry,
	ReviewKeyTraversalSuccessful,
	ReviewKeyTraversalFailed,
	ReviewKeyCopyPending,
	ReviewKeyCopySuccessful,
	ReviewKeyCopyFailed,
	ReviewKeyDeletePending,
	ReviewKeyDeleteDeleted,
	ReviewKeyDeleteFailed,
	ReviewKeyExcluded,
	ReviewKeyFolders,
	ReviewKeyFiles,
	ReviewKeySizeSrc,
	ReviewKeySizeDst,
	ReviewKeySizeSelected,
	ReviewKeySizeDeleteSelected,
}

func ReviewKeyForStatus(phase, status string) string {
	if phase == "delete" {
		switch status {
		case DeleteStatusPending:
			return ReviewKeyDeletePending
		case DeleteStatusDeleted:
			return ReviewKeyDeleteDeleted
		case DeleteStatusFailed:
			return ReviewKeyDeleteFailed
		default:
			return ""
		}
	}
	if phase == "copy" {
		switch status {
		case CopyStatusPending:
			return ReviewKeyCopyPending
		case CopyStatusSuccessful, CopyStatusAlreadyExisted:
			return ReviewKeyCopySuccessful
		case CopyStatusFailed:
			return ReviewKeyCopyFailed
		default:
			return ""
		}
	}
	switch status {
	case StatusPending:
		return ReviewKeyTraversalPending
	case StatusSuccessful:
		return ReviewKeyTraversalSuccessful
	case StatusFailed:
		return ReviewKeyTraversalFailed
	case StatusExcluded, StatusExclusionInherited:
		return ReviewKeyExcluded
	default:
		return ""
	}
}

// ReviewStatsSnapshot is the canonical persisted review stats in the universal stats table.
type ReviewStatsSnapshot struct {
	TraversalPending      int64
	TraversalPendingRetry int64
	TraversalSuccessful   int64
	TraversalFailed       int64
	CopyPending           int64
	CopySuccessful        int64
	CopyFailed            int64
	DeletePending         int64
	DeleteDeleted         int64
	DeleteFailed          int64
	Excluded              int64
	Folders               int64
	Files                 int64
	SizeSrc               int64
	SizeDst               int64
	SizeSelected          int64
	SizeDeleteSelected    int64
}

// PhaseProgressCounts holds durable pending/successful/failed counts for copy or delete progress.
// Successful is copy_status=successful for copy, or delete_status=deleted for delete.
type PhaseProgressCounts struct {
	Pending    int64
	Successful int64
	Failed     int64
}

// Completed returns successful + failed (terminal outcomes count as processed work).
func (c PhaseProgressCounts) Completed() int64 {
	return c.Successful + c.Failed
}

// Eligible returns pending + completed (excludes skipped/excluded by construction of the counts).
func (c PhaseProgressCounts) Eligible() int64 {
	return c.Pending + c.Completed()
}

// CompletedForMode returns the numerator used by DeterministicProgressPercent.
func (c PhaseProgressCounts) CompletedForMode(retryMode bool) int64 {
	if retryMode {
		return c.Successful
	}
	return c.Successful + c.Failed
}

// BytesProgressPercent returns 0–100 from bytes done vs a fixed migration-wide total.
// When total is 0, returns 100 (nothing to transfer). Done is capped at 100%.
func BytesProgressPercent(done, total int64) float64 {
	if done < 0 {
		done = 0
	}
	if total < 0 {
		total = 0
	}
	if total <= 0 {
		return 100
	}
	pct := 100.0 * float64(done) / float64(total)
	if pct < 0 {
		return 0
	}
	if pct > 100 {
		return 100
	}
	return pct
}

// EligibleCountsByType holds migration-wide eligible folder/file counts for progress denominators.
type EligibleCountsByType struct {
	Folders int64
	Files   int64
}

func (c EligibleCountsByType) Total() int64 {
	return c.Folders + c.Files
}

// DepthWorkAbsolute is folder/file/byte totals at one depth (copy/delete work).
type DepthWorkAbsolute struct {
	Folders int64
	Files   int64
	Bytes   int64
}

// SealedWorkTotals is the cumulative copy- or delete-work denominator.
type SealedWorkTotals struct {
	Folders    int64
	Files      int64
	Bytes      int64
	Generation int64
}

func (t SealedWorkTotals) Items() int64 {
	return t.Folders + t.Files
}

// TraversalStatusCounts holds traversal status counts derived from status events.
type TraversalStatusCounts struct {
	Pending    int64
	Successful int64
	Failed     int64
	NotOnSrc   int64 // DST only; 0 for SRC
	Excluded   int64
}

// CopyStatusCounts holds copy status counts for SRC.
// Successful is actual copy-phase completes only; AlreadyExisted is root/DST matches.
type CopyStatusCounts struct {
	Pending        int64
	Successful     int64
	AlreadyExisted int64
	Failed         int64
	Skipped        int64
	Excluded       int64
}

// Complete returns copy-satisfied count (actual copies + already on DST).
func (c CopyStatusCounts) Complete() int64 {
	return c.Successful + c.AlreadyExisted
}

// DeleteStatusCounts holds delete status counts for SRC.
type DeleteStatusCounts struct {
	Pending int64
	Deleted int64
	Failed  int64
	Skipped int64
}

// StatsRow is one row (depth, key, count) for breakdown by level.
type StatsRow struct {
	Depth int
	Key   string
	Count int64
}

// Copy / delete work stats keys and reasons (universal stats table + round tables).
const (
	StatsKeyCopyWorkFolders = "copy_work/folders"
	StatsKeyCopyWorkFiles   = "copy_work/files"
	StatsKeyCopyWorkBytes   = "copy_work/bytes"
	StatsKeyCopyWorkGen     = "copy_work/generation"

	StatsKeyDeleteWorkFolders = "delete_work/folders"
	StatsKeyDeleteWorkFiles   = "delete_work/files"
	StatsKeyDeleteWorkBytes   = "delete_work/bytes"
	StatsKeyDeleteWorkGen     = "delete_work/generation"

	CopyWorkReasonSrcDiscover         = "src_discover"
	CopyWorkReasonSrcDiscoverComplete = "src_discover_complete"
	CopyWorkReasonSrcDiscoverStop     = "src_discover_stop"
	CopyWorkReasonSrcDiscoverRootPrep = "src_discover_root_prep"

	CopyWorkReasonDstAECorrection         = "dst_ae_correction"
	CopyWorkReasonDstAECorrectionComplete = "dst_ae_correction_complete"
	CopyWorkReasonDstAECorrectionStop     = "dst_ae_correction_stop"
	CopyWorkReasonDstAECorrectionRootPrep = "dst_ae_correction_root_prep"

	CopyWorkReasonDSTRound        = "dst_round"
	CopyWorkReasonSrcAfterDSTDone = "src_after_dst_done"
	CopyWorkReasonStopFlush       = "stop_flush"
	CopyWorkReasonTraversalRetry  = "traversal_retry"
	CopyWorkReasonMarkComplete    = "mark_complete"
	CopyWorkReasonReviewExclude   = "review_exclude"
	CopyWorkReasonReviewUnexclude = "review_unexclude"

	DeleteWorkReasonPhaseStart = "delete_phase_start"
)

// Queue stats phase families (queue_stats table).
const (
	QueueStatsPhaseTraversal = "traversal"
	QueueStatsPhaseCopy      = "copy"
	QueueStatsPhaseDelete    = "delete"
)

// DeterministicProgressPercent computes percent complete from durable status counts.
//
// Normal mode: completed = successful + failed (terminal outcomes), so a finished run with
// permanent failures still reaches 100%.
//
// Retry mode: completed = successful only. Failed items are the retry workload, so the bar
// starts at the already-processed baseline and the remaining % is pending + failed.
func DeterministicProgressPercent(pending, successful, failed int64, retryMode bool) float64 {
	var completed, eligible int64
	if retryMode {
		completed = successful
		eligible = pending + successful + failed
	} else {
		completed = successful + failed
		eligible = pending + completed
	}
	if eligible <= 0 {
		return 0
	}
	pct := 100.0 * float64(completed) / float64(eligible)
	if pct < 0 {
		return 0
	}
	if pct > 100 {
		return 100
	}
	return pct
}
