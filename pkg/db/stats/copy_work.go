// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package stats

import (
	"context"
	"fmt"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

// ReadSealedWorkTotals reads O(1) sealed work cumulatives for the given stats keys.
func ReadSealedWorkTotals(database *db.DB, foldersKey, filesKey, bytesKey, genKey string) (db.SealedWorkTotals, error) {
	var out db.SealedWorkTotals
	var err error
	if out.Folders, err = readStatsKeyCount(database, foldersKey); err != nil {
		return out, err
	}
	if out.Files, err = readStatsKeyCount(database, filesKey); err != nil {
		return out, err
	}
	if out.Bytes, err = readStatsKeyCount(database, bytesKey); err != nil {
		return out, err
	}
	if out.Generation, err = readStatsKeyCount(database, genKey); err != nil {
		return out, err
	}
	return out, nil
}

// GetWorkCreditedAtDepth returns net SUM of append-only work deltas for depth.
func GetWorkCreditedAtDepth(database *db.DB, table string, depth int) (db.DepthWorkAbsolute, error) {
	prefix := "copy"
	if table == db.TableDeleteWorkRoundStats {
		prefix = "delete"
	}
	return workCreditedAtDepthOps(database, depth, prefix)
}

func getCopyWorkCreditedAtDepthByReasonPrefix(database *db.DB, depth int, prefix string) (db.DepthWorkAbsolute, error) {
	return workCreditedAtDepthOps(database, depth, prefix)
}

func workCreditedAtDepthOps(database *db.DB, depth int, prefix string) (db.DepthWorkAbsolute, error) {
	var out db.DepthWorkAbsolute
	if database == nil || database.Ops() == nil {
		return out, fmt.Errorf("ops store required")
	}
	ops := database.Ops()
	var err error
	if out.Folders, err = ops.GetDepthStat(opsdb.SideSRC, depth, "cwcred/"+prefix+"/folders"); err != nil {
		return out, err
	}
	if out.Files, err = ops.GetDepthStat(opsdb.SideSRC, depth, "cwcred/"+prefix+"/files"); err != nil {
		return out, err
	}
	if out.Bytes, err = ops.GetDepthStat(opsdb.SideSRC, depth, "cwcred/"+prefix+"/bytes"); err != nil {
		return out, err
	}
	return out, nil
}

var copyDiscoveredStatuses = []string{
	db.CopyStatusPending,
	db.CopyStatusAlreadyExisted,
	db.CopyStatusSuccessful,
	db.CopyStatusFailed,
	db.CopyStatusSkipped,
}

var copyEligibleStatuses = []string{
	db.CopyStatusPending,
	db.CopyStatusSuccessful,
	db.CopyStatusFailed,
}

func copyWorkAbsoluteFromSrcStats(database *db.DB, depth int, statuses []string) (db.DepthWorkAbsolute, error) {
	var out db.DepthWorkAbsolute
	if len(statuses) == 0 || database == nil || database.Ops() == nil {
		return out, nil
	}
	ops := database.Ops()
	for _, st := range statuses {
		fk := db.StatsKeyTyped(db.StatsKindCopy, st, db.NodeTypeFolder)
		n, err := ops.GetDepthStat(opsdb.SideSRC, depth, fk)
		if err != nil {
			return out, err
		}
		out.Folders += n
		fileK := db.StatsKeyTyped(db.StatsKindCopy, st, db.NodeTypeFile)
		n, err = ops.GetDepthStat(opsdb.SideSRC, depth, fileK)
		if err != nil {
			return out, err
		}
		out.Files += n
		byteK := db.StatsKeyCopyFileBytes(st)
		n, err = ops.GetDepthStat(opsdb.SideSRC, depth, byteK)
		if err != nil {
			return out, err
		}
		out.Bytes += n
	}
	return out, nil
}

// GetCopyDiscoveredAtDepth returns SRC nodes at depth that count as potential copy work.
func GetCopyDiscoveredAtDepth(database *db.DB, depth int) (db.DepthWorkAbsolute, error) {
	return copyWorkAbsoluteFromSrcStats(database, depth, copyDiscoveredStatuses)
}

// GetCopyAlreadyExistedAtDepth returns SRC nodes at depth with copy_status already_existed.
func GetCopyAlreadyExistedAtDepth(database *db.DB, depth int) (db.DepthWorkAbsolute, error) {
	return copyWorkAbsoluteFromSrcStats(database, depth, []string{db.CopyStatusAlreadyExisted})
}

func workRoundStatsTable(kind db.StatsKind) string {
	if kind == db.StatsKindDelete {
		return db.TableDeleteWorkRoundStats
	}
	return db.TableCopyWorkRoundStats
}

func workRoundStatsKeys(kind db.StatsKind) (foldersKey, filesKey, bytesKey, genKey string) {
	if kind == db.StatsKindDelete {
		return db.StatsKeyDeleteWorkFolders, db.StatsKeyDeleteWorkFiles, db.StatsKeyDeleteWorkBytes, db.StatsKeyDeleteWorkGen
	}
	return db.StatsKeyCopyWorkFolders, db.StatsKeyCopyWorkFiles, db.StatsKeyCopyWorkBytes, db.StatsKeyCopyWorkGen
}

func deleteWorkAbsoluteFromSrcStats(database *db.DB, depth int, statuses []string) (db.DepthWorkAbsolute, error) {
	var out db.DepthWorkAbsolute
	if len(statuses) == 0 || database == nil || database.Ops() == nil {
		return out, nil
	}
	ops := database.Ops()
	for _, st := range statuses {
		fk := db.StatsKeyTyped(db.StatsKindDelete, st, db.NodeTypeFolder)
		n, err := ops.GetDepthStat(opsdb.SideSRC, depth, fk)
		if err != nil {
			return out, err
		}
		out.Folders += n
		fileK := db.StatsKeyTyped(db.StatsKindDelete, st, db.NodeTypeFile)
		n, err = ops.GetDepthStat(opsdb.SideSRC, depth, fileK)
		if err != nil {
			return out, err
		}
		out.Files += n
		if byteK := db.StatsKeyDeleteFileBytes(st); byteK != "" {
			n, err = ops.GetDepthStat(opsdb.SideSRC, depth, byteK)
			if err != nil {
				return out, err
			}
			out.Bytes += n
		}
	}
	return out, nil
}

// GetWorkEligibleAtDepth returns absolute copy- or delete-work eligible counts/sizes at one SRC depth.
func GetWorkEligibleAtDepth(database *db.DB, kind db.StatsKind, depth int) (db.DepthWorkAbsolute, error) {
	if kind != db.StatsKindDelete {
		return copyWorkAbsoluteFromSrcStats(database, depth, copyEligibleStatuses)
	}
	return deleteWorkAbsoluteFromSrcStats(database, depth, deletePopulationStatuses(SelectedEligible))
}

// SealSrcCopyWorkDiscovered appends a progress-stat catch-up delta so SRC discovery rows at depth
// match absolute counts. This is not catalog indexing; secondary indexes ride node insert.
func SealSrcCopyWorkDiscovered(database *db.DB, depth int, reason string) (db.DepthWorkAbsolute, error) {
	if reason == "" || !strings.HasPrefix(reason, "src_discover") {
		reason = db.CopyWorkReasonSrcDiscover
	}
	absolute, err := GetCopyDiscoveredAtDepth(database, depth)
	if err != nil {
		return db.DepthWorkAbsolute{}, err
	}
	credited, err := getCopyWorkCreditedAtDepthByReasonPrefix(database, depth, "src_discover")
	if err != nil {
		return db.DepthWorkAbsolute{}, err
	}
	delta := depthWorkDeltaSigned(absolute, credited)
	if delta.Folders == 0 && delta.Files == 0 && delta.Bytes == 0 {
		return delta, nil
	}
	if err := appendWorkDelta(database, db.TableCopyWorkRoundStats, depth, delta, reason,
		db.StatsKeyCopyWorkFolders, db.StatsKeyCopyWorkFiles, db.StatsKeyCopyWorkBytes, db.StatsKeyCopyWorkGen); err != nil {
		return db.DepthWorkAbsolute{}, err
	}
	return delta, nil
}

// SealDstCopyWorkAlreadyExistedCorrection appends a catch-up delta for DST AE correction.
func SealDstCopyWorkAlreadyExistedCorrection(database *db.DB, depth int, reason string) (db.DepthWorkAbsolute, error) {
	if reason == "" || !strings.HasPrefix(reason, "dst_ae_correction") {
		reason = db.CopyWorkReasonDstAECorrection
	}
	ae, err := GetCopyAlreadyExistedAtDepth(database, depth)
	if err != nil {
		return db.DepthWorkAbsolute{}, err
	}
	currentCorr, err := getCopyWorkCreditedAtDepthByReasonPrefix(database, depth, "dst_ae_correction")
	if err != nil {
		return db.DepthWorkAbsolute{}, err
	}
	target := db.DepthWorkAbsolute{Folders: -ae.Folders, Files: -ae.Files, Bytes: -ae.Bytes}
	delta := depthWorkDeltaSigned(target, currentCorr)
	if delta.Folders == 0 && delta.Files == 0 && delta.Bytes == 0 {
		return delta, nil
	}
	if err := appendWorkDelta(database, db.TableCopyWorkRoundStats, depth, delta, reason,
		db.StatsKeyCopyWorkFolders, db.StatsKeyCopyWorkFiles, db.StatsKeyCopyWorkBytes, db.StatsKeyCopyWorkGen); err != nil {
		return db.DepthWorkAbsolute{}, err
	}
	return delta, nil
}

// FinalizeWorkDepth seals copy- or delete-work at one depth (signed catch-up vs credited).
func FinalizeWorkDepth(database *db.DB, kind db.StatsKind, depth int, reason string) (db.DepthWorkAbsolute, error) {
	roundTable := workRoundStatsTable(kind)
	foldersKey, filesKey, bytesKey, genKey := workRoundStatsKeys(kind)
	absolute, err := GetWorkEligibleAtDepth(database, kind, depth)
	if err != nil {
		return db.DepthWorkAbsolute{}, err
	}
	credited, err := GetWorkCreditedAtDepth(database, roundTable, depth)
	if err != nil {
		return db.DepthWorkAbsolute{}, err
	}
	delta := depthWorkDeltaSigned(absolute, credited)
	if delta.Folders == 0 && delta.Files == 0 && delta.Bytes == 0 {
		return delta, nil
	}
	if err := appendWorkDelta(database, roundTable, depth, delta, reason, foldersKey, filesKey, bytesKey, genKey); err != nil {
		return db.DepthWorkAbsolute{}, err
	}
	return delta, nil
}

func depthWorkDeltaSigned(absolute, credited db.DepthWorkAbsolute) db.DepthWorkAbsolute {
	return db.DepthWorkAbsolute{
		Folders: absolute.Folders - credited.Folders,
		Files:   absolute.Files - credited.Files,
		Bytes:   absolute.Bytes - credited.Bytes,
	}
}

func workCreditReasonPrefix(reason string) string {
	switch {
	case strings.HasPrefix(reason, "src_discover"):
		return "src_discover"
	case strings.HasPrefix(reason, "dst_ae_correction"):
		return "dst_ae_correction"
	case strings.HasPrefix(reason, "delete") || reason == db.DeleteWorkReasonPhaseStart:
		return "delete"
	default:
		return "copy"
	}
}

func appendWorkDelta(
	database *db.DB,
	table string,
	depth int,
	delta db.DepthWorkAbsolute,
	reason string,
	foldersKey, filesKey, bytesKey, genKey string,
) error {
	if database == nil || database.Ops() == nil {
		return fmt.Errorf("ops store required")
	}
	if reason == "" {
		reason = "unspecified"
	}
	prefix := workCreditReasonPrefix(reason)
	depthDeltas := []opsdb.DepthCounterDelta{
		{Side: opsdb.SideSRC, Depth: depth, Key: "cwcred/" + prefix + "/folders", Delta: delta.Folders},
		{Side: opsdb.SideSRC, Depth: depth, Key: "cwcred/" + prefix + "/files", Delta: delta.Files},
		{Side: opsdb.SideSRC, Depth: depth, Key: "cwcred/" + prefix + "/bytes", Delta: delta.Bytes},
	}
	reviewDeltas := []struct {
		key   string
		delta int64
	}{
		{foldersKey, delta.Folders},
		{filesKey, delta.Files},
		{bytesKey, delta.Bytes},
		{genKey, 1},
	}
	reviewKeys := make([]string, 0, len(reviewDeltas))
	reviewVals := make([]int64, 0, len(reviewDeltas))
	for _, d := range reviewDeltas {
		if d.key == "" || d.delta == 0 {
			continue
		}
		reviewKeys = append(reviewKeys, d.key)
		reviewVals = append(reviewVals, d.delta)
	}
	return database.Ops().ApplyReviewAndDepth(reviewKeys, reviewVals, depthDeltas)
}

type SealCopyWorkDepthFn func(database *db.DB, depth int, reason string) (db.DepthWorkAbsolute, error)

// SealCopyWorkThroughDepth runs seal for depths 0..maxDepth inclusive.
func SealCopyWorkThroughDepth(
	database *db.DB,
	maxDepth int,
	reason string,
	seal SealCopyWorkDepthFn,
	errLabel string,
) error {
	if maxDepth < 0 {
		return nil
	}
	for d := 0; d <= maxDepth; d++ {
		if _, err := seal(database, d, reason); err != nil {
			return fmt.Errorf("%s depth %d: %w", errLabel, d, err)
		}
	}
	return nil
}

// AdjustCopyWorkForReview applies a signed folder/file/byte delta from path-review exclude/unexclude.
func AdjustCopyWorkForReview(database *db.DB, delta db.DepthWorkAbsolute, reason string) error {
	if delta.Folders == 0 && delta.Files == 0 && delta.Bytes == 0 {
		return nil
	}
	if reason == "" {
		reason = db.CopyWorkReasonReviewExclude
	}
	return appendWorkDelta(database, db.TableCopyWorkRoundStats, -1, delta, reason,
		db.StatsKeyCopyWorkFolders, db.StatsKeyCopyWorkFiles, db.StatsKeyCopyWorkBytes, db.StatsKeyCopyWorkGen)
}

// SnapshotDeleteWorkAtPhaseStart seals delete-eligible work for all SRC depths once.
func SnapshotDeleteWorkAtPhaseStart(database *db.DB) error {
	maxDepth, err := GetMaxDepth(database, "SRC")
	if err != nil {
		return err
	}
	if maxDepth < 0 {
		return nil
	}
	for d := 0; d <= maxDepth; d++ {
		if _, err := FinalizeWorkDepth(database, db.StatsKindDelete, d, db.DeleteWorkReasonPhaseStart); err != nil {
			return fmt.Errorf("finalize delete work depth %d: %w", d, err)
		}
	}
	return nil
}

// RehydrateCopyWorkCumulativesFromRounds is a no-op; Badger maintains copy_work counters inline.
func RehydrateCopyWorkCumulativesFromRounds(database *db.DB) error {
	_ = context.Background()
	return nil
}
