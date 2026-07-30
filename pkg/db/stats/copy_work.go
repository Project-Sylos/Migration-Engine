// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package stats

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
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

// GetWorkCreditedAtDepth returns net SUM of append-only work deltas for depth in the given round-stats table.
func GetWorkCreditedAtDepth(database *db.DB, table string, depth int) (db.DepthWorkAbsolute, error) {
	var out db.DepthWorkAbsolute
	conn, err := database.GetDB()
	if err != nil {
		return out, err
	}
	ctx := context.Background()
	err = conn.QueryRowContext(ctx, `SELECT
  COALESCE(SUM(folders), 0)::BIGINT,
  COALESCE(SUM(files), 0)::BIGINT,
  COALESCE(SUM(bytes), 0)::BIGINT
FROM `+table+` WHERE depth = $1`, depth).Scan(&out.Folders, &out.Files, &out.Bytes)
	return out, err
}

func getCopyWorkCreditedAtDepthByReasonPrefix(database *db.DB, depth int, prefix string) (db.DepthWorkAbsolute, error) {
	var out db.DepthWorkAbsolute
	conn, err := database.GetDB()
	if err != nil {
		return out, err
	}
	ctx := context.Background()
	err = conn.QueryRowContext(ctx, `SELECT
  COALESCE(SUM(folders), 0)::BIGINT,
  COALESCE(SUM(files), 0)::BIGINT,
  COALESCE(SUM(bytes), 0)::BIGINT
FROM `+db.TableCopyWorkRoundStats+`
WHERE depth = $1 AND reason LIKE $2`, depth, prefix+"%").Scan(&out.Folders, &out.Files, &out.Bytes)
	return out, err
}

// GetCopyDiscoveredAtDepth returns SRC nodes at depth that count as potential copy work
// until DST AE-corrects. Excluded (root-pick / review) nodes are omitted so they never
// enter copy_work denominators.
func GetCopyDiscoveredAtDepth(database *db.DB, depth int) (db.DepthWorkAbsolute, error) {
	var out db.DepthWorkAbsolute
	conn, err := database.GetDB()
	if err != nil {
		return out, err
	}
	ctx := context.Background()
	err = conn.QueryRowContext(ctx, `SELECT
  COUNT(*) FILTER (WHERE n.type = 'folder')::BIGINT,
  COUNT(*) FILTER (WHERE n.type = 'file')::BIGINT,
  COALESCE(SUM(CASE WHEN n.type = 'file' THEN n.size ELSE 0 END), 0)::BIGINT
FROM `+db.TableSrcNodes+` n
LEFT JOIN `+db.CTESrcCurrentStatus+` cur ON n.id = cur.id
WHERE n.depth = $1
  AND COALESCE(cur.copy_status,'') NOT IN `+db.SQLCopyStatusExcludedIN, depth).
		Scan(&out.Folders, &out.Files, &out.Bytes)
	return out, err
}

// GetCopyAlreadyExistedAtDepth returns SRC nodes at depth whose latest copy_status is already_existed.
func GetCopyAlreadyExistedAtDepth(database *db.DB, depth int) (db.DepthWorkAbsolute, error) {
	var out db.DepthWorkAbsolute
	conn, err := database.GetDB()
	if err != nil {
		return out, err
	}
	ctx := context.Background()
	err = conn.QueryRowContext(ctx, `SELECT
  COUNT(*) FILTER (WHERE n.type = 'folder')::BIGINT,
  COUNT(*) FILTER (WHERE n.type = 'file')::BIGINT,
  COALESCE(SUM(CASE WHEN n.type = 'file' THEN n.size ELSE 0 END), 0)::BIGINT
FROM `+db.TableSrcNodes+` n
LEFT JOIN `+db.CTESrcCurrentStatus+` cur ON n.id = cur.id
WHERE n.depth = $1
  AND COALESCE(cur.copy_status,'') = $2`, depth, db.CopyStatusAlreadyExisted).
		Scan(&out.Folders, &out.Files, &out.Bytes)
	return out, err
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

// GetWorkEligibleAtDepth returns absolute copy- or delete-work eligible counts/sizes at one SRC depth.
func GetWorkEligibleAtDepth(database *db.DB, kind db.StatsKind, depth int) (db.DepthWorkAbsolute, error) {
	var out db.DepthWorkAbsolute
	conn, err := database.GetDB()
	if err != nil {
		return out, err
	}
	ctx := context.Background()
	err = conn.QueryRowContext(ctx, `SELECT
  COUNT(*) FILTER (WHERE n.type = 'folder')::BIGINT,
  COUNT(*) FILTER (WHERE n.type = 'file')::BIGINT,
  COALESCE(SUM(CASE WHEN n.type = 'file' THEN n.size ELSE 0 END), 0)::BIGINT
FROM `+db.TableSrcNodes+` n
LEFT JOIN `+db.CTESrcCurrentStatus+` cur ON n.id = cur.id
WHERE n.depth = $1
  AND `+workStatusWhere(kind, SelectedEligible), depth).
		Scan(&out.Folders, &out.Files, &out.Bytes)
	return out, err
}

// SealSrcCopyWorkDiscovered appends a catch-up delta so SRC discovery rows at depth match
// absolute discovered node counts. Idempotent across retries (delta 0 when already caught up).
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

// SealDstCopyWorkAlreadyExistedCorrection appends a catch-up delta so DST correction rows at
// depth sum to -already_existed. Idempotent: re-sealing the same AE set yields delta 0.
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
	// Target correction sum is -ae; delta brings currentCorr to that target.
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

func appendWorkDelta(
	database *db.DB,
	table string,
	depth int,
	delta db.DepthWorkAbsolute,
	reason string,
	foldersKey, filesKey, bytesKey, genKey string,
) error {
	if reason == "" {
		reason = "unspecified"
	}
	return database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			ctx := context.Background()
			_, err := w.Tx().ExecContext(ctx,
				`INSERT INTO `+table+` (depth, folders, files, bytes, reason) VALUES ($1, $2, $3, $4, $5)`,
				depth, delta.Folders, delta.Files, delta.Bytes, reason,
			)
			if err != nil {
				return fmt.Errorf("append %s: %w", table, err)
			}
			deltas := []db.ReviewStatsDelta{
				{Key: foldersKey, Delta: delta.Folders},
				{Key: filesKey, Delta: delta.Files},
				{Key: bytesKey, Delta: delta.Bytes},
				{Key: genKey, Delta: 1},
			}
			return w.ApplyReviewStatsDeltas(deltas)
		})
	})
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

// AdjustCopyWorkForReview applies a signed folder/file/byte delta from path-review
// exclude/unexclude so sealed copy_work/* stays aligned with the selected plan.
// Depth is recorded as -1 (review-wide, not a traversal round). No-op when delta is zero.
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
// Safe to call again: signed catch-up only appends when absolute changed.
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

// RehydrateCopyWorkCumulativesFromRounds rebuilds copy_work/* keys from the append-only table.
func RehydrateCopyWorkCumulativesFromRounds(database *db.DB) error {
	conn, err := database.GetDB()
	if err != nil {
		return err
	}
	ctx := context.Background()
	var folders, files, bytes sql.NullInt64
	err = conn.QueryRowContext(ctx, `SELECT
  COALESCE(SUM(folders), 0)::BIGINT,
  COALESCE(SUM(files), 0)::BIGINT,
  COALESCE(SUM(bytes), 0)::BIGINT
FROM `+db.TableCopyWorkRoundStats).Scan(&folders, &files, &bytes)
	if err != nil {
		return err
	}
	return database.RunWrite(ctx, func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			pairs := []struct {
				key   string
				count int64
			}{
				{db.StatsKeyCopyWorkFolders, folders.Int64},
				{db.StatsKeyCopyWorkFiles, files.Int64},
				{db.StatsKeyCopyWorkBytes, bytes.Int64},
			}
			for _, p := range pairs {
				_, err := w.Tx().ExecContext(ctx,
					`INSERT INTO `+db.TableStats+` (key, count) VALUES ($1, $2)
					 ON CONFLICT (key) DO UPDATE SET count = excluded.count`,
					p.key, p.count,
				)
				if err != nil {
					return err
				}
			}
			return nil
		})
	})
}
