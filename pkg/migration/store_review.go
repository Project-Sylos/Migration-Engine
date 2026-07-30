// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/failurelog"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/subtree"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/review"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
)

// deltaKeyToReviewKey maps a migration-layer delta key to the universal stats table key.
func deltaKeyToReviewKey(k string) string {
	switch k {
	case DeltaTraversalPending:
		return db.ReviewKeyTraversalPending
	case DeltaTraversalPendingRetry:
		return db.ReviewKeyTraversalPendingRetry
	case DeltaTraversalFailed:
		return db.ReviewKeyTraversalFailed
	case DeltaCopyPending:
		return db.ReviewKeyCopyPending
	case DeltaCopyFailed:
		return db.ReviewKeyCopyFailed
	case DeltaCopySuccessful:
		return db.ReviewKeyCopySuccessful
	case DeltaDeletePending:
		return db.ReviewKeyDeletePending
	case DeltaDeleteFailed:
		return db.ReviewKeyDeleteFailed
	case DeltaDeleteDeleted:
		return db.ReviewKeyDeleteDeleted
	case DeltaExcluded:
		return db.ReviewKeyExcluded
	case DeltaFolders:
		return db.ReviewKeyFolders
	case DeltaFiles:
		return db.ReviewKeyFiles
	case DeltaSizeSrc:
		return db.ReviewKeySizeSrc
	case DeltaSizeDst:
		return db.ReviewKeySizeDst
	case DeltaSizeSelected:
		return db.ReviewKeySizeSelected
	case DeltaSizeDeleteSelected:
		return db.ReviewKeySizeDeleteSelected
	default:
		return ""
	}
}

// persistReviewDeltas applies migration-layer deltas to the universal stats table.
func (s *migrationStore) persistReviewDeltas(deltas map[string]int64) error {
	if len(deltas) == 0 {
		return nil
	}
	// Delete-selected mutations emit sizeSelected / folders / files for optimistic UI only.
	// Persist size_delete_selected and delete status keys; keep copy size_selected and
	// folders/files snapshots (copy-selected) intact.
	skipCopySelectedSnapshot := deltas[DeltaSizeDeleteSelected] != 0 ||
		deltas[DeltaDeleteSkipped] != 0 ||
		deltas[DeltaDeleteFailed] != 0 ||
		deltas[DeltaDeleteDeleted] != 0
	dbDeltas := make([]db.ReviewStatsDelta, 0, len(deltas))
	for k, v := range deltas {
		if skipCopySelectedSnapshot && (k == DeltaSizeSelected || k == DeltaFolders || k == DeltaFiles) {
			continue
		}
		if rk := deltaKeyToReviewKey(k); rk != "" {
			dbDeltas = append(dbDeltas, db.ReviewStatsDelta{Key: rk, Delta: v})
		}
	}
	if len(dbDeltas) == 0 {
		return nil
	}
	return s.db.RunWrite(context.Background(), func(sess *db.WriteSession) error {
		return sess.WithTx(func(w *db.Writer) error {
			return w.ApplyReviewStatsDeltas(dbDeltas)
		})
	})
}

func mergedRowToDiffItem(r review.MergedReviewRow) DiffItem {
	item := DiffItem{
		Path:               r.Path,
		Name:               r.Name,
		Depth:              r.Depth,
		Type:               r.Type,
		SrcNodeID:          r.SrcNodeID,
		DstNodeID:          r.DstNodeID,
		SrcTraversalStatus: r.SrcTraversalStatus,
		DstTraversalStatus: r.DstTraversalStatus,
		CopyStatus:         db.CopyStatusForDisplay(r.CopyStatus),
		DeleteStatus:       r.DeleteStatus,
		Excluded:           r.Excluded,
		Size:               r.Size,
		MissingOnSource:    r.SrcNodeID == "",
		MissingOnDest:      r.DstNodeID == "",
		ResolvedDstName:    strings.TrimSpace(r.ResolvedDstName),
	}
	if item.MissingOnSource {
		item.CopyStatus = ""
	}
	if item.Name == "" {
		item.Name = path.Base(item.Path)
	}
	return item
}

func diffItemNeedsSrcFailureLog(item DiffItem) bool {
	if item.SrcNodeID == "" {
		return false
	}
	if strings.EqualFold(item.SrcTraversalStatus, db.StatusFailed) {
		return true
	}
	return strings.EqualFold(item.CopyStatus, db.CopyStatusFailed)
}

func diffItemNeedsDstFailureLog(item DiffItem) bool {
	if item.DstNodeID == "" {
		return false
	}
	return strings.EqualFold(item.DstTraversalStatus, db.StatusFailed)
}

func (s *migrationStore) enrichDiffItemsWithFailureLogs(items []DiffItem) error {
	if s == nil || s.db == nil || len(items) == 0 {
		return nil
	}
	ctx := context.Background()

	srcIDs := make([]string, 0)
	dstIDs := make([]string, 0)
	for _, item := range items {
		if diffItemNeedsSrcFailureLog(item) {
			srcIDs = append(srcIDs, item.SrcNodeID)
		}
		if diffItemNeedsDstFailureLog(item) {
			dstIDs = append(dstIDs, item.DstNodeID)
		}
	}

	srcLogByNode, err := failurelog.LatestFailureLogIDsByNodeIDs(ctx, s.db, db.TableSrcStatusEvents, srcIDs)
	if err != nil {
		return fmt.Errorf("load src failure log ids: %w", err)
	}
	dstLogByNode, err := failurelog.LatestFailureLogIDsByNodeIDs(ctx, s.db, db.TableDstStatusEvents, dstIDs)
	if err != nil {
		return fmt.Errorf("load dst failure log ids: %w", err)
	}

	logIDs := make([]string, 0, len(srcLogByNode)+len(dstLogByNode))
	for _, id := range srcLogByNode {
		logIDs = append(logIDs, id)
	}
	for _, id := range dstLogByNode {
		logIDs = append(logIDs, id)
	}
	logsByID, err := failurelog.GetFailureLogsByIDs(ctx, s.db, logIDs)
	if err != nil {
		return fmt.Errorf("load failure logs: %w", err)
	}

	for i := range items {
		if logID, ok := srcLogByNode[items[i].SrcNodeID]; ok {
			items[i].SrcFailureLogID = logID
			if log, ok := logsByID[logID]; ok {
				items[i].SrcFailureMessage = failurelog.FailureLogDisplayText(log)
			}
		}
		if logID, ok := dstLogByNode[items[i].DstNodeID]; ok {
			items[i].DstFailureLogID = logID
			if log, ok := logsByID[logID]; ok {
				items[i].DstFailureMessage = failurelog.FailureLogDisplayText(log)
			}
		}
	}
	return nil
}

func (s *migrationStore) queryNodes(filter NodeQueryFilter) ([]db.NodeState, error) {
	table := "SRC"
	if strings.ToUpper(filter.Queue) == "DST" {
		table = "DST"
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}
	return review.QueryNodesForReview(s.db, table, filter.Depth, filter.Status, filter.Excluded, filter.PathLike, filter.OrderByPath, limit, offset)
}

func statusCountsAsPending(status, pendingStatus string) bool {
	switch status {
	case "", pendingStatus:
		return true
	default:
		return false
	}
}

// copyWorkDeltaForNode returns the signed folder/file/byte adjustment for one node
// entering (+) or leaving (−) the copy-work denominator.
func copyWorkDeltaForNode(nodeType string, size int64, sign int64) db.DepthWorkAbsolute {
	var d db.DepthWorkAbsolute
	switch nodeType {
	case db.NodeTypeFolder:
		d.Folders = sign
	case db.NodeTypeFile:
		d.Files = sign
		d.Bytes = sign * size
	}
	return d
}

// addPendingTypeCountDelta adjusts Path Review folders/files counts (selected set) for one node.
func addPendingTypeCountDelta(deltas map[string]int64, nodeType string, sign int64) {
	switch nodeType {
	case db.NodeTypeFolder:
		addReviewDelta(deltas, DeltaFolders, sign)
	case db.NodeTypeFile:
		addReviewDelta(deltas, DeltaFiles, sign)
	}
}

func copyStatusIsComplete(status string) bool {
	switch status {
	case db.CopyStatusSuccessful, db.CopyStatusAlreadyExisted:
		return true
	default:
		return false
	}
}

// addDeleteSelectedSizeDelta adjusts source-cleanup Selected bytes when a copy-complete
// file enters/leaves delete_status=pending. Persists size_delete_selected (not copy size_selected).
// Also emits optimistic folders/files deltas (same selected-set semantics as Selected).
// API still exposes the byte value as sizeSelected for the Path Review footer.
func addDeleteSelectedSizeDelta(deltas map[string]int64, node *db.NodeState, from, to string) {
	if node == nil || !copyStatusIsComplete(node.CopyStatus) {
		return
	}
	wasPending := statusCountsAsPending(from, db.DeleteStatusPending)
	nowPending := statusCountsAsPending(to, db.DeleteStatusPending)
	var sign int64
	switch {
	case wasPending && !nowPending:
		sign = -1
	case !wasPending && nowPending:
		sign = 1
	default:
		return
	}
	addPendingTypeCountDelta(deltas, node.Type, sign)
	if node.Type != db.NodeTypeFile || node.Size == 0 {
		return
	}
	addReviewDelta(deltas, DeltaSizeDeleteSelected, sign*node.Size)
	// Mirror into sizeSelected so existing UI optimistic applyStatsDeltas keeps working.
	addReviewDelta(deltas, DeltaSizeSelected, sign*node.Size)
}

func (s *migrationStore) setNodeExcluded(nodeID string, excluded bool) (int64, map[string]int64, error) {
	// Exclude/unexclude is SRC-only. Pending/empty ↔ excluded only.
	node, err := pull.GetNodeByID(s.db, "SRC", nodeID)
	if err != nil {
		return 0, nil, err
	}
	if node == nil {
		return 0, nil, fmt.Errorf("node %s not found in SRC", nodeID)
	}
	if excluded {
		if node.Excluded || !statusCountsAsPending(node.CopyStatus, db.CopyStatusPending) {
			return 0, nil, nil
		}
	} else {
		if !node.Excluded {
			return 0, nil, nil
		}
	}
	err = s.db.RunWrite(context.Background(), func(sess *db.WriteSession) error {
		return sess.WithTx(func(w *db.Writer) error {
			if !excluded {
				prior, err2 := w.LatestNonExclusionCopyStatus(nodeID)
				if err2 != nil {
					return err2
				}
				if prior != "" && prior != db.CopyStatusPending {
					return nil
				}
			}
			return w.SetNodeExcluded("SRC", nodeID, excluded)
		})
	})
	if err != nil {
		return 0, nil, err
	}
	// Re-read: SetNodeExcluded may no-op inside the tx without surfacing it.
	after, err := pull.GetNodeByID(s.db, "SRC", nodeID)
	if err != nil || after == nil {
		return 0, nil, err
	}
	if excluded && !after.Excluded {
		return 0, nil, nil
	}
	if !excluded && after.Excluded {
		return 0, nil, nil
	}
	deltas := make(map[string]int64)
	if excluded {
		addReviewDelta(deltas, DeltaExcluded, 1)
		addReviewDelta(deltas, DeltaCopyPending, -1)
		addPendingTypeCountDelta(deltas, node.Type, -1)
		if node.Type == db.NodeTypeFile {
			addReviewDelta(deltas, DeltaSizeSelected, -node.Size)
		}
		if err := stats.AdjustCopyWorkForReview(s.db, copyWorkDeltaForNode(node.Type, node.Size, -1), db.CopyWorkReasonReviewExclude); err != nil {
			return 0, nil, fmt.Errorf("adjust copy work on exclude: %w", err)
		}
	} else {
		addReviewDelta(deltas, DeltaExcluded, -1)
		addReviewDelta(deltas, DeltaCopyPending, 1)
		addPendingTypeCountDelta(deltas, node.Type, 1)
		if node.Type == db.NodeTypeFile {
			addReviewDelta(deltas, DeltaSizeSelected, node.Size)
		}
		if err := stats.AdjustCopyWorkForReview(s.db, copyWorkDeltaForNode(node.Type, node.Size, 1), db.CopyWorkReasonReviewUnexclude); err != nil {
			return 0, nil, fmt.Errorf("adjust copy work on unexclude: %w", err)
		}
	}
	if err := s.persistReviewDeltas(deltas); err != nil {
		return 0, nil, fmt.Errorf("persist review deltas: %w", err)
	}
	return 1, deltas, nil
}

func (s *migrationStore) listRecentLogs(limit int) ([]LogEntry, error) {
	if limit <= 0 {
		limit = 50
	}
	conn, err := s.db.GetDB()
	if err != nil {
		return nil, err
	}
	rows, err := conn.QueryContext(
		context.Background(),
		`SELECT id, created_at, level, message FROM logs ORDER BY created_at DESC LIMIT $1`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("get recent logs: %w", err)
	}
	defer rows.Close()
	out := make([]LogEntry, 0, limit)
	for rows.Next() {
		var (
			id        string
			timestamp time.Time
			level     string
			message   string
		)
		if err := rows.Scan(&id, &timestamp, &level, &message); err != nil {
			return nil, err
		}
		out = append(out, LogEntry{
			ID:        id,
			Timestamp: timestamp,
			Level:     level,
			Message:   message,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *migrationStore) setNodeTraversalStatus(nodeID, status string) error {
	// Try SRC first, then DST. This avoids a read-before-write table resolution hop.
	err := s.db.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			return w.SetNodeTraversalStatus("SRC", nodeID, status)
		})
	})
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	err = s.db.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			return w.SetNodeTraversalStatus("DST", nodeID, status)
		})
	})
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("node %s not found", nodeID)
	}
	return err
}

func (s *migrationStore) setNodeCopyStatus(nodeID, status string) (int64, map[string]int64, error) {
	node, err := pull.GetNodeByID(s.db, "SRC", nodeID)
	if err != nil {
		return 0, nil, err
	}
	if node == nil {
		return 0, nil, fmt.Errorf("node %s not found in SRC", nodeID)
	}
	switch status {
	case db.CopyStatusPending:
		if node.CopyStatus != db.CopyStatusFailed {
			return 0, nil, nil
		}
	case db.CopyStatusFailed:
		if node.CopyStatus != db.CopyStatusPending && node.CopyStatus != "" {
			return 0, nil, nil
		}
	default:
		return 0, nil, fmt.Errorf("unsupported copy status transition to %q", status)
	}
	err = s.db.RunWrite(context.Background(), func(sess *db.WriteSession) error {
		return sess.WithTx(func(w *db.Writer) error {
			return w.SetNodeCopyStatus("SRC", nodeID, status)
		})
	})
	if err != nil {
		return 0, nil, err
	}
	deltas := make(map[string]int64)
	if status == db.CopyStatusPending {
		addReviewDelta(deltas, DeltaCopyFailed, -1)
		addReviewDelta(deltas, DeltaCopyPending, 1)
	} else {
		addReviewDelta(deltas, DeltaCopyPending, -1)
		addReviewDelta(deltas, DeltaCopyFailed, 1)
	}
	if err := s.persistReviewDeltas(deltas); err != nil {
		return 0, nil, fmt.Errorf("persist review deltas: %w", err)
	}
	return 1, deltas, nil
}

func (s *migrationStore) setNodeDeleteStatus(nodeID, status string) (int64, map[string]int64, error) {
	node, err := pull.GetNodeByID(s.db, "SRC", nodeID)
	if err != nil {
		return 0, nil, err
	}
	if node == nil {
		return 0, nil, fmt.Errorf("node %s not found in SRC", nodeID)
	}
	oldDelete := node.DeleteStatus
	if oldDelete == status {
		return 0, nil, nil
	}
	switch status {
	case db.DeleteStatusPending:
		// init (empty→pending), mark-retry (failed→pending), or unskip (skipped→pending)
		if oldDelete != "" && oldDelete != db.DeleteStatusFailed && oldDelete != db.DeleteStatusSkipped {
			return 0, nil, nil
		}
	case db.DeleteStatusFailed:
		if oldDelete != db.DeleteStatusPending && oldDelete != "" {
			return 0, nil, nil
		}
	case db.DeleteStatusSkipped:
		if oldDelete != db.DeleteStatusPending && oldDelete != "" {
			return 0, nil, nil
		}
	}
	var anc subtree.SubtreeDeleteMutationResult
	err = s.db.RunWrite(context.Background(), func(sess *db.WriteSession) error {
		return sess.WithTx(func(w *db.Writer) error {
			if err := w.SetNodeDeleteStatus("SRC", nodeID, status); err != nil {
				return err
			}
			if status == db.DeleteStatusSkipped {
				var aerr error
				anc, aerr = subtree.InsertDeleteStatusEventsForAncestors(w, node.Path, db.DeleteStatusSkipped, db.SQLDeleteSubtreeSkipEligible)
				return aerr
			}
			return nil
		})
	})
	if err != nil {
		return 0, nil, err
	}
	deltas := make(map[string]int64)
	addReviewDeltaForDeleteStatusTransition(deltas, oldDelete, status)
	addDeleteSelectedSizeDelta(deltas, node, oldDelete, status)
	if anc.Affected > 0 {
		addReviewDeltaForDeleteStatus(deltas, db.DeleteStatusPending, -anc.Affected)
		addReviewDeltaForDeleteStatus(deltas, db.DeleteStatusSkipped, anc.Affected)
		addReviewDelta(deltas, DeltaFolders, -anc.Folders)
		addReviewDelta(deltas, DeltaFiles, -anc.Files)
		if anc.SelectedBytes != 0 {
			addReviewDelta(deltas, DeltaSizeDeleteSelected, -anc.SelectedBytes)
			addReviewDelta(deltas, DeltaSizeSelected, -anc.SelectedBytes)
		}
	}
	if err := s.persistReviewDeltas(deltas); err != nil {
		return 0, nil, fmt.Errorf("persist review deltas: %w", err)
	}
	return 1 + anc.Affected, deltas, nil
}

func (s *migrationStore) setNodeDeleteStatusWithPropagation(nodeID, targetStatus string) (int64, map[string]int64, error) {
	node, err := pull.GetNodeByID(s.db, "SRC", nodeID)
	if err != nil {
		return 0, nil, err
	}
	if node == nil {
		return 0, nil, fmt.Errorf("node %s not found in SRC", nodeID)
	}
	if node.Type != db.NodeTypeFolder {
		return s.setNodeDeleteStatus(nodeID, targetStatus)
	}
	rootPath := node.Path
	var mut subtree.SubtreeDeleteMutationResult
	err = s.db.RunWrite(context.Background(), func(sess *db.WriteSession) error {
		return sess.WithTx(func(w *db.Writer) error {
			var err error
			switch targetStatus {
			case db.DeleteStatusSkipped:
				mut, err = subtree.InsertDeleteStatusEventsForSubtree(w, rootPath, db.DeleteStatusSkipped, db.SQLDeleteSubtreeSkipEligible)
				if err != nil {
					return err
				}
				anc, aerr := subtree.InsertDeleteStatusEventsForAncestors(w, rootPath, db.DeleteStatusSkipped, db.SQLDeleteSubtreeSkipEligible)
				if aerr != nil {
					return aerr
				}
				mut.Affected += anc.Affected
				mut.Folders += anc.Folders
				mut.Files += anc.Files
				mut.SelectedBytes += anc.SelectedBytes
				return nil
			case db.DeleteStatusPending:
				mut, err = subtree.InsertDeleteStatusEventsForSubtree(w, rootPath, db.DeleteStatusPending, db.SQLDeleteSubtreeUnskipEligible)
			default:
				return fmt.Errorf("delete status propagation only supports skip or unskip")
			}
			return err
		})
	})
	if err != nil {
		return 0, nil, fmt.Errorf("set delete status with propagation: %w", err)
	}
	if mut.Affected == 0 {
		return 0, nil, nil
	}
	deltas := make(map[string]int64)
	pendingDelta := mut.Affected
	selectedSizeDelta := mut.SelectedBytes
	foldersDelta := mut.Folders
	filesDelta := mut.Files
	if targetStatus == db.DeleteStatusSkipped {
		pendingDelta = -mut.Affected
		selectedSizeDelta = -mut.SelectedBytes
		foldersDelta = -mut.Folders
		filesDelta = -mut.Files
	}
	addReviewDeltaForDeleteStatus(deltas, db.DeleteStatusPending, pendingDelta)
	addReviewDeltaForDeleteStatus(deltas, db.DeleteStatusSkipped, -pendingDelta)
	addReviewDelta(deltas, DeltaFolders, foldersDelta)
	addReviewDelta(deltas, DeltaFiles, filesDelta)
	if selectedSizeDelta != 0 {
		addReviewDelta(deltas, DeltaSizeDeleteSelected, selectedSizeDelta)
		addReviewDelta(deltas, DeltaSizeSelected, selectedSizeDelta)
	}
	if err := s.persistReviewDeltas(deltas); err != nil {
		return 0, nil, fmt.Errorf("persist review deltas: %w", err)
	}
	return mut.Affected, deltas, nil
}

// markNodeForRetryDiscovery looks up the node by ID in SRC then DST (nodeID is either a SRC or DST node ID).
// For a SRC node at path P: deletes all DST descendants under P (not the DST node at P), marks SRC and DST node at P as pending. Non-recursive; DST children are removed so they can be re-derived on next traversal.
// Accepts failed (normal retry) or excluded (root-pick / traversal-skipped activate).
func (s *migrationStore) markNodeForRetryDiscovery(nodeID string) (int64, map[string]int64, error) {
	srcNode, err := pull.GetNodeByID(s.db, "SRC", nodeID)
	if err != nil {
		return 0, nil, err
	}
	if srcNode != nil {
		fromExcluded := srcNode.TraversalStatus == db.StatusExcluded
		if srcNode.TraversalStatus != db.StatusFailed && !fromExcluded {
			return 0, nil, nil
		}
		path := srcNode.Path
		dstAtPath, _ := pull.GetNodeByPath(s.db, "DST", path)
		var desc subtree.DstDescendantsReviewStats
		err := s.db.RunWrite(context.Background(), func(sess *db.WriteSession) error {
			return sess.WithTx(func(w *db.Writer) error {
				var err error
				desc, err = subtree.CountDstDescendantsReviewStats(w, path)
				if err != nil {
					return err
				}
				if err := w.SetNodeTraversalStatus("SRC", nodeID, db.StatusPending); err != nil {
					return err
				}
				if fromExcluded {
					if err := w.SetNodeExcluded("SRC", nodeID, false); err != nil {
						return err
					}
				}
				if dstAtPath != nil {
					if err := w.SetNodeTraversalStatus("DST", dstAtPath.ID, db.StatusPending); err != nil {
						return err
					}
					// Deletes DST descendants; review deltas come from desc aggregates (no RecomputeStatsForDepth).
					if err := w.DeleteDescendantsUnderPath("DST", path); err != nil {
						return err
					}
				}
				return nil
			})
		})
		if err != nil {
			return 0, nil, err
		}
		deltas := make(map[string]int64)
		if fromExcluded {
			addReviewDelta(deltas, DeltaExcluded, -1)
			addReviewDelta(deltas, DeltaTraversalPendingRetry, 1)
			addReviewDelta(deltas, DeltaCopyPending, 1)
			addPendingTypeCountDelta(deltas, srcNode.Type, 1)
			if srcNode.Type == db.NodeTypeFile {
				addReviewDelta(deltas, DeltaSizeSelected, srcNode.Size)
			}
		} else {
			addReviewDelta(deltas, DeltaTraversalFailed, -1+-desc.TraversalFailed)
			addReviewDelta(deltas, DeltaTraversalPendingRetry, 1)
			addReviewDelta(deltas, DeltaFolders, -desc.Folders)
			addReviewDelta(deltas, DeltaFiles, -desc.Files)
			addReviewDelta(deltas, DeltaExcluded, -desc.Excluded)
			addReviewDelta(deltas, DeltaSizeDst, -desc.SizeDst)
		}
		if err := s.persistReviewDeltas(deltas); err != nil {
			return 0, nil, fmt.Errorf("persist review deltas: %w", err)
		}
		return 1, deltas, nil
	}
	dstNode, err := pull.GetNodeByID(s.db, "DST", nodeID)
	if err != nil || dstNode == nil {
		if err != nil {
			return 0, nil, err
		}
		return 0, nil, fmt.Errorf("node %s not found", nodeID)
	}
	if dstNode.TraversalStatus != db.StatusFailed {
		return 0, nil, nil
	}
	err = s.db.RunWrite(context.Background(), func(sess *db.WriteSession) error {
		return sess.WithTx(func(w *db.Writer) error {
			return w.SetNodeTraversalStatus("DST", nodeID, db.StatusPending)
		})
	})
	if err != nil {
		return 0, nil, err
	}
	deltas := make(map[string]int64)
	addReviewDelta(deltas, DeltaTraversalFailed, -1)
	addReviewDelta(deltas, DeltaTraversalPendingRetry, 1)
	if err := s.persistReviewDeltas(deltas); err != nil {
		return 0, nil, fmt.Errorf("persist review deltas: %w", err)
	}
	return 1, deltas, nil
}

// unmarkNodeForRetryDiscovery sets SRC node back to failed (or excluded when that was the prior status)
// and restores the paired DST node (if any) to its prior non-pending traversal status.
// not_on_src is only for DST-only paths; a DST node at a path that still has SRC must not get it.
// Does not recreate DST children that were deleted on mark-for-retry.
func (s *migrationStore) unmarkNodeForRetryDiscovery(nodeID string) (int64, map[string]int64, error) {
	srcNode, err := pull.GetNodeByID(s.db, "SRC", nodeID)
	if err != nil {
		return 0, nil, err
	}
	if srcNode != nil {
		if srcNode.TraversalStatus != db.StatusPending {
			return 0, nil, nil
		}
		path := srcNode.Path
		dstAtPath, _ := pull.GetNodeByPath(s.db, "DST", path)
		restoreExcluded := false
		err := s.db.RunWrite(context.Background(), func(sess *db.WriteSession) error {
			return sess.WithTx(func(w *db.Writer) error {
				prior, err2 := w.LatestNonPendingTraversalStatus("SRC", nodeID)
				if err2 != nil {
					return err2
				}
				restoreExcluded = prior == db.StatusExcluded
				restore := db.StatusFailed
				if restoreExcluded {
					restore = db.StatusExcluded
				}
				if err := w.SetNodeTraversalStatus("SRC", nodeID, restore); err != nil {
					return err
				}
				if restoreExcluded {
					if err := w.SetNodeExcluded("SRC", nodeID, true); err != nil {
						return err
					}
				}
				if dstAtPath != nil {
					dstPrior, err2 := w.LatestNonPendingTraversalStatus("DST", dstAtPath.ID)
					if err2 != nil {
						return err2
					}
					dstRestore := dstPrior
					if dstRestore == "" || dstRestore == db.StatusNotOnSrc {
						dstRestore = db.StatusSuccessful
					}
					if err := w.SetNodeTraversalStatus("DST", dstAtPath.ID, dstRestore); err != nil {
						return err
					}
				}
				return nil
			})
		})
		if err != nil {
			return 0, nil, err
		}
		deltas := make(map[string]int64)
		addReviewDelta(deltas, DeltaTraversalPendingRetry, -1)
		if restoreExcluded {
			addReviewDelta(deltas, DeltaExcluded, 1)
			addReviewDelta(deltas, DeltaCopyPending, -1)
			addPendingTypeCountDelta(deltas, srcNode.Type, -1)
			if srcNode.Type == db.NodeTypeFile {
				addReviewDelta(deltas, DeltaSizeSelected, -srcNode.Size)
			}
		} else {
			addReviewDelta(deltas, DeltaTraversalFailed, 1)
		}
		if err := s.persistReviewDeltas(deltas); err != nil {
			return 0, nil, fmt.Errorf("persist review deltas: %w", err)
		}
		return 1, deltas, nil
	}
	dstNode, err := pull.GetNodeByID(s.db, "DST", nodeID)
	if err != nil || dstNode == nil {
		if err != nil {
			return 0, nil, err
		}
		return 0, nil, fmt.Errorf("node %s not found", nodeID)
	}
	if dstNode.TraversalStatus != db.StatusPending {
		return 0, nil, nil
	}
	err = s.db.RunWrite(context.Background(), func(sess *db.WriteSession) error {
		return sess.WithTx(func(w *db.Writer) error {
			return w.SetNodeTraversalStatus("DST", nodeID, db.StatusFailed)
		})
	})
	if err != nil {
		return 0, nil, err
	}
	deltas := make(map[string]int64)
	addReviewDelta(deltas, DeltaTraversalPendingRetry, -1)
	addReviewDelta(deltas, DeltaTraversalFailed, 1)
	if err := s.persistReviewDeltas(deltas); err != nil {
		return 0, nil, fmt.Errorf("persist review deltas: %w", err)
	}
	return 1, deltas, nil
}

func (s *migrationStore) setNodeExcludedWithPropagation(nodeID string, excluded bool) (int64, map[string]int64, error) {
	node, err := pull.GetNodeByID(s.db, "SRC", nodeID)
	if err != nil {
		return 0, nil, err
	}
	if node == nil {
		return 0, nil, fmt.Errorf("node %s not found in SRC", nodeID)
	}
	rootPath := node.Path
	var mut subtree.SubtreeCopyMutationResult
	err = s.db.RunWrite(context.Background(), func(sess *db.WriteSession) error {
		return sess.WithTx(func(w *db.Writer) error {
			var err2 error
			if excluded {
				mut, err2 = subtree.InsertExclusionEventsForSubtree(w, "SRC", rootPath)
				if err2 != nil {
					return err2
				}
				if mut.Affected == 0 {
					return nil
				}
				if err := subtree.InsertGPLStatusEventsForSubtree(w, "SRC", rootPath, db.GPLStatusIgnored, true); err != nil {
					return err
				}
				return subtree.InsertGPLStatusEventsForSubtree(w, "DST", rootPath, db.GPLStatusIgnored, true)
			}
			mut, err2 = subtree.InsertUnexcludeEventsForSubtree(w, "SRC", rootPath)
			if err2 != nil {
				return err2
			}
			if mut.Affected == 0 {
				return nil
			}
			if err := subtree.InsertGPLRestoredEventsForSubtree(w, "SRC", rootPath); err != nil {
				return err
			}
			return subtree.InsertGPLRestoredEventsForSubtree(w, "DST", rootPath)
		})
	})
	if err != nil {
		return 0, nil, fmt.Errorf("set exclusion with propagation: %w", err)
	}
	if mut.Affected == 0 {
		return 0, nil, nil
	}
	deltas := make(map[string]int64)
	sign := int64(1)
	if excluded {
		sign = -1
	}
	addReviewDelta(deltas, DeltaExcluded, -sign*mut.Affected)
	addReviewDelta(deltas, DeltaCopyPending, sign*mut.Affected)
	addReviewDelta(deltas, DeltaSizeSelected, sign*mut.PendingBytes)
	addReviewDelta(deltas, DeltaFolders, sign*mut.Folders)
	addReviewDelta(deltas, DeltaFiles, sign*mut.Files)
	cw := db.DepthWorkAbsolute{Folders: sign * mut.Folders, Files: sign * mut.Files, Bytes: sign * mut.PendingBytes}
	reason := db.CopyWorkReasonReviewUnexclude
	if excluded {
		reason = db.CopyWorkReasonReviewExclude
	}
	if err := stats.AdjustCopyWorkForReview(s.db, cw, reason); err != nil {
		return 0, nil, fmt.Errorf("adjust copy work: %w", err)
	}
	if err := s.persistReviewDeltas(deltas); err != nil {
		return 0, nil, fmt.Errorf("persist review deltas: %w", err)
	}
	return mut.Affected, deltas, nil
}

func conditionStringValue(v any) (string, bool) {
	if v == nil {
		return "", false
	}
	switch t := v.(type) {
	case string:
		return t, true
	case float64:
		return strconv.FormatInt(int64(t), 10), true
	case int:
		return strconv.Itoa(t), true
	case int64:
		return strconv.FormatInt(t, 10), true
	default:
		return fmt.Sprintf("%v", t), true
	}
}

func conditionIntValue(v any) (int, bool) {
	switch t := v.(type) {
	case int:
		return t, true
	case int64:
		return int(t), true
	case float64:
		return int(t), true
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(t))
		return n, err == nil
	default:
		return 0, false
	}
}

func conditionInt64Value(v any) (int64, bool) {
	switch t := v.(type) {
	case int:
		return int64(t), true
	case int64:
		return t, true
	case float64:
		return int64(t), true
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func normalizeDepthSizeOp(op string) string {
	switch strings.ToLower(strings.TrimSpace(op)) {
	case "gt", ">":
		return ">"
	case "gte", ">=":
		return ">="
	case "lt", "<":
		return "<"
	case "lte", "<=":
		return "<="
	case "equals", "=":
		return "="
	default:
		return "="
	}
}

// searchRequestToReviewFilter maps API/UI SearchRequest onto review.ReviewFilter (merged view, status from events).
func searchRequestToReviewFilter(req SearchRequest) review.ReviewFilter {
	f := review.ReviewFilter{
		ParentPath:             strings.TrimSpace(req.Path),
		Query:                  strings.TrimSpace(req.Query),
		FoldersOnly:            req.FoldersOnly,
		ExcludeRoot:            req.Path == "",
		StatusSearchType:       strings.TrimSpace(req.StatusSearchType),
		TraversalStatus:        strings.TrimSpace(req.TraversalStatus),
		CopyStatus:             strings.TrimSpace(req.CopyStatus),
		DeleteStatus:           strings.TrimSpace(req.DeleteStatus),
		ExcludeDestinationOnly: excludeDestinationOnly(req.IncludeDestinationOnly),
	}
	for _, c := range req.Conditions {
		field := strings.ToLower(strings.TrimSpace(c.Field))
		switch field {
		case "path":
			if s, ok := conditionStringValue(c.Value); ok && strings.TrimSpace(s) != "" {
				f.Query = strings.TrimSpace(s)
				f.QueryField = "path"
			}
		case "name":
			if s, ok := conditionStringValue(c.Value); ok && strings.TrimSpace(s) != "" {
				f.Query = strings.TrimSpace(s)
				f.QueryField = "name"
			}
		case "type":
			if s, ok := conditionStringValue(c.Value); ok {
				f.TypeFilter = strings.ToLower(strings.TrimSpace(s))
			}
		case "traversalstatus":
			if s, ok := conditionStringValue(c.Value); ok {
				f.TraversalStatus = strings.TrimSpace(s)
			}
		case "copystatus":
			if s, ok := conditionStringValue(c.Value); ok {
				f.CopyStatus = strings.TrimSpace(s)
			}
		case "deletestatus":
			if s, ok := conditionStringValue(c.Value); ok {
				f.DeleteStatus = strings.TrimSpace(s)
			}
		case "pathissuestatus", "pathissuefilter", "compatibilitystatus":
			if s, ok := conditionStringValue(c.Value); ok {
				f.PathIssueFilter = strings.TrimSpace(s)
			}
		case "pathissuecategory", "compatibilitycategory":
			if s, ok := conditionStringValue(c.Value); ok {
				f.PathIssueCategory = strings.TrimSpace(s)
			}
		case "depth":
			f.DepthOperator = normalizeDepthSizeOp(c.Operator)
			if n, ok := conditionIntValue(c.Value); ok {
				f.DepthValue = &n
			}
		case "size":
			f.SizeOperator = normalizeDepthSizeOp(c.Operator)
			if n, ok := conditionInt64Value(c.Value); ok {
				f.SizeValue = &n
			}
		}
	}
	return f
}

// excludeDestinationOnly maps includeDestinationOnly pointer: nil/true → keep dst-only; false → hide.
func excludeDestinationOnly(include *bool) bool {
	return include != nil && !*include
}

func sanitizeSort(sortBy, sortDirection string) string {
	column := "path"
	switch strings.ToLower(strings.TrimSpace(sortBy)) {
	case "name":
		column = "name"
	case "depth":
		column = "depth"
	case "size":
		column = "size"
	case "type":
		column = "type"
	case "status", "traversalstatus", "traversal_status":
		column = "src_traversal_status"
	case "copystatus", "copy_status":
		column = "copy_status"
	case "path":
		column = "path"
	}
	direction := "ASC"
	if strings.EqualFold(strings.TrimSpace(sortDirection), "desc") {
		direction = "DESC"
	}
	primary := column + " " + direction
	// Stable ordering and deterministic pagination when primary values tie.
	if column == "path" {
		return primary
	}
	return primary + ", path ASC"
}

func (s *migrationStore) listChildrenDiffs(req ListChildrenDiffsRequest) (ListChildrenDiffsResult, error) {
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	offset := req.Offset
	if offset < 0 {
		offset = 0
	}
	orderBy := sanitizeSort(req.SortBy, req.SortDirection)
	f := review.ReviewFilter{
		ParentPath:             req.Path,
		FoldersOnly:            req.FoldersOnly,
		TraversalStatus:        strings.TrimSpace(req.TraversalStatus),
		CopyStatus:             strings.TrimSpace(req.CopyStatus),
		ExcludeDestinationOnly: excludeDestinationOnly(req.IncludeDestinationOnly),
	}
	rows, total, err := review.ListMergedReviewDiffs(s.db, f, orderBy, limit, offset)
	if err != nil {
		return ListChildrenDiffsResult{}, err
	}
	items := make([]DiffItem, 0, len(rows))
	for i := range rows {
		items = append(items, mergedRowToDiffItem(rows[i]))
	}
	if err := s.enrichDiffItemsWithFailureLogs(items); err != nil {
		return ListChildrenDiffsResult{}, err
	}
	return ListChildrenDiffsResult{
		Items:  items,
		Total:  total,
		Limit:  limit,
		Offset: offset,
	}, nil
}

func (s *migrationStore) searchPathReviewItems(req SearchRequest) (SearchResult, error) {
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	offset := req.Offset
	if offset < 0 {
		offset = 0
	}
	orderBy := sanitizeSort(req.SortBy, req.SortDirection)
	f := searchRequestToReviewFilter(req)
	if !review.ReviewFilterHasSearchPredicate(f) {
		return SearchResult{}, ErrSearchRequiresFilter
	}
	rows, hasMore, err := review.ListMergedReviewDiffsPage(s.db, f, orderBy, limit, offset)
	if err != nil {
		return SearchResult{}, err
	}
	items := make([]DiffItem, 0, len(rows))
	for i := range rows {
		items = append(items, mergedRowToDiffItem(rows[i]))
	}
	if err := s.enrichDiffItemsWithFailureLogs(items); err != nil {
		return SearchResult{}, err
	}
	return SearchResult{
		Items:   items,
		Total:   nil, // unknown on search hot path; use GetSearchStats for exact counts
		HasMore: hasMore,
		Limit:   limit,
		Offset:  offset,
	}, nil
}

func (s *migrationStore) getChildrenDiffsStats(path string, foldersOnly bool, includeDestinationOnly *bool) (DiffsStats, error) {
	f := review.ReviewFilter{
		ParentPath:             path,
		FoldersOnly:            foldersOnly,
		ExcludeDestinationOnly: excludeDestinationOnly(includeDestinationOnly),
	}
	stats, err := review.GetMergedReviewStats(s.db, f)
	if err != nil {
		return DiffsStats{}, err
	}
	return DiffsStats{
		Total:           stats.Total,
		Folders:         stats.Folders,
		Files:           stats.Files,
		MissingOnSource: stats.MissingOnSource,
		MissingOnDest:   stats.MissingOnDest,
		Excluded:        stats.Excluded,
	}, nil
}

func (s *migrationStore) getSearchStats(req SearchRequest) (DiffsStats, error) {
	f := searchRequestToReviewFilter(req)
	if !review.ReviewFilterHasSearchPredicate(f) {
		return DiffsStats{}, ErrSearchRequiresFilter
	}
	stats, err := review.GetMergedReviewStats(s.db, f)
	if err != nil {
		return DiffsStats{}, err
	}
	return DiffsStats{
		Total:           stats.Total,
		Folders:         stats.Folders,
		Files:           stats.Files,
		MissingOnSource: stats.MissingOnSource,
		MissingOnDest:   stats.MissingOnDest,
		Excluded:        stats.Excluded,
	}, nil
}

func (s *migrationStore) getQueueMetrics() (QueueMetricsSnapshot, error) {
	raw, err := stats.GetAllQueueStats(s.db)
	if err != nil {
		return QueueMetricsSnapshot{}, err
	}
	out := QueueMetricsSnapshot{
		Queues: make(map[string]map[string]any, len(raw)),
	}
	for key, blob := range raw {
		var parsed map[string]any
		if err := json.Unmarshal(blob, &parsed); err != nil {
			parsed = map[string]any{
				"raw": string(blob),
			}
		}
		out.Queues[key] = parsed
	}
	return out, nil
}
