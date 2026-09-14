// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/failurelog"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/filterapply"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/review"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/subtree"
	"codeberg.org/Sylos/Migration-Engine/pkg/filter"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
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
	case DeltaCopyPendingRetry:
		return db.ReviewKeyCopyPendingRetry
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
	case DeltaDeleteSkipped:
		return db.ReviewKeyDeleteSkipped
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
	return s.db.ApplyReviewStatsDeltas(dbDeltas)
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
		DstSize:            r.DstSize,
		HasDstSize:         r.HasDstSize,
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

func (s *migrationStore) enrichDiffItemsWithDisplayPaths(items []DiffItem) error {
	if s == nil || s.db == nil || len(items) == 0 {
		return nil
	}
	srcIDs := make([]string, 0, len(items))
	dstIDs := make([]string, 0, len(items))
	for i := range items {
		if items[i].SrcNodeID != "" {
			srcIDs = append(srcIDs, items[i].SrcNodeID)
		}
		if items[i].DstNodeID != "" {
			dstIDs = append(dstIDs, items[i].DstNodeID)
		}
	}
	srcPaths, err := db.ComposeDisplayPaths(s.db, "SRC", srcIDs)
	if err != nil {
		return err
	}
	dstPaths, err := db.ComposeDisplayPaths(s.db, "DST", dstIDs)
	if err != nil {
		return err
	}
	for i := range items {
		if items[i].SrcNodeID != "" {
			items[i].DisplayPath = srcPaths[items[i].SrcNodeID]
		} else if items[i].DstNodeID != "" {
			items[i].DisplayPath = dstPaths[items[i].DstNodeID]
		}
		if items[i].DstNodeID != "" {
			items[i].DstDisplayPath = dstPaths[items[i].DstNodeID]
		}
		// When SRC exists and DST was renamed, keep DisplayPath as SRC names;
		// DstDisplayPath carries the destination-side friendly path for the leaf.
		if items[i].DisplayPath == "" && items[i].DstDisplayPath != "" {
			items[i].DisplayPath = items[i].DstDisplayPath
		}
	}
	return nil
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
	if pendingStatus == db.DeleteStatusPendingExplicit {
		return status == "" || db.DeleteStatusIsPending(status)
	}
	return status == "" || status == pendingStatus
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
	wasPending := statusCountsAsPending(from, db.DeleteStatusPendingExplicit)
	nowPending := statusCountsAsPending(to, db.DeleteStatusPendingExplicit)
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
	mut, err := s.db.ApplyNodeCopyExclusion(nodeID, excluded)
	if err != nil {
		return 0, nil, err
	}
	if mut.Affected == 0 {
		return 0, nil, nil
	}
	subMut := subtree.SubtreeCopyMutationResult{
		Affected: mut.Affected, Folders: mut.Folders, Files: mut.Files, PendingBytes: mut.PendingBytes,
	}
	deltas, err := s.persistCopyExclusionMutation(subMut, excluded)
	if err != nil {
		return 0, nil, err
	}
	return mut.Affected, deltas, nil
}

func (s *migrationStore) listRecentLogs(limit int) ([]LogEntry, error) {
	if limit <= 0 {
		limit = 50
	}
	recs, err := s.db.Ops().ListRecentLogs(limit)
	if err != nil {
		return nil, fmt.Errorf("get recent logs: %w", err)
	}
	out := make([]LogEntry, 0, len(recs))
	for _, rec := range recs {
		ts := rec.At
		if ts.IsZero() {
			ts = time.Now()
		}
		out = append(out, LogEntry{
			ID:        rec.ID,
			Timestamp: ts,
			Level:     rec.Level,
			Message:   rec.Message,
		})
	}
	return out, nil
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
	mark := status == db.CopyStatusPending
	var opsMut opsdb.SubtreeMutationResult
	if node.Type == db.NodeTypeFolder {
		opsMut, err = s.db.Ops().ApplySubtreeCopyRetry(node.Path, mark)
	} else {
		opsMut, err = s.db.Ops().ApplyNodeCopyRetry(nodeID, mark)
	}
	if err != nil {
		return 0, nil, err
	}
	mut := db.CopyMutationResult{
		Affected: opsMut.Affected, Folders: opsMut.Folders, Files: opsMut.Files, PendingBytes: opsMut.SelectedBytes,
	}
	if mut.Affected == 0 {
		return 0, nil, nil
	}
	deltas := make(map[string]int64)
	n := mut.Affected
	if status == db.CopyStatusPending {
		addReviewDelta(deltas, DeltaCopyFailed, -n)
		addReviewDelta(deltas, DeltaCopyPending, n)
		addReviewDelta(deltas, DeltaCopyPendingRetry, n)
		addReviewDelta(deltas, DeltaFolders, mut.Folders)
		addReviewDelta(deltas, DeltaFiles, mut.Files)
		addReviewDelta(deltas, DeltaSizeSelected, mut.PendingBytes)
	} else {
		addReviewDelta(deltas, DeltaCopyPending, -n)
		addReviewDelta(deltas, DeltaCopyPendingRetry, -n)
		addReviewDelta(deltas, DeltaCopyFailed, n)
		addReviewDelta(deltas, DeltaFolders, -mut.Folders)
		addReviewDelta(deltas, DeltaFiles, -mut.Files)
		addReviewDelta(deltas, DeltaSizeSelected, -mut.PendingBytes)
	}
	if err := s.persistReviewDeltas(deltas); err != nil {
		return 0, nil, fmt.Errorf("persist review deltas: %w", err)
	}
	return mut.Affected, deltas, nil
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
	case db.DeleteStatusPendingExplicit, db.DeleteStatusPendingInherited:
		// init, mark-retry (failed→pending*), or unskip (skipped→pending*)
		if oldDelete != "" && oldDelete != db.DeleteStatusFailed && oldDelete != db.DeleteStatusSkipped &&
			!db.DeleteStatusIsPending(oldDelete) {
			return 0, nil, nil
		}
	case db.DeleteStatusFailed:
		if !db.DeleteStatusIsPending(oldDelete) && oldDelete != "" {
			return 0, nil, nil
		}
	case db.DeleteStatusSkipped:
		if !db.DeleteStatusIsPending(oldDelete) && oldDelete != "" {
			return 0, nil, nil
		}
	}
	var mut db.DeleteMutationResult
	if node.Type == db.NodeTypeFolder {
		switch {
		case db.DeleteStatusIsPending(status):
			opsMut, aerr := s.db.Ops().ApplySubtreeDeleteRetry(node.Path, true)
			err = aerr
			mut = db.DeleteMutationResult{
				Affected: opsMut.Affected, Folders: opsMut.Folders, Files: opsMut.Files, SelectedBytes: opsMut.SelectedBytes,
			}
		case status == db.DeleteStatusFailed:
			opsMut, aerr := s.db.Ops().ApplySubtreeDeleteRetry(node.Path, false)
			err = aerr
			mut = db.DeleteMutationResult{
				Affected: opsMut.Affected, Folders: opsMut.Folders, Files: opsMut.Files, SelectedBytes: opsMut.SelectedBytes,
			}
		case status == db.DeleteStatusSkipped:
			mut, err = s.db.ApplyDeleteForestSkip(node.Path)
		}
	} else {
		switch {
		case db.DeleteStatusIsPending(status) && oldDelete == db.DeleteStatusSkipped:
			mut, err = s.db.ApplyDeleteForestUnskip(node.Path)
		case db.DeleteStatusIsPending(status):
			opsMut, aerr := s.db.Ops().ApplyNodeDeleteRetry(nodeID, true)
			err = aerr
			mut = db.DeleteMutationResult{
				Affected: opsMut.Affected, Folders: opsMut.Folders, Files: opsMut.Files, SelectedBytes: opsMut.SelectedBytes,
			}
		case status == db.DeleteStatusFailed:
			opsMut, aerr := s.db.Ops().ApplyNodeDeleteRetry(nodeID, false)
			err = aerr
			mut = db.DeleteMutationResult{
				Affected: opsMut.Affected, Folders: opsMut.Folders, Files: opsMut.Files, SelectedBytes: opsMut.SelectedBytes,
			}
		case status == db.DeleteStatusSkipped:
			mut, err = s.db.ApplyDeleteForestSkip(node.Path)
		}
	}
	if err != nil {
		return 0, nil, err
	}
	if mut.Affected == 0 {
		return 0, nil, nil
	}
	deltas := make(map[string]int64)
	n := mut.Affected
	addReviewDeltaForDeleteStatus(deltas, oldDelete, -n)
	addReviewDeltaForDeleteStatus(deltas, status, n)
	wasPending := statusCountsAsPending(oldDelete, db.DeleteStatusPendingExplicit)
	nowPending := statusCountsAsPending(status, db.DeleteStatusPendingExplicit)
	if wasPending != nowPending {
		sign := int64(1)
		if wasPending {
			sign = -1
		}
		addReviewDelta(deltas, DeltaFolders, sign*mut.Folders)
		addReviewDelta(deltas, DeltaFiles, sign*mut.Files)
		if mut.SelectedBytes != 0 {
			addReviewDelta(deltas, DeltaSizeDeleteSelected, sign*mut.SelectedBytes)
			addReviewDelta(deltas, DeltaSizeSelected, sign*mut.SelectedBytes)
		}
	}
	if err := s.persistReviewDeltas(deltas); err != nil {
		return 0, nil, fmt.Errorf("persist review deltas: %w", err)
	}
	return mut.Affected, deltas, nil
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
	var mut db.DeleteMutationResult
	if targetStatus == db.DeleteStatusSkipped {
		mut, err = s.db.ApplyDeleteForestSkip(rootPath)
	} else {
		mut, err = s.db.ApplyDeleteForestUnskip(rootPath)
	}
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
	addReviewDeltaForDeleteStatus(deltas, db.DeleteStatusPendingExplicit, pendingDelta)
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

func (s *migrationStore) loadReviewNode(table, id string) (*db.NodeState, error) {
	return pull.GetNodeByID(s.db, table, id)
}

func (s *migrationStore) pairedReviewNode(table, path, mappedID string) (*db.NodeState, error) {
	n, err := pull.GetNodeByPath(s.db, table, path)
	if err != nil {
		return nil, err
	}
	if n != nil {
		return n, nil
	}
	if mappedID == "" {
		return nil, nil
	}
	return s.loadReviewNode(table, mappedID)
}

func (s *migrationStore) mappedID(side, nodeID string) (string, error) {
	if s.db.Ops() == nil {
		return "", nil
	}
	if side == "DST" {
		m, ok, err := s.db.Ops().GetMapBySrc(nodeID)
		if err != nil || !ok {
			return "", err
		}
		return m.DstID, nil
	}
	m, ok, err := s.db.Ops().GetMapByDst(nodeID)
	if err != nil || !ok {
		return "", err
	}
	return m.SrcID, nil
}

// reviewPairFromID resolves the SRC/DST pair for a review node id (either side).
func (s *migrationStore) reviewPairFromID(nodeID string) (src, dst *db.NodeState, err error) {
	src, err = s.loadReviewNode("SRC", nodeID)
	if err != nil {
		return nil, nil, err
	}
	if src != nil {
		mapped, err := s.mappedID("DST", src.ID)
		if err != nil {
			return nil, nil, err
		}
		dst, err = s.pairedReviewNode("DST", src.Path, mapped)
		return src, dst, err
	}
	dst, err = s.loadReviewNode("DST", nodeID)
	if err != nil {
		return nil, nil, err
	}
	if dst == nil {
		return nil, nil, fmt.Errorf("node %s not found", nodeID)
	}
	mapped, err := s.mappedID("SRC", dst.ID)
	if err != nil {
		return nil, nil, err
	}
	src, err = s.pairedReviewNode("SRC", dst.Path, mapped)
	return src, dst, err
}

// markNodeForRetryDiscovery marks whichever side(s) of the pair are failed (or SRC excluded)
// via live Badger path-prefix mutations.
func (s *migrationStore) markNodeForRetryDiscovery(nodeID string) (int64, map[string]int64, error) {
	srcNode, dstNode, err := s.reviewPairFromID(nodeID)
	if err != nil {
		return 0, nil, err
	}
	srcRetry := srcNode != nil && (srcNode.TraversalStatus == db.StatusFailed || srcNode.TraversalStatus == db.StatusExcluded)
	dstRetry := dstNode != nil && dstNode.TraversalStatus == db.StatusFailed
	if !srcRetry && !dstRetry {
		return 0, nil, nil
	}
	if srcRetry {
		dstID := ""
		if dstNode != nil {
			dstID = dstNode.ID
		}
		res, err := s.db.Ops().ApplyTraversalRetryMark(srcNode.ID, dstID)
		if err != nil {
			return 0, nil, err
		}
		if res.Affected == 0 {
			return 0, nil, nil
		}
		purged := res.DstPurged
		deltas := make(map[string]int64)
		addReviewDelta(deltas, DeltaTraversalFailed, -res.FromFailed)
		addReviewDelta(deltas, DeltaExcluded, -res.FromExcluded)
		addReviewDelta(deltas, DeltaTraversalPendingRetry, res.Affected)
		// Excluded→retry becomes copy-pending in Badger; credit the copy counter once here.
		// Failed→retry keeps copy_status=pending (already in copy/pending); do not debit/credit it.
		if res.FromExcluded > 0 {
			addReviewDelta(deltas, DeltaCopyPending, res.FromExcluded)
			addPendingTypeCountDelta(deltas, srcNode.Type, res.FromExcluded)
			if srcNode.Type == db.NodeTypeFile {
				addReviewDelta(deltas, DeltaSizeSelected, srcNode.Size)
			}
		}
		// DST purge removes destination nodes only. folders/files and traversal failed/excluded
		// review keys are SRC inventory; only sizeDst tracks purged DST bytes.
		addReviewDelta(deltas, DeltaSizeDst, -purged.SizeDst)
		if err := s.persistReviewDeltas(deltas); err != nil {
			return 0, nil, fmt.Errorf("persist review deltas: %w", err)
		}
		return res.Affected, deltas, nil
	}
	res, err := s.db.Ops().ApplyDSTTraversalRetry(dstNode.ID, true)
	if err != nil {
		return 0, nil, err
	}
	if res.Affected == 0 {
		return 0, nil, nil
	}
	// DST-only marks enroll pend:trav but do not move SRC Failed / pending_retry.
	return res.Affected, nil, nil
}

// unmarkNodeForRetryDiscovery clears a live discovery-retry mark on the pair.
func (s *migrationStore) unmarkNodeForRetryDiscovery(nodeID string) (int64, map[string]int64, error) {
	srcNode, dstNode, err := s.reviewPairFromID(nodeID)
	if err != nil {
		return 0, nil, err
	}
	if srcNode != nil && srcNode.TraversalStatus == db.StatusPending {
		dstID := ""
		if dstNode != nil {
			dstID = dstNode.ID
		}
		res, err := s.db.Ops().ApplyTraversalRetryUnmark(srcNode.ID, dstID)
		if err != nil {
			return 0, nil, err
		}
		if res.Affected == 0 {
			return 0, nil, nil
		}
		deltas := make(map[string]int64)
		addReviewDelta(deltas, DeltaTraversalPendingRetry, -res.Affected)
		addReviewDelta(deltas, DeltaExcluded, res.FromExcluded)
		addReviewDelta(deltas, DeltaTraversalFailed, res.FromFailed)
		if res.FromExcluded > 0 {
			addReviewDelta(deltas, DeltaCopyPending, -res.FromExcluded)
			addPendingTypeCountDelta(deltas, srcNode.Type, -res.FromExcluded)
			if srcNode.Type == db.NodeTypeFile {
				addReviewDelta(deltas, DeltaSizeSelected, -srcNode.Size)
			}
		}
		if err := s.persistReviewDeltas(deltas); err != nil {
			return 0, nil, fmt.Errorf("persist review deltas: %w", err)
		}
		return res.Affected, deltas, nil
	}
	if dstNode == nil || dstNode.TraversalStatus != db.StatusPending {
		return 0, nil, nil
	}
	res, err := s.db.Ops().ApplyDSTTraversalRetry(dstNode.ID, false)
	if err != nil {
		return 0, nil, err
	}
	if res.Affected == 0 {
		return 0, nil, nil
	}
	return res.Affected, nil, nil
}

func (s *migrationStore) setNodeExcludedWithPropagation(nodeID string, excluded bool) (int64, map[string]int64, error) {
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
	} else if !node.Excluded {
		return 0, nil, nil
	}
	mut, err := s.db.ApplySubtreeCopyExclusion(node.Path, excluded, nil)
	if err != nil {
		return 0, nil, fmt.Errorf("set exclusion with propagation: %w", err)
	}
	if mut.Affected == 0 {
		return 0, nil, nil
	}
	subMut := subtree.SubtreeCopyMutationResult{
		Affected: mut.Affected, Folders: mut.Folders, Files: mut.Files, PendingBytes: mut.PendingBytes,
	}
	deltas, err := s.persistCopyExclusionMutation(subMut, excluded)
	if err != nil {
		return 0, nil, err
	}
	return mut.Affected, deltas, nil
}

func (s *migrationStore) applySearchExclusion(
	f review.ReviewFilter,
	criteriaJSON string,
	exceptIDs []string,
	applicationID string,
	eventTime int64,
) (int64, map[string]int64, error) {
	mut, err := filterapply.ApplySearchExclusionOps(s.db, f, criteriaJSON, exceptIDs, applicationID, eventTime)
	if err != nil {
		return 0, nil, fmt.Errorf("apply search exclusion: %w", err)
	}
	if mut.Affected == 0 {
		return 0, nil, nil
	}
	subMut := subtree.SubtreeCopyMutationResult{
		Affected: mut.Affected, Folders: mut.Folders, Files: mut.Files, PendingBytes: mut.PendingBytes,
	}
	deltas, err := s.persistCopyExclusionMutation(subMut, true)
	if err != nil {
		return 0, nil, err
	}
	return mut.Affected, deltas, nil
}

func (s *migrationStore) applySearchUnexclusion(
	f review.ReviewFilter,
	exceptIDs []string,
	eventTime int64,
) (int64, map[string]int64, error) {
	mut, err := filterapply.ApplySearchUnexclusionOps(s.db, f, exceptIDs, eventTime)
	if err != nil {
		return 0, nil, fmt.Errorf("apply search unexclusion: %w", err)
	}
	if mut.Affected == 0 {
		return 0, nil, nil
	}
	subMut := subtree.SubtreeCopyMutationResult{
		Affected: mut.Affected, Folders: mut.Folders, Files: mut.Files, PendingBytes: mut.PendingBytes,
	}
	deltas, err := s.persistCopyExclusionMutation(subMut, false)
	if err != nil {
		return 0, nil, err
	}
	return mut.Affected, deltas, nil
}

func (s *migrationStore) persistCopyExclusionMutation(mut subtree.SubtreeCopyMutationResult, excluded bool) (map[string]int64, error) {
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
		return nil, fmt.Errorf("adjust copy work: %w", err)
	}
	if err := s.persistReviewDeltas(deltas); err != nil {
		return nil, fmt.Errorf("persist review deltas: %w", err)
	}
	return deltas, nil
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
func searchRequestToReviewFilter(req SearchRequest) (review.ReviewFilter, error) {
	f := review.ReviewFilter{
		ParentPath:             strings.TrimSpace(req.Path),
		UnderPath:              strings.TrimSpace(req.UnderPath),
		Query:                  strings.TrimSpace(req.Query),
		FoldersOnly:            req.FoldersOnly,
		ExcludeRoot:            req.Path == "",
		StatusSearchType:       strings.TrimSpace(req.StatusSearchType),
		TraversalStatus:        strings.TrimSpace(req.TraversalStatus),
		CopyStatus:             strings.TrimSpace(req.CopyStatus),
		DeleteStatus:           strings.TrimSpace(req.DeleteStatus),
		ExcludeDestinationOnly: excludeDestinationOnly(req.IncludeDestinationOnly),
		AfterPath:              strings.TrimSpace(req.AfterPath),
		AfterID:                strings.TrimSpace(req.AfterID),
	}
	for _, c := range req.Conditions {
		field := strings.ToLower(strings.TrimSpace(c.Field))
		switch field {
		case "path":
			if s, ok := conditionStringValue(c.Value); ok && strings.TrimSpace(s) != "" {
				// Repeated path conditions are ordered "in path" segments (not id_path LIKE).
				f.PathSegments = append(f.PathSegments, strings.TrimSpace(s))
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
	if !filter.EmptyRuleset(req.Ruleset) {
		rs := *req.Ruleset
		segs, rest, hasRest := splitSearchPathSegments(rs.RootGroup)
		for _, seg := range segs {
			f.PathSegments = append(f.PathSegments, seg)
		}
		if hasRest {
			rs.RootGroup = rest
			compiled, err := filter.Compile(rs)
			if err != nil {
				return f, fmt.Errorf("compile search ruleset: %w", err)
			}
			f.CompiledFilter = compiled
			liftReviewSearchFieldsFromRuleset(&f, compiled.Ruleset.RootGroup)
		}
	}
	return f, nil
}

// splitSearchPathSegments lifts path rules into ordered in-path segments and
// returns the ruleset without those leaves. Open path contains/glob/regex is
// not a search index; segments use the same idx:seg ancestry path as the search bar.
// Overview: docs/search_indexes.md.
func splitSearchPathSegments(g filter.Group) (segs []string, rest filter.Group, hasRest bool) {
	rest = g
	rest.Children = nil
	for _, ch := range g.Children {
		if ch.Condition != nil && ch.Condition.Field == filter.FieldPath {
			if !ch.Condition.Negate {
				segs = append(segs, pathSegmentValues(ch.Condition.Value)...)
			}
			continue
		}
		if ch.Group != nil {
			nestedSegs, nested, nestedOK := splitSearchPathSegments(*ch.Group)
			segs = append(segs, nestedSegs...)
			if nestedOK {
				copy := nested
				rest.Children = append(rest.Children, filter.Child{Group: &copy})
			}
			continue
		}
		if ch.Condition != nil {
			rest.Children = append(rest.Children, ch)
		}
	}
	return segs, rest, len(rest.Children) > 0
}

func pathSegmentValues(v any) []string {
	var raw []string
	switch t := v.(type) {
	case string:
		raw = []string{t}
	case []string:
		raw = t
	case []any:
		for _, x := range t {
			if s, ok := x.(string); ok {
				raw = append(raw, s)
			}
		}
	}
	out := make([]string, 0, len(raw))
	for _, s := range raw {
		s = strings.TrimSpace(s)
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

// liftReviewSearchFieldsFromRuleset maps search-only ruleset leaves onto flat ReviewFilter
// status / path-issue fields so Badger post-filters and status overlays stay consistent.
func liftReviewSearchFieldsFromRuleset(f *review.ReviewFilter, g filter.Group) {
	if f == nil {
		return
	}
	for _, ch := range g.Children {
		if ch.Group != nil {
			liftReviewSearchFieldsFromRuleset(f, *ch.Group)
			continue
		}
		if ch.Condition == nil {
			continue
		}
		c := ch.Condition
		switch c.Field {
		case filter.FieldReviewStatus:
			if c.Operator != filter.OpEQ && c.Operator != "" {
				continue
			}
			s, ok := conditionStringValue(c.Value)
			if !ok || strings.TrimSpace(s) == "" {
				continue
			}
			applyReviewStatusToken(f, strings.TrimSpace(s))
		case filter.FieldPathIssueStatus:
			if c.Operator != filter.OpEQ && c.Operator != "" {
				continue
			}
			s, ok := conditionStringValue(c.Value)
			if !ok || strings.TrimSpace(s) == "" {
				continue
			}
			if f.PathIssueFilter == "" {
				f.PathIssueFilter = strings.TrimSpace(s)
			}
		case filter.FieldPathIssueCategory:
			if c.Operator != filter.OpEQ && c.Operator != "" {
				continue
			}
			s, ok := conditionStringValue(c.Value)
			if !ok || strings.TrimSpace(s) == "" {
				continue
			}
			if f.PathIssueCategory == "" {
				f.PathIssueCategory = strings.TrimSpace(s)
			}
		}
	}
}

// applyReviewStatusToken maps UI review_status onto flat copy/traversal filters (copy-plan defaults).
func applyReviewStatusToken(f *review.ReviewFilter, token string) {
	switch strings.ToLower(token) {
	case "pending", "pending_retry":
		if f.CopyStatus == "" {
			f.CopyStatus = db.CopyStatusPending
			if f.StatusSearchType == "" {
				f.StatusSearchType = "copy"
			}
		}
	case "excluded":
		if f.CopyStatus == "" {
			f.CopyStatus = db.CopyStatusExcluded
			if f.StatusSearchType == "" {
				f.StatusSearchType = "copy"
			}
		}
	case "failed":
		if f.CopyStatus == "" {
			f.CopyStatus = db.StatusFailed
			if f.StatusSearchType == "" {
				f.StatusSearchType = "copy"
			}
		}
	case "successful":
		if f.CopyStatus == "" {
			f.CopyStatus = db.CopyStatusSuccessful
			if f.StatusSearchType == "" {
				f.StatusSearchType = "copy"
			}
		}
	case "not_on_src":
		if f.TraversalStatus == "" {
			f.TraversalStatus = db.StatusNotOnSrc
			if f.StatusSearchType == "" {
				f.StatusSearchType = "traversal"
			}
		}
	}
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
		AfterPath:              strings.TrimSpace(req.AfterPath),
		AfterID:                strings.TrimSpace(req.AfterID),
	}
	if f.AfterPath != "" || f.AfterID != "" {
		offset = 0
	}
	rows, hasMore, err := review.ListFolderChildrenPage(s.db, f, orderBy, limit, offset)
	if err != nil {
		return ListChildrenDiffsResult{}, err
	}
	items := make([]DiffItem, 0, len(rows))
	for i := range rows {
		items = append(items, mergedRowToDiffItem(rows[i]))
	}
	if err := s.enrichDiffItemsWithDisplayPaths(items); err != nil {
		return ListChildrenDiffsResult{}, err
	}
	if err := s.enrichDiffItemsWithFailureLogs(items); err != nil {
		return ListChildrenDiffsResult{}, err
	}
	return ListChildrenDiffsResult{
		Items:   items,
		Total:   nil,
		HasMore: hasMore,
		Limit:   limit,
		Offset:  offset,
	}, nil
}

func (s *migrationStore) searchPathReviewItems(ctx context.Context, req SearchRequest) (SearchResult, error) {
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	offset := req.Offset
	if offset < 0 {
		offset = 0
	}
	orderBy := sanitizeSort(req.SortBy, req.SortDirection)
	f, err := searchRequestToReviewFilter(req)
	if err != nil {
		return SearchResult{}, err
	}
	if !review.ReviewFilterHasSearchPredicate(f) {
		return SearchResult{}, ErrSearchRequiresFilter
	}
	if review.StatusDrivenSearch(f) {
		// Status pages by id; ignore path sort so we do not join nodes before LIMIT.
		orderBy = "id ASC"
		if strings.EqualFold(strings.TrimSpace(req.SortDirection), "desc") {
			orderBy = "id DESC"
		}
	}
	rows, hasMore, err := review.ListMergedReviewDiffsPageCtx(ctx, s.db, f, orderBy, limit, offset)
	if err != nil {
		return SearchResult{}, err
	}
	items := make([]DiffItem, 0, len(rows))
	for i := range rows {
		items = append(items, mergedRowToDiffItem(rows[i]))
	}
	if err := s.enrichDiffItemsWithDisplayPaths(items); err != nil {
		return SearchResult{}, err
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

func (s *migrationStore) getSearchStats(ctx context.Context, req SearchRequest) (DiffsStats, error) {
	f, err := searchRequestToReviewFilter(req)
	if err != nil {
		return DiffsStats{}, err
	}
	if !review.ReviewFilterHasSearchPredicate(f) {
		return DiffsStats{}, ErrSearchRequiresFilter
	}
	stats, err := review.GetMergedReviewStatsCtx(ctx, s.db, f)
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
		Truncated:       stats.Truncated,
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
