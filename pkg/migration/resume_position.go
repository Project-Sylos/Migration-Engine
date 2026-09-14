// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

func minPendingTraversalFolderDepth(database *db.DB, side string) int {
	if database == nil {
		return 0
	}
	maxD, err := stats.GetMaxDepth(database, side)
	if err != nil || maxD < 0 {
		return 0
	}
	start := 0
	if maxD > 0 {
		start = 1
	}
	for d := start; d <= maxD; d++ {
		batch, err := pull.ListNodesPendingAtDepthKeyset(database, side, d, "", 1, db.NodeTypeFolder)
		if err == nil && len(batch) > 0 {
			return d
		}
	}
	return 0
}

func resumeRoundForSide(database *db.DB, side string, savedRound int) int {
	if database == nil {
		return savedRound
	}
	// Prefer an explicit saved round only when it still has pending work and is not
	// the stale depth-0 case (depth 0 pending is ignored when deeper rounds exist).
	if savedRound > 0 && firstPendingTraversalFolderID(database, side, savedRound) != "" {
		return savedRound
	}
	jumped := minPendingTraversalFolderDepth(database, side)
	if jumped > 0 {
		return jumped
	}
	if firstPendingTraversalFolderID(database, side, savedRound) != "" {
		return savedRound
	}
	if savedRound > 0 {
		return savedRound
	}
	return 0
}

func firstPendingTraversalFolderID(database *db.DB, side string, depth int) string {
	if database == nil {
		return ""
	}
	batch, err := pull.ListNodesPendingAtDepthKeyset(database, side, depth, "", 1, db.NodeTypeFolder)
	if err != nil || len(batch) == 0 {
		return ""
	}
	return batch[0].Key
}

func keysetCursorBeforeFirstPendingFolder(database *db.DB, side string, depth int) string {
	first := firstPendingTraversalFolderID(database, side, depth)
	if first == "" || database == nil {
		return ""
	}
	if database.Ops() != nil {
		ids, err := listFolderIDsAtDepthOps(database, side, depth)
		if err != nil {
			return ""
		}
		pred := ""
		for _, id := range ids {
			if id < first && id > pred {
				pred = id
			}
		}
		return pred
	}
	return ""
}

// listFolderIDsAtDepthOps returns folder node ids at depth from Badger (path index + hydrate).
func listFolderIDsAtDepthOps(database *db.DB, side string, depth int) ([]string, error) {
	opsSide := opsdb.SideSRC
	if side == "DST" {
		opsSide = opsdb.SideDST
	}
	ids, err := database.Ops().ListSubtreeIDs(opsSide, "/", 0)
	if err != nil {
		return nil, err
	}
	nodes, err := database.Ops().BatchGetNode(opsSide, ids)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(nodes))
	for id, n := range nodes {
		if n.Depth != depth {
			continue
		}
		if n.Type != db.NodeTypeFolder && n.Type != opsdb.NodeTypeFolder {
			continue
		}
		out = append(out, id)
	}
	return out, nil
}

func restoreOrReconstructKeysetCursor(database *db.DB, side string, depth int, saved string) string {
	if saved != "" {
		return saved
	}
	if database != nil && database.Ops() != nil {
		srcR, srcC, dstR, dstC, ok := database.LoadTraversalQueuePositions()
		if ok {
			if side == "DST" && dstR == depth && dstC != "" {
				return dstC
			}
			if side != "DST" && srcR == depth && srcC != "" {
				return srcC
			}
		}
	}
	return keysetCursorBeforeFirstPendingFolder(database, side, depth)
}

func applyTraversalResume(database *db.DB, resume *RuntimeSuspendV1, srcQueue, dstQueue *queue.Queue, coordinator *queue.QueueCoordinator) {
	if resume == nil || srcQueue == nil || dstQueue == nil {
		return
	}
	srcQueue.SetMode(queue.QueueModeTraversal)
	dstQueue.SetMode(queue.QueueModeTraversal)
	srcRound := resumeRoundForSide(database, "SRC", resume.LastRoundSrc)
	dstRound := resumeRoundForSide(database, "DST", resume.LastRoundDst)
	if database != nil && database.Ops() != nil {
		if sr, _, dr, _, ok := database.LoadTraversalQueuePositions(); ok {
			if resume.LastRoundSrc == 0 && sr > 0 {
				srcRound = resumeRoundForSide(database, "SRC", sr)
			}
			if resume.LastRoundDst == 0 && dr > 0 {
				dstRound = resumeRoundForSide(database, "DST", dr)
			}
		}
	}
	srcQueue.SetRound(srcRound)
	dstQueue.SetRound(dstRound)
	if cur := restoreOrReconstructKeysetCursor(database, "SRC", srcRound, resume.SrcKeysetCursor); cur != "" {
		srcQueue.SetKeysetCursor(cur)
	}
	if cur := restoreOrReconstructKeysetCursor(database, "DST", dstRound, resume.DstKeysetCursor); cur != "" {
		dstQueue.SetKeysetCursor(cur)
	}
	if coordinator != nil {
		coordinator.UpdateRound("src", srcRound)
		coordinator.UpdateRound("dst", dstRound)
	}
	srcQueue.SetExpectedFromStatsBucket(srcRound)
	dstQueue.SetExpectedFromStatsBucket(dstRound)
	srcQueue.SetTraversalCacheLoaded(true)
	dstQueue.SetTraversalCacheLoaded(true)
}
