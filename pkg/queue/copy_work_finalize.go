// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
)

// FinalizeCopyWorkAfterTraversalRoundAdvance catches up copy-work progress stats for the round
// that just finished. This is not catalog indexing; Badger secondary indexes ride node insert.
//
// SRC and DST are decoupled:
//   - SRC: append discovery totals for child depth (= newRound) — potential copy work.
//   - DST: append already_existed corrections for child depth (= newRound) — subtract matches.
//
// Net denominator = SUM(all append-only rows). Retry re-seals are idempotent catch-ups.
func (q *Queue) FinalizeCopyWorkAfterTraversalRoundAdvance(newRound int) {
	database := q.Database()
	if database == nil {
		return
	}
	retry := q.GetMode() == QueueModeRetry
	// Children discovered / matched while processing finishedRound live at depth newRound.
	depth := newRound
	if depth < 0 {
		return
	}

	switch q.name {
	case "src":
		reason := db.CopyWorkReasonSrcDiscover
		if retry {
			reason = db.CopyWorkReasonSrcDiscover // same prefix; catch-up is idempotent
		}
		// Also seal depth 0 once SRC leaves round 0 (roots seeded before round work).
		if newRound == 1 {
			if _, err := stats.SealSrcCopyWorkDiscovered(database, 0, reason); err != nil {
				q.logCopyWorkFinalizeErr(err.Error())
			}
		}
		if _, err := stats.SealSrcCopyWorkDiscovered(database, depth, reason); err != nil {
			q.logCopyWorkFinalizeErr(err.Error())
		}
	case "dst":
		reason := db.CopyWorkReasonDstAECorrection
		if newRound == 1 {
			if _, err := stats.SealDstCopyWorkAlreadyExistedCorrection(database, 0, reason); err != nil {
				q.logCopyWorkFinalizeErr(err.Error())
			}
		}
		if _, err := stats.SealDstCopyWorkAlreadyExistedCorrection(database, depth, reason); err != nil {
			q.logCopyWorkFinalizeErr(err.Error())
		}
	}
}

// finalizeCopyWorkOnQueueComplete seals remaining depths when SRC or DST traversal completes.
func (q *Queue) finalizeCopyWorkOnQueueComplete() {
	database := q.Database()
	if database == nil {
		return
	}
	maxDepth, err := stats.GetMaxDepth(database, "SRC")
	if err != nil {
		q.logCopyWorkFinalizeErr(fmt.Sprintf("GetMaxDepth: %v", err))
		return
	}
	switch q.name {
	case "src":
		if err := stats.SealCopyWorkThroughDepth(database, maxDepth, db.CopyWorkReasonSrcDiscoverComplete, stats.SealSrcCopyWorkDiscovered, "src discover seal"); err != nil {
			q.logCopyWorkFinalizeErr(err.Error())
		}
	case "dst":
		if err := stats.SealCopyWorkThroughDepth(database, maxDepth, db.CopyWorkReasonDstAECorrectionComplete, stats.SealDstCopyWorkAlreadyExistedCorrection, "dst ae correction seal"); err != nil {
			q.logCopyWorkFinalizeErr(err.Error())
		}
	}
}

func (q *Queue) logCopyWorkFinalizeErr(msg string) {
	if logservice.LS != nil {
		_ = logservice.LS.Log("warning", "copy work finalize: "+msg, "queue", q.name, q.name)
	} else {
		fmt.Println("copy work finalize:", msg)
	}
}

// FinalizeCopyWorkOnStop seals whatever each side has finished so far (durable mid-run offload).
// Caller must Flush first.
func FinalizeCopyWorkOnStop(database *db.DB, coordinator *QueueCoordinator) error {
	if database == nil {
		return nil
	}
	maxDepth, err := stats.GetMaxDepth(database, "SRC")
	if err != nil {
		return err
	}

	srcDone := coordinator != nil && coordinator.IsCompleted("src")
	dstDone := coordinator != nil && coordinator.IsCompleted("dst")
	srcRound := 0
	dstRound := 0
	if coordinator != nil {
		srcRound = coordinator.GetRound("src")
		dstRound = coordinator.GetRound("dst")
	}

	// SRC: seal discovery through depths finished so far (srcRound after finishing R-1 → depth srcRound).
	srcThrough := srcRound
	if srcDone {
		srcThrough = maxDepth
	}
	if srcThrough > maxDepth {
		srcThrough = maxDepth
	}
	if err := stats.SealCopyWorkThroughDepth(database, srcThrough, db.CopyWorkReasonSrcDiscoverStop, stats.SealSrcCopyWorkDiscovered, "src discover seal"); err != nil {
		return err
	}

	// DST: seal AE corrections through depths finished so far.
	dstThrough := dstRound
	if dstDone {
		dstThrough = maxDepth
	}
	if dstThrough > maxDepth {
		dstThrough = maxDepth
	}
	if err := stats.SealCopyWorkThroughDepth(database, dstThrough, db.CopyWorkReasonDstAECorrectionStop, stats.SealDstCopyWorkAlreadyExistedCorrection, "dst ae correction seal"); err != nil {
		return err
	}
	return nil
}
