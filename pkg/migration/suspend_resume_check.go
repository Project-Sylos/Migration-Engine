// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

func logTraversalResumePositionCheck(resume *RuntimeSuspendV1, srcQueue, dstQueue *queue.Queue) {
	if resume == nil || srcQueue == nil || dstQueue == nil {
		return
	}
	if srcQueue.GetMode() != queue.QueueModeRetry || dstQueue.GetMode() != queue.QueueModeRetry {
		logResumeCheck("warning", fmt.Sprintf(
			"traversal resume: expected retry mode (src=%s dst=%s)",
			srcQueue.GetMode(), dstQueue.GetMode(),
		))
		return
	}
	srcRound := srcQueue.Stats().Round
	dstRound := dstQueue.Stats().Round
	if resume.LastRoundSrc > 0 && srcRound != resume.LastRoundSrc {
		logResumeCheck("info", fmt.Sprintf(
			"traversal resume: restarting at src round %d (suspended at %d) in retry mode",
			srcRound, resume.LastRoundSrc,
		))
	}
	if resume.LastRoundDst > 0 && dstRound != resume.LastRoundDst {
		logResumeCheck("info", fmt.Sprintf(
			"traversal resume: restarting at dst round %d (suspended at %d) in retry mode",
			dstRound, resume.LastRoundDst,
		))
	}
}

func logCopyResumePositionCheck(resume *RuntimeSuspendV1, copyQueue *queue.Queue, startRound int) {
	if resume == nil || copyQueue == nil {
		return
	}
	if copyQueue.GetMode() != queue.QueueModeCopy {
		logResumeCheck("warning", fmt.Sprintf(
			"copy resume: expected copy mode, got %s", copyQueue.GetMode(),
		))
		return
	}
	if resume.LastKnownCopyRound > 0 && startRound != resume.LastKnownCopyRound {
		logResumeCheck("info", fmt.Sprintf(
			"copy resume: derived start round %d (suspended at %d, copy pass %d)",
			startRound, resume.LastKnownCopyRound, resume.CopyPass,
		))
	}
}

func logResumeCheck(level, message string) {
	if logservice.LS == nil {
		return
	}
	_ = logservice.LS.Log(level, message, "migration", "resume-check", "")
}
