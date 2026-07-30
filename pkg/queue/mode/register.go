// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package mode

import "codeberg.org/Sylos/Migration-Engine/pkg/queue"

func init() {
	queue.RegisterModeHooks(queue.ModeHooks{
		PullTraversal:     PullTraversalTasks,
		PullRetry:         PullRetryTasks,
		PullGPL:           PullGPLTasks,
		PullCopy:          PullCopyTasks,
		PullDelete:        PullDeleteTasks,
		CompleteTraversal: CompleteTraversalTask,
		CompleteGPL:       CompleteGPLTask,
		CompleteCopy:      CompleteCopyTask,
		CompleteDelete:    CompleteDeleteTask,
		FailTraversal:     FailTraversalTask,
		FailGPL:           FailGPLTask,
		FailCopy:          FailCopyTask,
		FailDelete:        FailDeleteTask,
		CheckTraversal:    CheckTraversalCompletion,
		CheckCopy:         CheckCopyCompletion,
		CheckDelete:       CheckDeleteCompletion,
		AdvanceTraversal:  AdvanceTraversalRound,
		AdvanceCopy:       AdvanceCopyRound,
		AdvanceDelete:     AdvanceDeleteRound,
	})
}
