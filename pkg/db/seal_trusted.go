// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import "codeberg.org/Sylos/Migration-Engine/pkg/opsdb"

func prevStatusFromEvent(e StatusEvent) opsdb.StatusRecord {
	return opsdb.StatusRecord{
		TraversalStatus: e.PrevTraversalStatus,
		CopyStatus:      e.PrevCopyStatus,
		DeleteStatus:    e.PrevDeleteStatus,
		GPLStatus:       e.PrevGPLStatus,
	}
}

func travPendingWasSet(s string) bool {
	return s == "" || s == StatusPending
}

func copyPendingWasSet(s string) bool {
	return CopyStatusIsPending(s)
}

func deletePendingWasSet(s string) bool {
	return DeleteStatusOnFrontier(s)
}
