// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/filter"
)

// StampDisplayPath records the immutable discovery-time display path used by
// post-traversal SQL filter predicates.
func StampDisplayPath(parentDisplayPath string, parentDepth int, state *db.NodeState) {
	if state == nil {
		return
	}
	state.DisplayPath = filter.DisplayPathForChild(parentDisplayPath, parentDepth, state.Name)
}
