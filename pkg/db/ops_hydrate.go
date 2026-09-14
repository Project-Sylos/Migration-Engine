// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import "codeberg.org/Sylos/Migration-Engine/pkg/opsdb"

// HydrateNodeFromOps applies Badger status onto a node state.
func HydrateNodeFromOps(n *NodeState, st opsdb.StatusRecord) {
	if n == nil {
		return
	}
	applyStatusToNode(n, st)
	switch st.CopyStatus {
	case CopyStatusExcludedExplicit, CopyStatusExcludedInherited:
		n.Excluded = true
	default:
		n.Excluded = false
	}
}
