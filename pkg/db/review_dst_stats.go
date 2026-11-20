// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

// ApplyDeleteForestMutation marks a subtree skipped with ancestor poison and sibling promotion (skip=true),
// or restores a skipped subtree with ancestor recovery and sibling demotion (skip=false).
func (db *DB) ApplyDeleteForestMutation(rootPath string, skip bool) (DeleteMutationResult, error) {
	var out DeleteMutationResult
	if db == nil || db.Ops() == nil {
		return out, fmt.Errorf("ops store required")
	}
	rootPath = NormalizeSubtreeRootPathForPropagation(rootPath)
	var mut opsdb.SubtreeMutationResult
	var err error
	if skip {
		mut, err = db.Ops().ApplyDeleteForestSkip(rootPath)
	} else {
		mut, err = db.Ops().ApplyDeleteForestUnskip(rootPath)
	}
	if err != nil {
		return out, err
	}
	return deleteMutationFromOps(mut), nil
}
