// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

// ApplyDeleteForestSkip marks a subtree skipped with ancestor poison and sibling promotion.
func (db *DB) ApplyDeleteForestSkip(rootPath string) (DeleteMutationResult, error) {
	return db.applyDeleteForestMutation(rootPath, true)
}

// ApplyDeleteForestUnskip restores a skipped subtree with ancestor recovery and sibling demotion.
func (db *DB) ApplyDeleteForestUnskip(rootPath string) (DeleteMutationResult, error) {
	return db.applyDeleteForestMutation(rootPath, false)
}

func (db *DB) applyDeleteForestMutation(rootPath string, skip bool) (DeleteMutationResult, error) {
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
