// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

// CopyMutationResult aggregates live copy exclude/retry writes.
type CopyMutationResult struct {
	Affected     int64
	Folders      int64
	Files        int64
	PendingBytes int64
}

// DeleteMutationResult aggregates live delete forest/retry writes.
type DeleteMutationResult struct {
	Affected      int64
	Folders       int64
	Files         int64
	SelectedBytes int64
}

func copyCompleteForDelete(st opsdb.StatusRecord) bool {
	cs := st.CopyStatus
	return cs == CopyStatusSuccessful || cs == CopyStatusAlreadyExisted
}

// ApplySubtreeCopyExclusion writes live copy exclude/unexclude over a path prefix.
func (db *DB) ApplySubtreeCopyExclusion(rootPath string, excluded bool, skip func(id, path, typ string) bool) (CopyMutationResult, error) {
	var out CopyMutationResult
	if db == nil || db.Ops() == nil {
		return out, fmt.Errorf("ops store required")
	}
	mut, err := db.Ops().ApplySubtreeCopyExclusion(NormalizeSubtreeRootPathForPropagation(rootPath), excluded, skip)
	if err != nil {
		return out, err
	}
	return copyMutationFromOps(mut), nil
}

// ApplyNodeCopyExclusion writes live copy exclude/unexclude for one SRC node.
func (db *DB) ApplyNodeCopyExclusion(nodeID string, excluded bool) (CopyMutationResult, error) {
	var out CopyMutationResult
	if db == nil || db.Ops() == nil || nodeID == "" {
		return out, fmt.Errorf("ops store required")
	}
	mut, err := db.Ops().ApplyNodeCopyExclusion(nodeID, excluded)
	if err != nil {
		return out, err
	}
	return copyMutationFromOps(mut), nil
}

func copyMutationFromOps(mut opsdb.SubtreeMutationResult) CopyMutationResult {
	return CopyMutationResult{
		Affected: mut.Affected, Folders: mut.Folders, Files: mut.Files, PendingBytes: mut.SelectedBytes,
	}
}

func deleteMutationFromOps(mut opsdb.SubtreeMutationResult) DeleteMutationResult {
	return DeleteMutationResult{
		Affected: mut.Affected, Folders: mut.Folders, Files: mut.Files, SelectedBytes: mut.SelectedBytes,
	}
}

func (db *DB) normalizeDeleteForestOps() error {
	ops := db.Ops()
	ids, err := ops.ListSubtreeIDs(opsdb.SideSRC, "/", 0)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	nodes, err := ops.BatchGetNode(opsdb.SideSRC, ids)
	if err != nil {
		return err
	}
	pathByID := make(map[string]string, len(nodes))
	for id, n := range nodes {
		pathByID[id] = n.Path
	}
	stMap, err := ops.BatchGetStatus(opsdb.SideSRC, ids)
	if err != nil {
		return err
	}
	deleteByPath := make(map[string]string)
	for _, id := range ids {
		st := stMap[id]
		if !copyCompleteForDelete(st) || !DeleteStatusIsPending(st.DeleteStatus) {
			continue
		}
		deleteByPath[pathByID[id]] = st.DeleteStatus
	}
	hasPendingAncestor := func(path string) bool {
		path = NormalizeRootRelativePath(path)
		if path == "" || path == "/" {
			return false
		}
		for {
			idx := strings.LastIndex(path, "/")
			if idx <= 0 {
				path = "/"
			} else {
				path = path[:idx]
			}
			if ds, ok := deleteByPath[path]; ok && DeleteStatusIsPending(ds) {
				return true
			}
			if path == "/" {
				break
			}
		}
		return false
	}
	for _, id := range ids {
		st := stMap[id]
		if !copyCompleteForDelete(st) || !DeleteStatusIsPending(st.DeleteStatus) {
			continue
		}
		n := nodes[id]
		p := pathByID[id]
		desired := DeleteStatusPendingExplicit
		if hasPendingAncestor(p) {
			desired = DeleteStatusPendingInherited
		}
		if st.DeleteStatus == desired {
			continue
		}
		prev := st.DeleteStatus
		st.DeleteStatus = desired
		nt := NormalizeQueueNodeType(n.Type)
		deltas := []opsdb.PendingDelta{{
			Phase: opsdb.PhaseDel, NodeType: nt,
			Add:        DeleteStatusOnFrontier(desired),
			PendWasSet: DeleteStatusOnFrontier(prev),
		}}
		sched, err := ops.PutStatusPendingKeys(opsdb.SideSRC, id, st, n.Depth, deltas)
		if err != nil {
			return err
		}
		if err := ops.ApplySchedCountDeltas(sched); err != nil {
			return err
		}
		deleteByPath[p] = desired
	}
	return nil
}

// NormalizeDeleteForestOps rewrites live pending* delete status to explicit/inherited roots
// and retargets pend:del. Used as a one-shot safety net (PrepareSourceCleanup / StartDelete).
func (db *DB) NormalizeDeleteForestOps() error {
	if db == nil || db.Ops() == nil {
		return nil
	}
	return db.normalizeDeleteForestOps()
}

// SeedUnsetDeleteStatuses writes live pending* for copy-complete nodes with empty delete_status,
// then normalizes the forest.
func (db *DB) SeedUnsetDeleteStatuses() (DeleteMutationResult, error) {
	var out DeleteMutationResult
	if db == nil || db.Ops() == nil {
		return out, fmt.Errorf("ops store required")
	}
	mut, err := db.Ops().SeedUnsetDeleteStatuses()
	if err != nil {
		return out, err
	}
	if err := db.NormalizeDeleteForestOps(); err != nil {
		return out, err
	}
	return deleteMutationFromOps(mut), nil
}
