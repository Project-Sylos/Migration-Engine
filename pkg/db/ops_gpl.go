// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"errors"
	"fmt"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/filter"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

// UpdateNodeGPLState writes SRC node gpl_state JSON in Badger.
func (db *DB) UpdateNodeGPLState(nodeID, gplState string) error {
	if db == nil || db.Ops() == nil || nodeID == "" {
		return fmt.Errorf("ops store required")
	}
	n, ok, err := db.Ops().GetNode(opsdb.SideSRC, nodeID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("update gpl_state: node %s not found", nodeID)
	}
	n.GPLState = gplState
	return db.Ops().PutNode(opsdb.SideSRC, n)
}

// PutGPLIssue writes a sparse GPL issue row in Badger.
func (db *DB) PutGPLIssue(srcID, status, proposedName, issuesJSON, dstAction string, updatedAt int64) error {
	if db == nil || db.Ops() == nil || srcID == "" {
		return fmt.Errorf("ops store required")
	}
	return db.Ops().PutGPL(opsdb.GPLRecord{
		SrcID:        srcID,
		Status:       status,
		ProposedName: proposedName,
		IssuesJSON:   issuesJSON,
		UpdatedAt:    updatedAt,
		DstAction:    dstAction,
	})
}

// DeleteGPLIssue removes a sparse GPL issue row.
func (db *DB) DeleteGPLIssue(srcID string) error {
	if db == nil || db.Ops() == nil {
		return fmt.Errorf("ops store required")
	}
	return db.Ops().DeleteGPL(srcID)
}

// DeleteGPLIssuesUnderPath removes sparse GPL rows for SRC descendants of rootPath.
func (db *DB) DeleteGPLIssuesUnderPath(rootPath string, includeRoot bool) error {
	if db == nil || db.Ops() == nil {
		return fmt.Errorf("ops store required")
	}
	rootPath = NormalizeSubtreeRootPathForPropagation(rootPath)
	ids, err := db.Ops().ListSubtreeIDs(opsdb.SideSRC, rootPath, 0)
	if err != nil {
		return err
	}
	if !includeRoot {
		nodes, err := db.Ops().BatchGetNode(opsdb.SideSRC, ids)
		if err != nil {
			return err
		}
		filtered := ids[:0]
		for _, id := range ids {
			if NormalizeSubtreeRootPathForPropagation(nodes[id].Path) == rootPath {
				continue
			}
			filtered = append(filtered, id)
		}
		ids = filtered
	}
	for _, id := range ids {
		if err := db.Ops().DeleteGPL(id); err != nil {
			return err
		}
	}
	return nil
}

// SetSubtreeGPLStatus sets gpl_status on every node under rootPath.
func (db *DB) SetSubtreeGPLStatus(side, rootPath, gplStatus string, includeRoot bool) error {
	if db == nil || db.Ops() == nil || side == "" {
		return fmt.Errorf("ops store required")
	}
	rootPath = NormalizeSubtreeRootPathForPropagation(rootPath)
	ids, err := db.Ops().ListSubtreeIDs(side, rootPath, 0)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	nodes, err := db.Ops().BatchGetNode(side, ids)
	if err != nil {
		return err
	}
	if !includeRoot {
		filtered := ids[:0]
		for _, id := range ids {
			if NormalizeSubtreeRootPathForPropagation(nodes[id].Path) == rootPath {
				continue
			}
			filtered = append(filtered, id)
		}
		ids = filtered
	}
	if len(ids) == 0 {
		return nil
	}
	stMap, err := db.Ops().BatchGetStatus(side, ids)
	if err != nil {
		return err
	}
	puts := make(map[string]opsdb.StatusRecord, len(ids))
	for _, id := range ids {
		st := stMap[id]
		st.GPLStatus = gplStatus
		puts[id] = st
	}
	return db.Ops().BatchPutStatus(side, puts)
}

// NodeRenameHooks are optional progress/abort callbacks for ApplyNodeRenameWithHooks.
type NodeRenameHooks struct {
	Progress func(done, total int64)
	Aborted  func() bool
}

// ErrNodeRenameAborted is returned when NodeRenameHooks.Aborted becomes true mid-cascade.
var ErrNodeRenameAborted = errors.New("node rename aborted")

// ApplyNodeRenameWithHooks updates name/service_id after an FS rename, rewrites DisplayPath
// for the node and descendants (so idx:tri / filter paths stay fresh), patches the
// parent kids pack, and prefix-rewrites LocalFS-style ServiceIDs under the node.
// id_path (Path) is unchanged. hooks supply progress and cooperative abort.
func (db *DB) ApplyNodeRenameWithHooks(side, nodeID, name, oldServiceID, newServiceID string, hooks NodeRenameHooks) error {
	if db == nil || db.Ops() == nil || nodeID == "" {
		return fmt.Errorf("ops store required")
	}
	side = strings.ToLower(strings.TrimSpace(side))
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("rename: empty name")
	}
	n, ok, err := db.Ops().GetNode(side, nodeID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("rename: node %s not found", nodeID)
	}

	oldDisplayPath := strings.TrimSpace(n.DisplayPath)
	if oldDisplayPath == "" {
		oldDisplayPath = filter.RenameDisplayPathLeaf("", n.Name, n.Depth)
	}
	oldSID := n.ServiceID
	if oldServiceID == "" {
		oldServiceID = oldSID
	}
	if newServiceID == "" {
		newServiceID = oldSID
	}
	newDisplayPath := filter.RenameDisplayPathLeaf(oldDisplayPath, name, n.Depth)
	serviceChanged := oldServiceID != "" && newServiceID != "" && oldServiceID != newServiceID

	if n.Name == name && n.DisplayPath == newDisplayPath && !serviceChanged {
		return nil
	}

	n.Name = name
	n.DisplayPath = newDisplayPath
	if newServiceID != "" {
		n.ServiceID = newServiceID
	}
	if err := db.Ops().PutNode(side, n); err != nil {
		return err
	}

	if n.ParentID != "" {
		if err := db.Ops().PatchKidInPack(side, n.ParentID, nodeID, func(k *opsdb.KidRecord) {
			k.Name = name
			if newServiceID != "" {
				k.ServiceID = newServiceID
			}
		}); err != nil {
			return err
		}
	}

	isFolder := n.Type == opsdb.NodeTypeFolder
	if !isFolder && !serviceChanged {
		if hooks.Progress != nil {
			hooks.Progress(1, 1)
		}
		return nil
	}

	// Total from finalize fold (ChildCount); never live-count the subtree here.
	st, _, _ := db.Ops().GetStatus(side, nodeID)
	total := st.ChildCount
	if total < 0 {
		total = 0
	}
	var done int64
	if hooks.Progress != nil {
		hooks.Progress(done, total)
	}
	const page = 512
	err = db.Ops().ApplySubtreeScan(side, n.Path, opsdb.SubtreeScanOpts{ExcludeRoot: true}, page, func(chunk opsdb.SubtreeChunk) error {
		if hooks.Aborted != nil && hooks.Aborted() {
			return ErrNodeRenameAborted
		}
		for _, id := range chunk.IDs {
			child, ok := chunk.Nodes[id]
			if !ok {
				done++
				continue
			}
			changed := false
			if isFolder && oldDisplayPath != newDisplayPath {
				nextDP := filter.ReplaceDisplayPathPrefix(child.DisplayPath, oldDisplayPath, newDisplayPath)
				if nextDP != child.DisplayPath {
					child.DisplayPath = nextDP
					changed = true
				}
			}
			if serviceChanged {
				if child.ParentID == nodeID {
					if child.ParentServiceID != newServiceID {
						child.ParentServiceID = newServiceID
						changed = true
					}
				} else if strings.HasPrefix(child.ParentServiceID, oldServiceID) {
					child.ParentServiceID = newServiceID + child.ParentServiceID[len(oldServiceID):]
					changed = true
				}
				if strings.HasPrefix(child.ServiceID, oldServiceID) {
					child.ServiceID = newServiceID + child.ServiceID[len(oldServiceID):]
					changed = true
				}
			}
			if changed {
				if err := db.Ops().PutNode(side, child); err != nil {
					return err
				}
			}
			done++
			if hooks.Progress != nil && done%16 == 0 {
				hooks.Progress(done, total)
			}
		}
		if hooks.Progress != nil {
			hooks.Progress(done, total)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if hooks.Progress != nil {
		if total > 0 {
			hooks.Progress(total, total)
		} else {
			hooks.Progress(done, done)
		}
	}
	return nil
}

// ClearGPLIssueDstAction clears dst_action on an accepted GPL issue.
func (db *DB) ClearGPLIssueDstAction(srcID string) error {
	if db == nil || db.Ops() == nil || srcID == "" {
		return fmt.Errorf("ops store required")
	}
	rec, ok, err := db.Ops().GetGPL(srcID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	rec.DstAction = ""
	return db.Ops().PutGPL(rec)
}
