// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"
	"strings"

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

// UpdateSrcNodeName writes SRC node display name in Badger.
func (db *DB) UpdateSrcNodeName(nodeID, name string) error {
	return db.updateNodeName(opsdb.SideSRC, nodeID, name)
}

func (db *DB) updateNodeName(side, nodeID, name string) error {
	if db == nil || db.Ops() == nil || nodeID == "" {
		return fmt.Errorf("ops store required")
	}
	n, ok, err := db.Ops().GetNode(side, nodeID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("update name: node %s not found", nodeID)
	}
	n.Name = name
	return db.Ops().PutNode(side, n)
}

// ApplyNodeRename updates name/service_id after an FS rename and rewrites child parent_service_id.
func (db *DB) ApplyNodeRename(side, nodeID, name, oldServiceID, newServiceID string) error {
	if db == nil || db.Ops() == nil || nodeID == "" {
		return fmt.Errorf("ops store required")
	}
	n, ok, err := db.Ops().GetNode(side, nodeID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("rename: node %s not found", nodeID)
	}
	n.Name = name
	if newServiceID != "" {
		n.ServiceID = newServiceID
	}
	if err := db.Ops().PutNode(side, n); err != nil {
		return err
	}
	if newServiceID == "" || newServiceID == oldServiceID {
		return nil
	}
	childIDs, err := db.Ops().ListChildren(side, nodeID, "", 10000)
	if err != nil {
		return err
	}
	if len(childIDs) == 0 {
		return nil
	}
	children, err := db.Ops().BatchGetNode(side, childIDs)
	if err != nil {
		return err
	}
	for _, child := range children {
		child.ParentServiceID = newServiceID
		if oldServiceID != "" && strings.HasPrefix(child.ServiceID, oldServiceID) {
			child.ServiceID = newServiceID + child.ServiceID[len(oldServiceID):]
		}
		if err := db.Ops().PutNode(side, child); err != nil {
			return err
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
