// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	fstypes "codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// ApplyDSTRename renames the mapped DST node for an accepted gpl_issues row with dst_action=rename.
func (m *Migration) ApplyDSTRename(srcID string) error {
	if m == nil || m.DB == nil {
		return fmt.Errorf("migration db not open")
	}
	adapter := m.destinationAdapter()
	if adapter == nil {
		return fmt.Errorf("destination adapter not available for rename")
	}
	renamer, ok := fstypes.RenameFrom(adapter)
	if !ok {
		return fstypes.ErrRenameUnsupported
	}

	gplRec, ok, err := m.DB.Ops().GetGPL(srcID)
	if err != nil {
		return err
	}
	if !ok || gplRec.Status != db.GPLIssueStatusAccepted || gplRec.DstAction != db.DstActionRename {
		return fmt.Errorf("load rename work for %s: not found", srcID)
	}
	mapped, mappedOK, err := m.DB.Ops().GetMapBySrc(srcID)
	if err != nil {
		return err
	}
	if !mappedOK || mapped.DstID == "" {
		return fmt.Errorf("load rename work for %s: no destination map", srcID)
	}
	srcNode, err := pull.GetNodeByID(m.DB, "SRC", srcID)
	if err != nil {
		return err
	}
	dstNode, err := pull.GetNodeByID(m.DB, "DST", mapped.DstID)
	if err != nil {
		return err
	}
	if srcNode == nil || dstNode == nil {
		return fmt.Errorf("load rename work for %s: node missing", srcID)
	}
	proposed := db.NormalizeNodeBasename(gplRec.ProposedName)
	if proposed == "" || dstNode.ServiceID == "" {
		return fmt.Errorf("rename work incomplete for %s", srcID)
	}

	ctx := context.Background()
	res, err := renamer.RenameNode(ctx, dstNode.ParentServiceID, dstNode.ServiceID, proposed, srcNode.Type)
	if err != nil {
		return err
	}
	newServiceID := res.ServiceID
	if newServiceID == "" {
		newServiceID = dstNode.ServiceID
	}
	newName := res.DisplayName
	if newName == "" {
		newName = proposed
	}
	if err := m.DB.ApplyNodeRename("dst", dstNode.ID, newName, dstNode.ServiceID, newServiceID); err != nil {
		return err
	}
	return m.DB.ClearGPLIssueDstAction(srcID)
}

// RunDSTRenameSweep applies all pending dst_action=rename rows in depth order (BFS).
func (m *Migration) RunDSTRenameSweep() error {
	if m == nil || m.DB == nil {
		return fmt.Errorf("migration db not open")
	}
	ids, err := m.DB.Ops().ListGPLByStatus(db.GPLIssueStatusAccepted, 0)
	if err != nil {
		return err
	}
	recs, err := m.DB.Ops().BatchGetGPL(ids)
	if err != nil {
		return err
	}
	filtered := ids[:0]
	for _, id := range ids {
		if recs[id].DstAction == db.DstActionRename {
			filtered = append(filtered, id)
		}
	}
	ids = filtered
	for _, id := range ids {
		if err := m.ApplyDSTRename(id); err != nil {
			return err
		}
	}
	return nil
}

func (m *Migration) destinationAdapter() fstypes.FSAdapter {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.lastRunConfig != nil {
		return m.lastRunConfig.Destination.Adapter
	}
	return nil
}
