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

// ApplySRCRename renames the SRC node on the filesystem and updates name/service_id in DB.
// id_path ancestry keys are unchanged; GPL fan-out is handled by acceptPathChange.
func (m *Migration) ApplySRCRename(srcID, proposedName string) error {
	if m == nil || m.DB == nil {
		return fmt.Errorf("migration db not open")
	}
	proposedName = db.NormalizeNodeBasename(proposedName)
	if proposedName == "" {
		return fmt.Errorf("proposed name is empty")
	}
	adapter := m.sourceAdapter()
	if adapter == nil {
		return fmt.Errorf("source adapter not available for rename")
	}
	renamer, ok := fstypes.RenameFrom(adapter)
	if !ok {
		return fstypes.ErrRenameUnsupported
	}

	node, err := pull.GetNodeByID(m.DB, "SRC", srcID)
	if err != nil {
		return err
	}
	if node == nil {
		return fmt.Errorf("load src node for rename %s: not found", srcID)
	}
	if node.ServiceID == "" {
		return fmt.Errorf("src service_id empty for %s", srcID)
	}
	if node.Name == proposedName {
		return nil
	}

	ctx := context.Background()
	res, err := renamer.RenameNode(ctx, node.ParentServiceID, node.ServiceID, proposedName, node.Type)
	if err != nil {
		return err
	}
	newServiceID := res.ServiceID
	if newServiceID == "" {
		newServiceID = node.ServiceID
	}
	newName := res.DisplayName
	if newName == "" {
		newName = proposedName
	}
	return m.DB.ApplyNodeRename("src", srcID, newName, node.ServiceID, newServiceID)
}

func (m *Migration) sourceAdapter() fstypes.FSAdapter {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.lastRunConfig != nil {
		return m.lastRunConfig.Source.Adapter
	}
	return nil
}
