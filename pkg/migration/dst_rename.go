// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
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

	conn, err := m.DB.GetDB()
	if err != nil {
		return err
	}
	ctx := context.Background()
	var proposed, nodeType, dstID, dstServiceID, dstParentServiceID string
	err = conn.QueryRowContext(ctx, `
SELECT COALESCE(gi.proposed_name,''), COALESCE(n.type,''),
       COALESCE(d.id,''), COALESCE(d.service_id,''), COALESCE(d.parent_service_id,'')
FROM gpl_issues gi
JOIN src_nodes n ON n.id = gi.src_id
JOIN id_map im ON im.src_internal_id = gi.src_id AND im.status = 'active'
JOIN dst_nodes d ON d.id = im.dst_internal_id
WHERE gi.src_id = $1 AND gi.status = $2 AND COALESCE(gi.dst_action,'') = $3
ORDER BY im.event_time DESC
LIMIT 1`, srcID, db.GPLIssueStatusAccepted, db.DstActionRename).
		Scan(&proposed, &nodeType, &dstID, &dstServiceID, &dstParentServiceID)
	if err != nil {
		return fmt.Errorf("load rename work for %s: %w", srcID, err)
	}
	proposed = db.NormalizeNodeBasename(proposed)
	if proposed == "" || dstServiceID == "" {
		return fmt.Errorf("rename work incomplete for %s", srcID)
	}

	res, err := renamer.RenameNode(ctx, dstParentServiceID, dstServiceID, proposed, nodeType)
	if err != nil {
		return err
	}
	newServiceID := res.ServiceID
	if newServiceID == "" {
		newServiceID = dstServiceID
	}
	newName := res.DisplayName
	if newName == "" {
		newName = proposed
	}

	return m.DB.RunWrite(ctx, func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.UpdateDstNodeName(dstID, newName); err != nil {
				return err
			}
			if newServiceID != dstServiceID {
				if err := w.UpdateDstNodeServiceID(dstID, newServiceID); err != nil {
					return err
				}
			}
			return w.ClearGPLIssueDstAction(srcID)
		})
	})
}

// RunDSTRenameSweep applies all pending dst_action=rename rows in depth order (BFS).
func (m *Migration) RunDSTRenameSweep() error {
	if m == nil || m.DB == nil {
		return fmt.Errorf("migration db not open")
	}
	conn, err := m.DB.GetDB()
	if err != nil {
		return err
	}
	rows, err := conn.QueryContext(context.Background(), `
SELECT gi.src_id
FROM gpl_issues gi
JOIN src_nodes n ON n.id = gi.src_id
WHERE gi.status = $1 AND COALESCE(gi.dst_action,'') = $2
ORDER BY n.depth ASC, n.path ASC`, db.GPLIssueStatusAccepted, db.DstActionRename)
	if err != nil {
		return err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return err
	}
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
