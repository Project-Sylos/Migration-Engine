// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package pull

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"context"
	"time"
)

// InsertRootNode inserts the root node (path "/", depth 0) as metadata only, then emits initial status event(s).
func InsertRootNode(d *db.DB, table string, state *db.NodeState) error {
	if state == nil {
		return nil
	}
	t := db.TableName(table)
	return d.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			path, parentPath := db.NodeInsertPathFields(state.Path, state.ParentPath, state.Depth)
			name := db.NodeInsertName(state.Name, state.Path)
			if table == "SRC" {
				gplState := state.GPLState
				if gplState == "" {
					gplState = `{"valid":true,"part":{"valid":true},"path":{"valid":true},"parts":[]}`
				}
				_, err := w.Tx().ExecContext(context.Background(),
					`INSERT INTO `+t+` (id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, gpl_state, name)
					 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
					state.ID, state.ServiceID, state.ParentID, state.ParentServiceID, path, parentPath, db.NormalizeQueueNodeType(state.Type), state.Size, state.MTime, state.Depth, gplState, name,
				)
				if err != nil {
					return err
				}
			} else {
				_, err := w.Tx().ExecContext(context.Background(),
					`INSERT INTO `+t+` (id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, name)
					 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
					state.ID, state.ServiceID, state.ParentID, state.ParentServiceID, path, parentPath, db.NormalizeQueueNodeType(state.Type), state.Size, state.MTime, state.Depth, name,
				)
				if err != nil {
					return err
				}
			}
			trav := state.TraversalStatus
			if trav == "" {
				trav = state.Status
			}
			copyStatus := state.CopyStatus
			if table == "SRC" && copyStatus == "" {
				copyStatus = db.CopyStatusAlreadyExisted
			}
			ev := &db.StatusEvent{
				ID:              state.ID,
				TraversalStatus: trav,
				CopyStatus:      copyStatus,
				EventTime:       time.Now().UnixNano(),
				Depth:           0,
			}
			if err := w.InsertStatusEvent(table, ev); err != nil {
				return err
			}
			deltas := []db.ReviewStatsDelta{
				{Key: db.ReviewKeyForStatus("traversal", trav), Delta: 1},
			}
			if table == "SRC" {
				if key := db.ReviewKeyForStatus("copy", copyStatus); key != "" {
					deltas = append(deltas, db.ReviewStatsDelta{Key: key, Delta: 1})
				}
				switch db.NormalizeQueueNodeType(state.Type) {
				case db.NodeTypeFolder:
					deltas = append(deltas, db.ReviewStatsDelta{Key: db.ReviewKeyFolders, Delta: 1})
				case db.NodeTypeFile:
					deltas = append(deltas, db.ReviewStatsDelta{Key: db.ReviewKeyFiles, Delta: 1})
					if state.Size > 0 {
						deltas = append(deltas, db.ReviewStatsDelta{Key: db.ReviewKeySizeSrc, Delta: state.Size})
					}
				}
			} else if db.NormalizeQueueNodeType(state.Type) == db.NodeTypeFile && state.Size > 0 {
				deltas = append(deltas, db.ReviewStatsDelta{Key: db.ReviewKeySizeDst, Delta: state.Size})
			}
			if err := w.ApplyReviewStatsDeltas(deltas); err != nil {
				return err
			}
			return nil
		})
	})
}

// BatchInsertNodes inserts nodes via the writer.
func BatchInsertNodes(d *db.DB, ops []db.InsertOperation) error {
	if len(ops) == 0 {
		return nil
	}
	return d.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			bo := &db.BatchInsertOperation{Operations: ops}
			return bo.Flush(w)
		})
	})
}
