// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"time"
)

// InsertRootNode inserts the root node (path "/", depth 0) as metadata only, then emits initial status event(s).
func InsertRootNode(d *DB, table string, state *NodeState) error {
	if state == nil {
		return nil
	}
	t := tableName(table)
	return d.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			_, err := w.tx.ExecContext(context.Background(),
				`INSERT INTO `+t+` (id, service_id, parent_id, parent_service_id, path, parent_path, path_hash, parent_path_hash, type, size, mtime, depth)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
				state.ID, state.ServiceID, state.ParentID, state.ParentServiceID, state.Path, state.ParentPath, PathHash(state.Path), PathHash(state.ParentPath), state.Type, state.Size, state.MTime, state.Depth,
			)
			if err != nil {
				return err
			}
			trav := state.TraversalStatus
			if trav == "" {
				trav = state.Status
			}
			copyStatus := state.CopyStatus
			if table == "SRC" && copyStatus == "" {
				copyStatus = CopyStatusSuccessful
			}
			ev := &StatusEvent{
				ID:              state.ID,
				TraversalStatus: trav,
				CopyStatus:      copyStatus,
				EventTime:       time.Now().UnixNano(),
				Depth:           0,
			}
			if err := w.InsertStatusEvent(table, ev); err != nil {
				return err
			}
			deltas := []ReviewStatsDelta{
				{Key: reviewKeyForTraversalStatus(trav), Delta: 1},
			}
			if table == "SRC" {
				if key := reviewKeyForCopyStatus(copyStatus); key != "" {
					deltas = append(deltas, ReviewStatsDelta{Key: key, Delta: 1})
				}
			}
			if err := w.ApplyReviewStatsDeltas(deltas); err != nil {
				return err
			}
			return nil
		})
	})
}

// BatchInsertNodes inserts nodes via the writer.
func BatchInsertNodes(d *DB, ops []InsertOperation) error {
	if len(ops) == 0 {
		return nil
	}
	return d.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			bo := &BatchInsertOperation{Operations: ops}
			return bo.flush(w)
		})
	})
}
