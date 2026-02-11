// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
)

// BootstrapRootStats writes (depth=0, key=traversal/pending, count=1) for SRC and DST so the queue sees pending work at round 0 after roots are seeded. Call once after inserting root nodes.
func BootstrapRootStats(d *DB) error {
	return d.RunUpdateWriterTx(func(w *Writer) error {
		if err := w.SetStatsCountForDepth("SRC", 0, StatsKeyTraversalStatus(StatusPending), 1); err != nil {
			return err
		}
		return w.SetStatsCountForDepth("DST", 0, StatsKeyTraversalStatus(StatusPending), 1)
	})
}

// InsertRootNode inserts the root node (path "/", depth 0) into the given table.
// Uses RunUpdateWriterTx with a direct single-row INSERT (not appender) for immediate turnaround.
func InsertRootNode(d *DB, table string, state *NodeState) error {
	if state == nil {
		return nil
	}
	t := tableName(table)
	return d.RunUpdateWriterTx(func(w *Writer) error {
		traversalStatus := state.TraversalStatus
		if traversalStatus == "" {
			traversalStatus = state.Status
		}
		_, err := w.tx.ExecContext(context.Background(),
			`INSERT INTO `+t+` (id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, traversal_status, copy_status, excluded, errors)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
			state.ID, state.ServiceID, state.ParentID, state.ParentServiceID, state.Path, state.ParentPath, state.Type, state.Size, state.MTime, state.Depth, traversalStatus, state.CopyStatus, state.Excluded, state.Errors,
		)
		return err
	})
}

// BatchInsertNodes inserts nodes via the writer. Uses RunUpdateWriterTx.
func BatchInsertNodes(d *DB, ops []InsertOperation) error {
	if len(ops) == 0 {
		return nil
	}
	return d.RunUpdateWriterTx(func(w *Writer) error {
		bo := &BatchInsertOperation{Operations: ops}
		return bo.flush(w)
	})
}
