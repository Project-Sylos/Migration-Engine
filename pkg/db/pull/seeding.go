// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package pull

import (
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

// InsertRootNode inserts the root node (path "/", depth 0) as metadata only, then emits initial status event(s).
func InsertRootNode(d *db.DB, table string, state *db.NodeState) error {
	if state == nil {
		return nil
	}
	if d.Ops() != nil {
		return d.SeedRootNode(table, state)
	}
	return fmt.Errorf("ops store required")
}

// BatchInsertNodes inserts nodes via the writer.
func BatchInsertNodes(d *db.DB, ops []db.InsertOperation) error {
	return d.SeedDiscoveredNodes(ops)
}
