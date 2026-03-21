// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

// EnsureNodeTableIndexes creates stable lookup indexes on the given node table
// (e.g. "src_nodes", "dst_nodes") for path and parent_path.
//
// Mutable status columns are intentionally left unindexed to avoid high write
// amplification during traversal/copy status updates.
//
// Idempotent for create/drop operations.
func EnsureNodeTableIndexes(db *DB, table string) error {
	conn, err := db.GetDB()
	if err != nil {
		return err
	}

	indexes := []struct {
		name   string
		column string
	}{
		{table + "_path_idx", "path"},
		{table + "_parent_path_idx", "parent_path"},
		{table + "_depth_idx", "depth"},
	}
	for _, idx := range indexes {
		_, err := conn.Exec("CREATE INDEX IF NOT EXISTS " + idx.name + " ON " + table + " (" + idx.column + ")")
		if err != nil {
			return err
		}
	}
	return nil
}
