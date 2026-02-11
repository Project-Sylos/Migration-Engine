// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

// EnsureNodeTableIndexes creates indexes on the given node table (e.g. "src_nodes", "dst_nodes") for path, parent_path, traversal_status, and copy_status.
// Call only after traversal (and copy) for that queue is complete; each index is O(n) over the table. Idempotent (CREATE INDEX IF NOT EXISTS).
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
		{table + "_traversal_status_idx", "traversal_status"},
		{table + "_copy_status_idx", "copy_status"},
	}
	for _, idx := range indexes {
		_, err := conn.Exec("CREATE INDEX IF NOT EXISTS " + idx.name + " ON " + table + " (" + idx.column + ")")
		if err != nil {
			return err
		}
	}
	return nil
}
