// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

// EnsureNodeTableIndexes creates stable lookup indexes on the given node table
// (e.g. "src_nodes", "dst_nodes") for path and parent_path.
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

// DropNodeTableIndexes drops the non-primary indexes on the given node table (e.g. "src_nodes", "dst_nodes").
// Call before a bulk phase to avoid index maintenance cost during inserts.
func DropNodeTableIndexes(db *DB, table string) error {
	conn, err := db.GetDB()
	if err != nil {
		return err
	}
	for _, name := range []string{
		table + "_path_idx",
		table + "_parent_path_idx",
		table + "_depth_idx",
	} {
		if _, err := conn.Exec("DROP INDEX IF EXISTS " + name); err != nil {
			return err
		}
	}
	return nil
}

// EnsureStatusEventTableIndexes creates indexes on the given status event table
// for id and (id, event_time) to support "latest event per node" queries.
// Idempotent.
func EnsureStatusEventTableIndexes(db *DB, table string) error {
	conn, err := db.GetDB()
	if err != nil {
		return err
	}
	for _, idx := range []struct {
		name   string
		clause string
	}{
		{table + "_id_idx", "(id)"},
		{table + "_id_event_time_idx", "(id, event_time)"},
	} {
		if _, err := conn.Exec("CREATE INDEX IF NOT EXISTS " + idx.name + " ON " + table + " " + idx.clause); err != nil {
			return err
		}
	}
	return nil
}

// DropStatusEventTableIndexes drops indexes on the given status event table. Call before a bulk phase.
func DropStatusEventTableIndexes(db *DB, table string) error {
	conn, err := db.GetDB()
	if err != nil {
		return err
	}
	for _, name := range []string{
		table + "_id_idx",
		table + "_id_event_time_idx",
	} {
		if _, err := conn.Exec("DROP INDEX IF EXISTS " + name); err != nil {
			return err
		}
	}
	return nil
}
