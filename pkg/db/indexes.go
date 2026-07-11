// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"database/sql"
	"strings"
)

// EnsureNodeTableIndexes creates stable lookup indexes on the given node table
// (e.g. "src_nodes", "dst_nodes") for path_hash, parent_path_hash, and depth.
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
		{table + "_path_hash_idx", "path_hash"},
		{table + "_parent_path_hash_idx", "parent_path_hash"},
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

// indexNamesOnTableLower returns lowercase index names for table from duckdb_indexes(), or (nil, err) if the catalog query fails.
func indexNamesOnTableLower(conn *sql.DB, table string) (map[string]struct{}, error) {
	ctx := context.Background()
	rows, err := conn.QueryContext(ctx,
		`SELECT index_name FROM duckdb_indexes() WHERE lower(table_name) = lower(?)`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]struct{})
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out[strings.ToLower(name)] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func hasIndexLower(present map[string]struct{}, indexName string) bool {
	_, ok := present[strings.ToLower(indexName)]
	return ok
}

// EnsureNodeTableIndexesIfMissing creates only secondary node indexes that are absent from the catalog.
// Same three indexes as EnsureNodeTableIndexes (join keys + depth). Falls back to EnsureNodeTableIndexes if duckdb_indexes is unavailable.
func EnsureNodeTableIndexesIfMissing(db *DB, table string) error {
	conn, err := db.GetDB()
	if err != nil {
		return err
	}
	present, err := indexNamesOnTableLower(conn, table)
	if err != nil {
		return EnsureNodeTableIndexes(db, table)
	}
	indexes := []struct {
		name   string
		column string
	}{
		{table + "_path_hash_idx", "path_hash"},
		{table + "_parent_path_hash_idx", "parent_path_hash"},
		{table + "_depth_idx", "depth"},
	}
	for _, idx := range indexes {
		if hasIndexLower(present, idx.name) {
			continue
		}
		if _, err := conn.Exec("CREATE INDEX IF NOT EXISTS " + idx.name + " ON " + table + " (" + idx.column + ")"); err != nil {
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
		table + "_path_hash_idx",
		table + "_parent_path_hash_idx",
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

// EnsureStatusEventTableIndexesIfMissing creates only status-event indexes absent from the catalog.
// Falls back to EnsureStatusEventTableIndexes if duckdb_indexes is unavailable.
func EnsureStatusEventTableIndexesIfMissing(db *DB, table string) error {
	conn, err := db.GetDB()
	if err != nil {
		return err
	}
	present, err := indexNamesOnTableLower(conn, table)
	if err != nil {
		return EnsureStatusEventTableIndexes(db, table)
	}
	for _, idx := range []struct {
		name   string
		clause string
	}{
		{table + "_id_idx", "(id)"},
		{table + "_id_event_time_idx", "(id, event_time)"},
	} {
		if hasIndexLower(present, idx.name) {
			continue
		}
		if _, err := conn.Exec("CREATE INDEX IF NOT EXISTS " + idx.name + " ON " + table + " " + idx.clause); err != nil {
			return err
		}
	}
	return nil
}

// EnsureQueueStatsIndexes creates an index on queue_stats for latest-per-(queue_key, phase) queries.
func EnsureQueueStatsIndexes(db *DB) error {
	conn, err := db.GetDB()
	if err != nil {
		return err
	}
	_, err = conn.Exec(`CREATE INDEX IF NOT EXISTS queue_stats_key_phase_time_idx ON queue_stats (queue_key, phase, event_time)`)
	return err
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
