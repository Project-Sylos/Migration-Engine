// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"database/sql"
	"strings"
)

// Secondary indexes are expensive on multi-million-row tables (each CREATE INDEX is a
// full scan + build). Keep the set minimal and phase-critical only:
//
//   nodes:     parent_id (children / id lookups), depth (BFS keyset pulls)
//   events:    (id, event_time) for arg_max / latest-status IN queries
//
// Deliberately omitted: path (review can scan; prefix LIKE rarely uses btree well),
// standalone events id (covered by the composite).

// bulkPhaseNodeTables are SRC/DST node tables indexed during bulk phases.
var bulkPhaseNodeTables = []string{TableSrcNodes, TableDstNodes}

// bulkPhaseStatusEventTables are SRC/DST status event tables indexed during bulk phases.
var bulkPhaseStatusEventTables = []string{TableSrcStatusEvents, TableDstStatusEvents}

// DropBulkPhaseNodeIndexes drops secondary indexes on both node tables.
func DropBulkPhaseNodeIndexes(db *DB) error {
	for _, table := range bulkPhaseNodeTables {
		if err := DropNodeTableIndexes(db, table); err != nil {
			return err
		}
	}
	return nil
}

// DropBulkPhaseStatusEventIndexes drops indexes on both status event tables.
func DropBulkPhaseStatusEventIndexes(db *DB) error {
	for _, table := range bulkPhaseStatusEventTables {
		if err := DropStatusEventTableIndexes(db, table); err != nil {
			return err
		}
	}
	return nil
}

// EnsureBulkPhaseNodeIndexesIfMissing creates only absent secondary node indexes (SRC + DST).
func EnsureBulkPhaseNodeIndexesIfMissing(db *DB) error {
	for _, table := range bulkPhaseNodeTables {
		_ = dropObsoleteNodeIndexes(db, table)
		if err := EnsureNodeTableIndexesIfMissing(db, table); err != nil {
			return err
		}
	}
	return nil
}

// EnsureBulkPhaseStatusEventIndexesIfMissing creates only absent status-event indexes (SRC + DST).
func EnsureBulkPhaseStatusEventIndexesIfMissing(db *DB) error {
	for _, table := range bulkPhaseStatusEventTables {
		_ = dropObsoleteStatusEventIndexes(db, table)
		if err := EnsureStatusEventTableIndexesIfMissing(db, table); err != nil {
			return err
		}
	}
	return nil
}

// EnsureNodeTableIndexes creates the minimal secondary indexes on a node table.
func EnsureNodeTableIndexes(db *DB, table string) error {
	conn, err := db.GetDB()
	if err != nil {
		return err
	}
	for _, idx := range nodeSecondaryIndexes(table) {
		if err := createIndex(conn, idx.name, "ON "+table+" ("+idx.column+")"); err != nil {
			return err
		}
	}
	return nil
}

func nodeSecondaryIndexes(table string) []struct{ name, column string } {
	return []struct{ name, column string }{
		{table + "_parent_id_idx", "parent_id"},
		{table + "_depth_idx", "depth"},
	}
}

// obsoleteNodeIndexNames are dropped for cleanup but never recreated.
func obsoleteNodeIndexNames(table string) []string {
	return []string{table + "_path_idx"}
}

func dropObsoleteNodeIndexes(db *DB, table string) error {
	conn, err := db.GetDB()
	if err != nil {
		return err
	}
	for _, name := range obsoleteNodeIndexNames(table) {
		if _, err := conn.Exec("DROP INDEX IF EXISTS " + name); err != nil {
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

func createIndex(conn *sql.DB, name, onClause string) error {
	_, err := conn.Exec("CREATE INDEX IF NOT EXISTS " + name + " " + onClause)
	return err
}

// EnsureNodeTableIndexesIfMissing creates only secondary node indexes that are absent from the catalog.
// Falls back to EnsureNodeTableIndexes if duckdb_indexes is unavailable.
func EnsureNodeTableIndexesIfMissing(db *DB, table string) error {
	conn, err := db.GetDB()
	if err != nil {
		return err
	}
	present, err := indexNamesOnTableLower(conn, table)
	if err != nil {
		return EnsureNodeTableIndexes(db, table)
	}
	for _, idx := range nodeSecondaryIndexes(table) {
		if hasIndexLower(present, idx.name) {
			continue
		}
		if err := createIndex(conn, idx.name, "ON "+table+" ("+idx.column+")"); err != nil {
			return err
		}
	}
	return nil
}

// DropNodeTableIndexes drops secondary indexes on the given node table (including obsolete ones).
// Call before a bulk phase to avoid index maintenance cost during inserts.
func DropNodeTableIndexes(db *DB, table string) error {
	conn, err := db.GetDB()
	if err != nil {
		return err
	}
	names := make([]string, 0, 4)
	for _, idx := range nodeSecondaryIndexes(table) {
		names = append(names, idx.name)
	}
	names = append(names, obsoleteNodeIndexNames(table)...)
	for _, name := range names {
		if _, err := conn.Exec("DROP INDEX IF EXISTS " + name); err != nil {
			return err
		}
	}
	return nil
}

func statusEventSecondaryIndexes(table string) []struct{ name, clause string } {
	return []struct{ name, clause string }{
		// Composite covers id equality and latest-by-time aggregation; no separate id-only index.
		{table + "_id_event_time_idx", "(id, event_time)"},
	}
}

func obsoleteStatusEventIndexNames(table string) []string {
	return []string{table + "_id_idx"}
}

func dropObsoleteStatusEventIndexes(db *DB, table string) error {
	conn, err := db.GetDB()
	if err != nil {
		return err
	}
	for _, name := range obsoleteStatusEventIndexNames(table) {
		if _, err := conn.Exec("DROP INDEX IF EXISTS " + name); err != nil {
			return err
		}
	}
	return nil
}

// EnsureStatusEventTableIndexes creates the minimal index on a status event table.
func EnsureStatusEventTableIndexes(db *DB, table string) error {
	conn, err := db.GetDB()
	if err != nil {
		return err
	}
	for _, idx := range statusEventSecondaryIndexes(table) {
		if err := createIndex(conn, idx.name, "ON "+table+" "+idx.clause); err != nil {
			return err
		}
	}
	return nil
}

// EnsureStatusEventTableIndexesIfMissing creates only status-event indexes absent from the catalog.
func EnsureStatusEventTableIndexesIfMissing(db *DB, table string) error {
	conn, err := db.GetDB()
	if err != nil {
		return err
	}
	present, err := indexNamesOnTableLower(conn, table)
	if err != nil {
		return EnsureStatusEventTableIndexes(db, table)
	}
	for _, idx := range statusEventSecondaryIndexes(table) {
		if hasIndexLower(present, idx.name) {
			continue
		}
		if err := createIndex(conn, idx.name, "ON "+table+" "+idx.clause); err != nil {
			return err
		}
	}
	return nil
}

// EnsureQueueStatsIndexes creates an index on queue_stats for latest-per-(queue_key, phase) queries.
// queue_stats stays small; this is cheap relative to node/event indexes.
func EnsureQueueStatsIndexes(db *DB) error {
	conn, err := db.GetDB()
	if err != nil {
		return err
	}
	_, err = conn.Exec(`CREATE INDEX IF NOT EXISTS queue_stats_key_phase_time_idx ON queue_stats (queue_key, phase, event_time)`)
	return err
}

// DropStatusEventTableIndexes drops indexes on the given status event table (including obsolete ones).
func DropStatusEventTableIndexes(db *DB, table string) error {
	conn, err := db.GetDB()
	if err != nil {
		return err
	}
	names := make([]string, 0, 2)
	for _, idx := range statusEventSecondaryIndexes(table) {
		names = append(names, idx.name)
	}
	names = append(names, obsoleteStatusEventIndexNames(table)...)
	for _, name := range names {
		if _, err := conn.Exec("DROP INDEX IF EXISTS " + name); err != nil {
			return err
		}
	}
	return nil
}
