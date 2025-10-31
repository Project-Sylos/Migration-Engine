// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"
	"strings"
	"context"
	"database/sql"
	"sync"
)

// TableDef is implemented by every table definition struct in tables.go.
type TableDef interface {
	Name() string
	Schema() string
}

// DB acts as the root database manager and table registry.
type DB struct {
	conn   *sql.DB
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	tables map[string]TableDef
}

// NewDB opens a new DuckDB connection and prepares the registry.
func NewDB(dbPath string) (*DB, error) {
	conn, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, err
	}
	return &DB{
		conn:   conn,
		ctx:    context.Background(),
		cancel: func() {},
		tables: make(map[string]TableDef),
	}, nil
}

// RegisterTable registers a schema object (e.g. LogsTable{}) and creates the table if needed.
func (db *DB) RegisterTable(def TableDef) error {
	name := def.Name()
	schema := def.Schema()

	if err := db.CreateTable(name, schema); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	db.tables[name] = def
	return nil
}

// Close gracefully closes the DB connection.
func (db *DB) Close() error {
	return db.conn.Close()
}

// Query executes a SQL query and returns rows.
func (db *DB) Query(query string, args ...any) (*sql.Rows, error) {
	return db.conn.QueryContext(db.ctx, query, args...)
}

// Write performs a direct INSERT into the specified table.
func (db *DB) Write(table string, args ...any) error {
	colCount := len(args)
	query := "INSERT INTO " + table + " VALUES " + buildPlaceholderGroup(colCount)
	res, err := db.conn.ExecContext(db.ctx, query, args...)
	if err != nil {
		fmt.Printf("[DB ERROR] Failed to insert into %s: %v\nQuery: %s\nArgs: %v\n", table, err, query, args)
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		fmt.Printf("[DB WARNING] Insert into %s affected 0 rows (query may have been ignored)\nQuery: %s\n", table, query)
	}
	return nil
}

// BulkWrite inserts multiple rows into a table in one statement.
// Each inner slice in rows is a full row of column values.
func (db *DB) BulkWrite(table string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}

	colCount := len(rows[0])
	for i, row := range rows {
		if len(row) != colCount {
			return fmt.Errorf("[DB ERROR] BulkWrite: row %d has %d columns, expected %d", i, len(row), colCount)
		}
	}

	// Build "(?, ?, ?), (?, ?, ?), ..." placeholder string
	group := buildPlaceholderGroup(colCount)
	var groups []string
	for range rows {
		groups = append(groups, group)
	}

	query := fmt.Sprintf("INSERT INTO %s VALUES %s", table, strings.Join(groups, ", "))

	// Flatten [][]any into []any for Exec()
	allArgs := make([]any, 0, len(rows)*colCount)
	for _, row := range rows {
		allArgs = append(allArgs, row...)
	}

	res, err := db.conn.ExecContext(db.ctx, query, allArgs...)
	if err != nil {
		fmt.Printf("[DB ERROR] Bulk insert into %s failed: %v\nQuery: %s\nArgs len: %d\n", table, err, query, len(allArgs))
		return err
	}

	if n, _ := res.RowsAffected(); n > 0 {
	} else {
		// print out the query and args it was trying to do for debugging purposes
		fmt.Printf("[DB DEBUG] Query: %s\nArgs: %v\n", query, allArgs)
		fmt.Printf("[DB WARN] Bulk insert into %s affected 0 rows (possible duplicates)\n", table)
	}
	return nil
}

// buildPlaceholderGroup creates "(?, ?, ?)" etc.
func buildPlaceholderGroup(n int) string {
	if n <= 0 {
		return "()"
	}
	s := "("
	for i := 0; i < n; i++ {
		if i > 0 {
			s += ", "
		}
		s += "?"
	}
	s += ")"
	return s
}

// CreateTable ensures a table exists.
func (db *DB) CreateTable(name, schema string) error {
	query := "CREATE TABLE IF NOT EXISTS " + name + " (" + schema + ")"
	_, err := db.conn.ExecContext(db.ctx, query)
	return err
}

// Conn returns the underlying sql.DB connection for advanced usage.
func (db *DB) Conn() *sql.DB {
	return db.conn
}
