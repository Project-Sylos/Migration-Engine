// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
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
	_, err := db.conn.ExecContext(db.ctx, query, args...)
	return err
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
