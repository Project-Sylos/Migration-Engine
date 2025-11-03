// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"
	"time"
	"sync"
	"strings"
	"context"
	"database/sql"
	_ "github.com/marcboeker/go-duckdb"
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

// NewDB opens a new DB connection and prepares the registry.
func NewDB(dbPath string) (*DB, error) {
	conn, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, err
	}

	// god help you if you ever need more than 1GB of memory for a migration lol
	conn.Exec("PRAGMA threads = 1;")
	conn.Exec("PRAGMA disable_object_cache;")
	conn.Exec("PRAGMA memory_limit = '1GB';")

	// Set the maximum number of open connections to 1
	conn.SetMaxOpenConns(1)
	// Set the maximum number of idle connections to 1
	conn.SetMaxIdleConns(1)


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
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.conn.Close()
}

// Checkpoint saves the database to a file.
func (db *DB) Checkpoint() error {
	_, err := db.Query("PRAGMA wal_checkpoint(FULL);")
	if err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)
	return nil
}

// Query executes a SQL query and returns rows.
func (db *DB) Query(query string, args ...any) (*sql.Rows, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.conn.QueryContext(db.ctx, query, args...)
}

// Write performs a direct INSERT into the specified table.
func (db *DB) Write(table string, args ...any) error {
	db.mu.Lock()
	defer db.mu.Unlock()

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
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(rows) == 0 {
		return nil
	}

	colCount := len(rows[0])
	for i, row := range rows {
		if len(row) != colCount {
			return fmt.Errorf("[DB ERROR] BulkWrite: row %d has %d columns, expected %d", i, len(row), colCount)
		}
	}

	// Begin transaction
	tx, err := db.conn.BeginTx(db.ctx, nil)
	if err != nil {
		fmt.Printf("[DB ERROR] Failed to begin transaction: %v\n", err)
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()

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

	res, err := tx.ExecContext(db.ctx, query, allArgs...)
	if err != nil {
		_ = tx.Rollback()
		fmt.Printf("[DB ERROR] Bulk insert into %s failed: %v\nQuery: %s\nArgs len: %d\n", table, err, query, allArgs)
		return err
	}

	if n, _ := res.RowsAffected(); n == 0 {
		// print out the query and args it was trying to do for debugging purposes
		fmt.Printf("[DB DEBUG] Query: %s\nArgs: %v\n", query, allArgs)
		fmt.Printf("[DB WARN] Bulk insert into %s affected 0 rows (possible duplicates)\n", table)
	}

	if err := tx.Commit(); err != nil {
		fmt.Printf("[DB ERROR] Failed to commit transaction for bulk insert into %s: %v\n", table, err)
		return err
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
	db.mu.Lock()
	defer db.mu.Unlock()

	query := "CREATE TABLE IF NOT EXISTS " + name + " (" + schema + ")"
	_, err := db.conn.ExecContext(db.ctx, query)
	return err
}

// Conn returns the underlying sql.DB connection for advanced usage.
func (db *DB) Conn() *sql.DB {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.conn
}
