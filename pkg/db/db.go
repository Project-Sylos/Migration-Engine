// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	_ "github.com/mattn/go-sqlite3"
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
	conn, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	// SQLite WAL mode configuration for concurrent reads + writes
	conn.Exec("PRAGMA journal_mode = WAL;")
	conn.Exec("PRAGMA synchronous = NORMAL;")
	conn.Exec("PRAGMA temp_store = MEMORY;")
	conn.Exec("PRAGMA foreign_keys = ON;")

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

	query := "INSERT INTO " + table + " VALUES " + buildPlaceholderGroup(colCount)

	res, err := tx.ExecContext(db.ctx, query, args...)
	if err != nil {
		_ = tx.Rollback()
		fmt.Printf("[DB ERROR] Failed to insert into %s: %v\nQuery: %s\nArgs: %v\n", table, err, query, args)
		return err
	}

	if n, _ := res.RowsAffected(); n == 0 {
		fmt.Printf("[DB WARN] Insert into %s affected 0 rows (query may have been ignored)\nQuery: %s\nArgs: %v\n", table, query, args)
	}

	if err := tx.Commit(); err != nil {
		fmt.Printf("[DB ERROR] Failed to commit transaction for insert into %s: %v\n", table, err)
		return err
	}

	return nil
}

// BulkWrite inserts multiple rows into a table in batched statements to respect SQLite's
// maximum variable limit. Each inner slice in rows is a full row of column values.
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

	// SQLite has a default maximum of 999 variables per statement. To avoid
	// "too many SQL variables" errors, we cap the number of rows per statement
	// both by a hard row limit and by the variable limit.
	// This obviously slows down pure performance in terms of DB writes...
	// but it's necessary for SQLite to actually write our data successfully.
	const (
		sqliteMaxVariables = 999
		maxRowsPerBatch    = 100
	)

	maxByVars := sqliteMaxVariables / colCount
	if maxByVars <= 0 {
		maxByVars = 1
	}
	batchSize := maxByVars
	if batchSize > maxRowsPerBatch {
		batchSize = maxRowsPerBatch
	}

	// Process rows in batches.
	for start := 0; start < len(rows); start += batchSize {
		end := start + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		chunk := rows[start:end]

		// Begin transaction for this chunk
		tx, err := db.conn.BeginTx(db.ctx, nil)
		if err != nil {
			fmt.Printf("[DB ERROR] Failed to begin transaction: %v\n", err)
			return err
		}

		func() {
			defer func() {
				if p := recover(); p != nil {
					_ = tx.Rollback()
					panic(p)
				}
			}()

			// Build "(?, ?, ?), (?, ?, ?), ..." placeholder string for this chunk
			group := buildPlaceholderGroup(colCount)
			var groups []string
			for range chunk {
				groups = append(groups, group)
			}

			query := fmt.Sprintf("INSERT INTO %s VALUES %s", table, strings.Join(groups, ", "))

			// Flatten [][]any into []any for Exec()
			allArgs := make([]any, 0, len(chunk)*colCount)
			for _, row := range chunk {
				allArgs = append(allArgs, row...)
			}

			res, err := tx.ExecContext(db.ctx, query, allArgs...)
			if err != nil {
				_ = tx.Rollback()
				fmt.Printf("[DB ERROR] Bulk insert into %s failed: %v\nQuery: %s\nArgs len: %d\n", table, err, query, len(allArgs))
				return
			}

			if n, _ := res.RowsAffected(); n == 0 {
				// print out the query and args it was trying to do for debugging purposes
				fmt.Printf("[DB DEBUG] Query: %s\nArgs: %v\n", query, allArgs)
				fmt.Printf("[DB WARN] Bulk insert into %s affected 0 rows (possible duplicates)\n", table)
			}

			if err := tx.Commit(); err != nil {
				fmt.Printf("[DB ERROR] Failed to commit transaction for bulk insert into %s: %v\n", table, err)
			}
		}()
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

// ValidateCoreSchema verifies that the core tables (src_nodes, dst_nodes, logs)
// exist and have the expected columns, in the expected order, matching tables.go.
// This is intentionally strict to avoid resuming on incompatible or modified DBs.
func (db *DB) ValidateCoreSchema() error {
	tables := []TableDef{
		SrcNodesTable{},
		DstNodesTable{},
		LogsTable{},
	}

	for _, def := range tables {
		cols := extractColumnNames(def.Schema())
		if len(cols) == 0 {
			return fmt.Errorf("schema validation: could not extract columns for table %s", def.Name())
		}
		if err := db.validateTableColumns(def.Name(), cols); err != nil {
			return err
		}
	}
	return nil
}

// extractColumnNames parses a table schema definition and returns the ordered
// list of column names. It assumes a simple "name TYPE ..." format per line,
// matching the style used in tables.go, and ignores empty lines and comments.
func extractColumnNames(schema string) []string {
	var cols []string
	lines := strings.Split(schema, "\n")
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		// Trim trailing comma, if present.
		line = strings.TrimSuffix(line, ",")
		// Ignore lines that don't look like column definitions (e.g., constraints),
		// which in our current schemas always start with an identifier.
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		name := parts[0]
		cols = append(cols, name)
	}
	return cols
}

// validateTableColumns introspects a table and ensures its columns match the expected
// names and ordering exactly.
func (db *DB) validateTableColumns(table string, expected []string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	rows, err := db.conn.QueryContext(db.ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return fmt.Errorf("schema validation: failed to inspect table %s: %w", table, err)
	}
	defer rows.Close()

	var actual []string
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return fmt.Errorf("schema validation: failed to read column metadata for table %s: %w", table, err)
		}
		actual = append(actual, name)
	}

	if len(actual) == 0 {
		return fmt.Errorf("schema validation: table %s does not exist or has no columns", table)
	}
	if len(actual) != len(expected) {
		return fmt.Errorf("schema validation: table %s has %d columns, expected %d", table, len(actual), len(expected))
	}
	for i, name := range expected {
		if actual[i] != name {
			return fmt.Errorf("schema validation: table %s column %d is %q, expected %q", table, i, actual[i], name)
		}
	}
	return nil
}
