// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

// TableDef is implemented by every table definition struct in tables.go.
type TableDef interface {
	Name() string
	Schema() string
}

// Table wraps runtime metadata for each registered table.
type Table struct {
	Def        TableDef
	IsBuffered bool
	Buffer     *InsertBuffer
}

// DB acts as the root database manager and table registry.
type DB struct {
	conn    *sql.DB
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	tables  map[string]*Table
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
		tables: make(map[string]*Table),
	}, nil
}

// RegisterTable registers a schema object (e.g. LogsTable{}) and attaches a buffer if configured.
func (db *DB) RegisterTable(def TableDef, buffered bool, batchSize int, flushInt time.Duration) error {
	name := def.Name()
	schema := def.Schema()

	if err := db.CreateTable(name, schema); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	t := &Table{
		Def:        def,
		IsBuffered: buffered,
	}
	if buffered {
		t.Buffer = NewInsertBuffer(db.conn, name, batchSize, flushInt)
	}
	db.tables[name] = t
	return nil
}

// Close gracefully stops buffers and closes the DB connection.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, t := range db.tables {
		if t.IsBuffered && t.Buffer != nil {
			t.Buffer.Stop()
		}
	}
	return db.conn.Close()
}

func (db *DB) Query(query string, args ...any) (*sql.Rows, error) {
    db.mu.Lock()
    defer db.mu.Unlock()

    // Flush any buffered tables involved in the query
    for _, tbl := range db.tables {
        if tbl.IsBuffered && tbl.Buffer != nil {
            tbl.Buffer.Flush()
        }
    }

    return db.conn.QueryContext(db.ctx, query, args...)
}

// Write routes to a buffer if one exists; otherwise performs direct insert.
func (db *DB) Write(table string, args ...any) error {
	db.mu.Lock()
	t, ok := db.tables[table]
	db.mu.Unlock()

	if ok && t.IsBuffered && t.Buffer != nil {
		t.Buffer.Add(args)
		return nil
	}
	return db.directInsert(table, args)
}

// directInsert performs a simple INSERT for non-buffered tables.
func (db *DB) directInsert(table string, args []any) error {
	colCount := len(args)
	query := "INSERT INTO " + table + " VALUES " + buildPlaceholderGroup(colCount)
	_, err := db.conn.ExecContext(db.ctx, query, args...)
	return err
}

// CreateTable ensures a table exists.
func (db *DB) CreateTable(name, schema string) error {
	query := "CREATE TABLE IF NOT EXISTS " + name + " (" + schema + ")"
	_, err := db.conn.ExecContext(db.ctx, query)
	return err
}
