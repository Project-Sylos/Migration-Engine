// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-3.0-or-later
 
package db

import (
	"context"
	"database/sql"
)

type DB struct {
	conn   *sql.DB
	ctx    context.Context
	cancel context.CancelFunc
}

func NewDB(dbPath string) (*DB, error) {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, err
	}
	return &DB{
		conn: db,
		ctx:  context.Background(),
		cancel: func() {},
	}, nil
}

func (db *DB) Close() error {
	return db.conn.Close()
}

// Note that the following functions are not queue commands but rather direct operations to the database.

func (db *DB) Query(query string, args ...any) (*sql.Rows, error) {
	return db.conn.QueryContext(db.ctx, query, args...)
}

func (db *DB) Write(query string, args ...any) error {
	_, err := db.conn.ExecContext(db.ctx, query, args...)
	return err
}

func (db *DB) CreateTable(tableName string, schema string) error {
	query := "CREATE TABLE IF NOT EXISTS " + tableName + " (" + schema + ")"
	return db.Write(query)
}

func (db *DB) DropTable(tableName string) error {
	query := "DROP TABLE IF EXISTS " + tableName
	return db.Write(query)
}