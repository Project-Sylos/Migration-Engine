// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "github.com/marcboeker/go-duckdb"
)

func openConnection(opts Options) (*sql.DB, string, error) {
	path := opts.Path
	if path == "" {
		path = ":memory:"
	}
	if path == ":memory:" {
		conn, err := sql.Open("duckdb", path)
		if err != nil {
			return nil, "", err
		}
		return conn, path, nil
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, "", err
	}
	if err := os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
		return nil, "", fmt.Errorf("create db dir: %w", err)
	}

	conn, err := sql.Open("duckdb", absPath)
	if err != nil {
		return nil, "", err
	}
	return conn, absPath, nil
}
