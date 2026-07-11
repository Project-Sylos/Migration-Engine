// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	duckdb "github.com/marcboeker/go-duckdb"
)

const encryptedCatalog = "sylos_main"

func openConnection(opts Options) (*sql.DB, string, error) {
	path := opts.Path
	if path == "" {
		path = ":memory:"
	}
	if len(opts.EncryptionKey) == 0 || path == ":memory:" {
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

	keyStr := base64.StdEncoding.EncodeToString(opts.EncryptionKey)
	escapedPath := strings.ReplaceAll(absPath, "'", "''")
	escapedKey := strings.ReplaceAll(keyStr, "'", "''")
	attachSQL := fmt.Sprintf(
		"ATTACH '%s' AS %s (ENCRYPTION_KEY '%s', TYPE DUCKDB)",
		escapedPath, encryptedCatalog, escapedKey,
	)
	useSQL := fmt.Sprintf("USE %s", encryptedCatalog)

	// go-duckdb shares one DuckDB instance across pooled connections; ATTACH runs once, USE on every conn.
	var attachOnce sync.Once
	var attachErr error

	connector, err := duckdb.NewConnector("", func(execer driver.ExecerContext) error {
		ctx := context.Background()
		if _, err := execer.ExecContext(ctx, "LOAD httpfs", nil); err != nil {
			return err
		}
		attachOnce.Do(func() {
			_, attachErr = execer.ExecContext(ctx, attachSQL, nil)
		})
		if attachErr != nil {
			return fmt.Errorf("attach encrypted database: %w", attachErr)
		}
		if _, err := execer.ExecContext(ctx, useSQL, nil); err != nil {
			return fmt.Errorf("use encrypted catalog: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, "", err
	}

	return sql.OpenDB(connector), absPath, nil
}
