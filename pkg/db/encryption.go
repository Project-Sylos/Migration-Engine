// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const encryptedCatalog = "sylos_main"

// EncryptionKeyString encodes a 32-byte key for DuckDB ENCRYPTION_KEY.
func EncryptionKeyString(key []byte) string {
	return base64.StdEncoding.EncodeToString(key)
}

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

	conn, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, "", err
	}
	_, _ = conn.Exec("LOAD httpfs")

	keyStr := EncryptionKeyString(opts.EncryptionKey)
	escapedPath := strings.ReplaceAll(absPath, "'", "''")
	escapedKey := strings.ReplaceAll(keyStr, "'", "''")

	attachSQL := fmt.Sprintf(
		"ATTACH '%s' AS %s (ENCRYPTION_KEY '%s', TYPE DUCKDB)",
		escapedPath, encryptedCatalog, escapedKey,
	)
	if _, err := conn.Exec(attachSQL); err != nil {
		_ = conn.Close()
		return nil, "", fmt.Errorf("open encrypted database %s: %w", absPath, err)
	}
	if _, err := conn.Exec(fmt.Sprintf("USE %s", encryptedCatalog)); err != nil {
		_ = conn.Close()
		return nil, "", fmt.Errorf("use encrypted catalog: %w", err)
	}
	return conn, absPath, nil
}
