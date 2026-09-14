// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"path/filepath"
	"testing"
)

// TestOpen opens a file-backed DB with matching Badger ops dir for tests.
func TestOpen(t *testing.T, name string) *DB {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name+".db")
	ops := filepath.Join(dir, name+".ops")
	database, err := Open(Options{Path: path, OpsDir: ops})
	if err != nil {
		t.Fatalf("TestOpen(%q): %v", name, err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}
