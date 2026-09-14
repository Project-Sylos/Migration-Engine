// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMigrationStorageExistsOpsOnly(t *testing.T) {
	dir := t.TempDir()
	id := "migration-test-1"
	migrationDir := filepath.Join(dir, id)
	if err := os.MkdirAll(filepath.Join(migrationDir, id+".ops"), 0755); err != nil {
		t.Fatal(err)
	}
	if !MigrationStorageExists(migrationDir, id) {
		t.Fatal("expected ops-only migration dir to exist")
	}
}

func TestGetMigrationOpsOnly(t *testing.T) {
	dir := t.TempDir()
	id := "migration-test-2"
	migrationDir := filepath.Join(dir, id)
	opsDir := filepath.Join(migrationDir, id+".ops")
	if err := os.MkdirAll(opsDir, 0755); err != nil {
		t.Fatal(err)
	}

	mgr := NewMigrationManager()
	cfg := CreateMigrationConfig{Name: "ops-only", MigrationID: id, MigrationDir: migrationDir}
	created, err := mgr.CreateMigration(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if created == nil {
		t.Fatal("nil migration")
	}

	loaded, err := mgr.GetMigration(id, migrationDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if loaded == nil {
		t.Fatal("GetMigration returned nil for ops-only migration")
	}
	if loaded.ID != id {
		t.Fatalf("id=%q want %q", loaded.ID, id)
	}
}
