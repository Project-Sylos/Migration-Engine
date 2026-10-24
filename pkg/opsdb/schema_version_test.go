// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"path/filepath"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
)

func TestSchemaVersionStampedOnFreshOpen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	v, err := s.SchemaVersionValue()
	if err != nil {
		t.Fatal(err)
	}
	if v != SchemaVersion {
		t.Fatalf("got %q want %q", v, SchemaVersion)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	v2, err := s2.SchemaVersionValue()
	if err != nil {
		t.Fatal(err)
	}
	if v2 != SchemaVersion {
		t.Fatalf("reopen got %q want %q", v2, SchemaVersion)
	}
}

func TestSchemaVersionRejectsUnsupported(t *testing.T) {
	dir := t.TempDir()
	bopts := badger.DefaultOptions(filepath.Clean(dir))
	bopts.Logger = nil
	db, err := badger.Open(bopts)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(metaSchemaVersionKey), []byte("99.0"))
	})
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = Open(Options{Dir: dir})
	if err == nil {
		t.Fatal("expected unsupported schema version error")
	}
}
