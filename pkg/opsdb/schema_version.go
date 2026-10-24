// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

const (
	// metaSchemaVersionKey is the permanent Badger key for the ops schema version.
	// Never rename or relocate this key.
	metaSchemaVersionKey = "meta:schema_version"

	// SchemaVersion is the ops layout version written into every store.
	// Stays at 0.1 until post-launch schema bumps begin.
	SchemaVersion = "0.1"
)

// EnsureSchemaVersion stamps meta:schema_version when missing, or rejects unsupported versions.
func (s *Store) EnsureSchemaVersion() error {
	if s == nil || s.db == nil {
		return nil
	}
	var existing string
	var found bool
	err := s.view(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(metaSchemaVersionKey))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			found = true
			existing = string(val)
			return nil
		})
	})
	if err != nil {
		return err
	}
	if !found {
		return s.update(func(txn *badger.Txn) error {
			return txn.Set([]byte(metaSchemaVersionKey), []byte(SchemaVersion))
		})
	}
	if !schemaVersionSupported(existing) {
		return fmt.Errorf("opsdb: unsupported schema version %q (supported: %s)", existing, SchemaVersion)
	}
	return nil
}

func schemaVersionSupported(v string) bool {
	return v == SchemaVersion
}

// SchemaVersionValue returns the stamped schema version, or empty if unset.
func (s *Store) SchemaVersionValue() (string, error) {
	if s == nil || s.db == nil {
		return "", nil
	}
	var out string
	err := s.view(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(metaSchemaVersionKey))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			out = string(val)
			return nil
		})
	})
	return out, err
}
