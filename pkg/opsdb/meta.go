// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

// MigrationMetaRecord holds per-migration row formerly in Duck migrations table.
type MigrationMetaRecord struct {
	MigrationID         string `json:"migration_id"`
	Name                string `json:"name,omitempty"`
	Phase               string `json:"phase,omitempty"`
	CreatedAt           int64  `json:"created_at,omitempty"`
	UpdatedAt           int64  `json:"updated_at,omitempty"`
	ServiceMetadataJSON string `json:"service_metadata_json,omitempty"`
	RootConfigJSON      string `json:"root_config_json,omitempty"`
	RuntimeStateJSON    string `json:"runtime_state_json,omitempty"`
}

func migrationMetaKey(id string) []byte {
	return []byte("mig:" + id)
}

func oauthCredKey(connectionID string) []byte {
	return []byte("oauth:" + connectionID)
}

func fsBindingKey(side string) []byte {
	return []byte("fsbind:" + side)
}

// PutMigrationMeta upserts migration metadata.
func (s *Store) PutMigrationMeta(rec MigrationMetaRecord) error {
	if s == nil || rec.MigrationID == "" {
		return fmt.Errorf("opsdb PutMigrationMeta: empty id")
	}
	b, err := encode(rec)
	if err != nil {
		return err
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(migrationMetaKey(rec.MigrationID), b)
	})
}

// GetMigrationMeta loads migration metadata.
func (s *Store) GetMigrationMeta(id string) (MigrationMetaRecord, bool, error) {
	var rec MigrationMetaRecord
	if s == nil || id == "" {
		return rec, false, nil
	}
	err := s.view(func(txn *badger.Txn) error {
		item, err := txn.Get(migrationMetaKey(id))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return decodeInto(val, &rec)
		})
	})
	return rec, rec.MigrationID != "", err
}

// ListMigrationMeta returns all migration meta records.
func (s *Store) ListMigrationMeta() ([]MigrationMetaRecord, error) {
	var out []MigrationMetaRecord
	if s == nil {
		return out, nil
	}
	prefix := []byte("mig:")
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			var rec MigrationMetaRecord
			if err := it.Item().Value(func(val []byte) error {
				return decodeInto(val, &rec)
			}); err != nil {
				return err
			}
			out = append(out, rec)
		}
		return nil
	})
	return out, err
}

// PutOAuthCred stores oauth credential payload by connection id.
func (s *Store) PutOAuthCred(connectionID, payload string) error {
	if s == nil || connectionID == "" {
		return nil
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(oauthCredKey(connectionID), []byte(payload))
	})
}

// GetOAuthCred loads oauth credential payload.
func (s *Store) GetOAuthCred(connectionID string) (string, bool, error) {
	if s == nil || connectionID == "" {
		return "", false, nil
	}
	var payload string
	err := s.view(func(txn *badger.Txn) error {
		item, err := txn.Get(oauthCredKey(connectionID))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			payload = string(val)
			return nil
		})
	})
	return payload, payload != "", err
}

// DeleteOAuthCred removes oauth credential payload.
func (s *Store) DeleteOAuthCred(connectionID string) error {
	if s == nil || connectionID == "" {
		return nil
	}
	return s.update(func(txn *badger.Txn) error {
		err := txn.Delete(oauthCredKey(connectionID))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	})
}

// FSBindingRecord is SRC/DST filesystem binding metadata.
type FSBindingRecord struct {
	Side           string `json:"side"`
	ConnectionID   string `json:"connection_id,omitempty"`
	CredsPath      string `json:"creds_path,omitempty"`
	ServiceID      string `json:"service_id,omitempty"`
	RootFolderJSON string `json:"root_folder_json,omitempty"`
}

// PutFSBinding stores one side's FS binding.
func (s *Store) PutFSBinding(rec FSBindingRecord) error {
	if s == nil || rec.Side == "" {
		return nil
	}
	b, err := encode(rec)
	if err != nil {
		return err
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(fsBindingKey(rec.Side), b)
	})
}

// GetFSBinding loads one side's FS binding.
func (s *Store) GetFSBinding(side string) (FSBindingRecord, bool, error) {
	var rec FSBindingRecord
	if s == nil || side == "" {
		return rec, false, nil
	}
	err := s.view(func(txn *badger.Txn) error {
		item, err := txn.Get(fsBindingKey(side))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return decodeInto(val, &rec)
		})
	})
	return rec, rec.Side != "", err
}

// DeleteMigrationMeta removes migration metadata.
func (s *Store) DeleteMigrationMeta(id string) error {
	if s == nil || id == "" {
		return nil
	}
	return s.update(func(txn *badger.Txn) error {
		err := txn.Delete(migrationMetaKey(id))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	})
}

// ListFSBindings returns both FS bindings if present.
func (s *Store) ListFSBindings() ([]FSBindingRecord, error) {
	var out []FSBindingRecord
	for _, side := range []string{"source", "destination", SideSRC, SideDST} {
		rec, ok, err := s.GetFSBinding(side)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, rec)
		}
	}
	return out, nil
}
