// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"
)

// Store is the migration persistence layer for one DuckDB file.
type Store = migrationStore

// RunStore executes fn against the migration store.
func (m *Migration) RunStore(fn func(*Store) error) error {
	if m.DB == nil {
		return fmt.Errorf("migration has no database")
	}
	return fn(m.store)
}

// GetFSCredentialBinding returns a binding by role ("source" or "destination"), or an error if missing.
func (m *Migration) GetFSCredentialBinding(role string) (*FSCredentialBinding, error) {
	var binding *FSCredentialBinding
	err := m.RunStore(func(s *Store) error {
		var err error
		binding, err = s.getFSCredentialBinding(role)
		return err
	})
	if err != nil {
		return nil, err
	}
	return binding, nil
}

// ListFSCredentialBindings returns all persisted FS credential bindings for this migration DB.
func (m *Migration) ListFSCredentialBindings() ([]FSCredentialBinding, error) {
	var bindings []FSCredentialBinding
	err := m.RunStore(func(s *Store) error {
		var err error
		bindings, err = s.listFSCredentialBindings()
		return err
	})
	if err != nil {
		return nil, err
	}
	return bindings, nil
}

// GetOAuthCredentials returns stored OAuth credentials JSON for a connection, decrypting when encrypted at rest.
func (m *Migration) GetOAuthCredentials(connectionID string) ([]byte, error) {
	var creds []byte
	err := m.RunStore(func(s *Store) error {
		var err error
		creds, err = s.getOAuthCredentials(connectionID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return creds, nil
}
