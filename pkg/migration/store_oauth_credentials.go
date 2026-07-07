// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"fmt"
	"time"
)

func (s *migrationStore) upsertOAuthCredentials(connectionID string, credsJSON []byte) error {
	if s.db == nil {
		return fmt.Errorf("upsertOAuthCredentials requires store db")
	}
	if connectionID == "" {
		return fmt.Errorf("connectionID required")
	}
	if len(credsJSON) == 0 {
		return fmt.Errorf("credsJSON required")
	}
	conn, err := s.db.GetDB()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	_, err = conn.ExecContext(context.Background(),
		`INSERT INTO oauth_credentials (connection_id, creds_json, updated_at)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (connection_id) DO UPDATE SET
		 creds_json = excluded.creds_json,
		 updated_at = excluded.updated_at`,
		connectionID, string(credsJSON), now,
	)
	if err != nil {
		return fmt.Errorf("upsert oauth_credentials: %w", err)
	}
	return nil
}

func (s *migrationStore) getOAuthCredentials(connectionID string) ([]byte, error) {
	if s.db == nil {
		return nil, fmt.Errorf("getOAuthCredentials requires store db")
	}
	conn, err := s.db.GetDB()
	if err != nil {
		return nil, err
	}
	var credsJSON string
	err = conn.QueryRowContext(context.Background(),
		`SELECT creds_json FROM oauth_credentials WHERE connection_id = $1`,
		connectionID,
	).Scan(&credsJSON)
	if err != nil {
		return nil, err
	}
	return []byte(credsJSON), nil
}

func (s *migrationStore) deleteOAuthCredentials(connectionID string) error {
	if s.db == nil {
		return fmt.Errorf("deleteOAuthCredentials requires store db")
	}
	conn, err := s.db.GetDB()
	if err != nil {
		return err
	}
	_, err = conn.ExecContext(context.Background(),
		`DELETE FROM oauth_credentials WHERE connection_id = $1`,
		connectionID,
	)
	return err
}
