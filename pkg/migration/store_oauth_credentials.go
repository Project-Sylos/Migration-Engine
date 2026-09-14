// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/db/oauth"
)

func (s *migrationStore) UpsertOAuthCredentials(connectionID string, credsJSON []byte) error {
	ops := s.ops()
	if ops == nil {
		return fmt.Errorf("UpsertOAuthCredentials requires store db")
	}
	if connectionID == "" {
		return fmt.Errorf("connectionID required")
	}
	if len(credsJSON) == 0 {
		return fmt.Errorf("credsJSON required")
	}
	stored, err := oauth.SealOAuthCredentials(credsJSON, s.tokenKey)
	if err != nil {
		return fmt.Errorf("seal oauth credentials: %w", err)
	}
	if err := ops.PutOAuthCred(connectionID, stored); err != nil {
		return fmt.Errorf("upsert oauth_credentials: %w", err)
	}
	return nil
}

func (s *migrationStore) getOAuthCredentials(connectionID string) ([]byte, error) {
	ops := s.ops()
	if ops == nil {
		return nil, fmt.Errorf("getOAuthCredentials requires store db")
	}
	credsJSON, ok, err := ops.GetOAuthCred(connectionID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("oauth credentials not found")
	}
	plain, err := oauth.OpenOAuthCredentials(credsJSON, s.tokenKey)
	if err != nil {
		return nil, fmt.Errorf("open oauth credentials: %w", err)
	}
	return plain, nil
}

func (s *migrationStore) DeleteOAuthCredentials(connectionID string) error {
	ops := s.ops()
	if ops == nil {
		return fmt.Errorf("DeleteOAuthCredentials requires store db")
	}
	return ops.DeleteOAuthCred(connectionID)
}
