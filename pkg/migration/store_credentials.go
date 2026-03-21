// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

// Envelope key size matches Sylos-FS pkg/credentials.KeySize (AES-256).
const envelopeMasterKeySize = 32

// FS credential roles (match roots.SetRootRequest.Role).
const (
	FSCredentialRoleSource      = "source"
	FSCredentialRoleDestination = "destination"
)

// FSCredentialBinding is one row from fs_credential_binding.
type FSCredentialBinding struct {
	Role             string
	ConnectionID     string
	CredsConfRelPath string
	ServiceID        string
	RootFolderJSON   string
}

func (s *migrationStore) ensureEnvelopeMasterKey() ([]byte, error) {
	if s.db == nil {
		return nil, fmt.Errorf("ensureEnvelopeMasterKey requires store db")
	}
	conn, err := s.db.GetDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	var key []byte
	err = tx.QueryRowContext(ctx,
		`SELECT envelope_master_key FROM `+db.TableMigrationEnvelope+` WHERE singleton = 1`,
	).Scan(&key)
	if err == nil && len(key) == envelopeMasterKeySize {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return key, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("read envelope key: %w", err)
	}

	key = make([]byte, envelopeMasterKeySize)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, fmt.Errorf("generate envelope key: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO `+db.TableMigrationEnvelope+` (singleton, envelope_master_key) VALUES (1, ?)`,
		key,
	); err != nil {
		return nil, fmt.Errorf("insert envelope key: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return key, nil
}

func (s *migrationStore) getEnvelopeMasterKey() ([]byte, error) {
	if s.db == nil {
		return nil, fmt.Errorf("getEnvelopeMasterKey requires store db")
	}
	conn, err := s.db.GetDB()
	if err != nil {
		return nil, err
	}
	var key []byte
	err = conn.QueryRowContext(context.Background(),
		`SELECT envelope_master_key FROM `+db.TableMigrationEnvelope+` WHERE singleton = 1`,
	).Scan(&key)
	if err != nil {
		return nil, err
	}
	if len(key) != envelopeMasterKeySize {
		return nil, fmt.Errorf("invalid envelope key length %d", len(key))
	}
	return key, nil
}

func (s *migrationStore) upsertFSCredentialBinding(binding FSCredentialBinding) error {
	if s.db == nil {
		return fmt.Errorf("upsertFSCredentialBinding requires store db")
	}
	if binding.Role != FSCredentialRoleSource && binding.Role != FSCredentialRoleDestination {
		return fmt.Errorf("invalid credential role %q", binding.Role)
	}
	conn, err := s.db.GetDB()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	_, err = conn.ExecContext(context.Background(),
		`INSERT INTO `+db.TableFSCredentialBinding+` (role, connection_id, creds_conf_relpath, service_id, root_folder_json, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (role) DO UPDATE SET
		 connection_id = excluded.connection_id,
		 creds_conf_relpath = excluded.creds_conf_relpath,
		 service_id = excluded.service_id,
		 root_folder_json = excluded.root_folder_json,
		 updated_at = excluded.updated_at`,
		binding.Role,
		binding.ConnectionID,
		nullStr(binding.CredsConfRelPath),
		nullStr(binding.ServiceID),
		nullStr(binding.RootFolderJSON),
		now,
	)
	if err != nil {
		return fmt.Errorf("upsert fs_credential_binding %s: %w", binding.Role, err)
	}
	return nil
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func (s *migrationStore) getFSCredentialBinding(role string) (*FSCredentialBinding, error) {
	if s.db == nil {
		return nil, fmt.Errorf("getFSCredentialBinding requires store db")
	}
	conn, err := s.db.GetDB()
	if err != nil {
		return nil, err
	}
	var b FSCredentialBinding
	var creds, svc, root sql.NullString
	err = conn.QueryRowContext(context.Background(),
		`SELECT role, connection_id, creds_conf_relpath, service_id, root_folder_json
		 FROM `+db.TableFSCredentialBinding+` WHERE role = $1`,
		role,
	).Scan(&b.Role, &b.ConnectionID, &creds, &svc, &root)
	if err != nil {
		return nil, err
	}
	if creds.Valid {
		b.CredsConfRelPath = creds.String
	}
	if svc.Valid {
		b.ServiceID = svc.String
	}
	if root.Valid {
		b.RootFolderJSON = root.String
	}
	return &b, nil
}

func (s *migrationStore) listFSCredentialBindings() ([]FSCredentialBinding, error) {
	if s.db == nil {
		return nil, fmt.Errorf("listFSCredentialBindings requires store db")
	}
	conn, err := s.db.GetDB()
	if err != nil {
		return nil, err
	}
	rows, err := conn.QueryContext(context.Background(),
		`SELECT role, connection_id, creds_conf_relpath, service_id, root_folder_json
		 FROM `+db.TableFSCredentialBinding+` ORDER BY role`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FSCredentialBinding
	for rows.Next() {
		var b FSCredentialBinding
		var creds, svc, root sql.NullString
		if err := rows.Scan(&b.Role, &b.ConnectionID, &creds, &svc, &root); err != nil {
			return nil, err
		}
		if creds.Valid {
			b.CredsConfRelPath = creds.String
		}
		if svc.Valid {
			b.ServiceID = svc.String
		}
		if root.Valid {
			b.RootFolderJSON = root.String
		}
		out = append(out, b)
	}
	return out, rows.Err()
}
