// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

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
