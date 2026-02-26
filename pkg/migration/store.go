// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

type migrationRecord struct {
	ID                  string
	Name                string
	Phase               Phase
	CreatedAt           time.Time
	UpdatedAt           time.Time
	ServiceMetadataJSON string
	RootConfigJSON      string
}

type migrationStore struct {
	db *db.DB
}

func newMigrationStore(database *db.DB) *migrationStore {
	return &migrationStore{db: database}
}

func (s *migrationStore) createMigration(record migrationRecord) error {
	conn, err := s.db.GetDB()
	if err != nil {
		return err
	}
	_, err = conn.ExecContext(
		context.Background(),
		`INSERT INTO migrations (
			migration_id,
			name,
			phase,
			created_at,
			updated_at,
			service_metadata_json,
			root_config_json
		) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		record.ID,
		record.Name,
		record.Phase.String(),
		record.CreatedAt,
		record.UpdatedAt,
		record.ServiceMetadataJSON,
		record.RootConfigJSON,
	)
	if err != nil {
		return fmt.Errorf("create migration %s: %w", record.ID, err)
	}
	return nil
}

func (s *migrationStore) getMigration(id string) (*migrationRecord, error) {
	conn, err := s.db.GetDB()
	if err != nil {
		return nil, err
	}
	var (
		record   migrationRecord
		phaseRaw string
	)
	err = conn.QueryRowContext(
		context.Background(),
		`SELECT migration_id, name, phase, created_at, updated_at, service_metadata_json, root_config_json
		 FROM migrations WHERE migration_id = $1`,
		id,
	).Scan(
		&record.ID,
		&record.Name,
		&phaseRaw,
		&record.CreatedAt,
		&record.UpdatedAt,
		&record.ServiceMetadataJSON,
		&record.RootConfigJSON,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get migration %s: %w", id, err)
	}
	record.Phase, err = ParsePhase(phaseRaw)
	if err != nil {
		return nil, fmt.Errorf("parse migration %s phase: %w", id, err)
	}
	return &record, nil
}

func (s *migrationStore) listMigrations() ([]migrationRecord, error) {
	conn, err := s.db.GetDB()
	if err != nil {
		return nil, err
	}
	rows, err := conn.QueryContext(
		context.Background(),
		`SELECT migration_id, name, phase, created_at, updated_at, service_metadata_json, root_config_json
		 FROM migrations ORDER BY created_at DESC`,
	)
	if err != nil {
		return nil, fmt.Errorf("list migrations: %w", err)
	}
	defer rows.Close()

	records := make([]migrationRecord, 0)
	for rows.Next() {
		var (
			record   migrationRecord
			phaseRaw string
		)
		if err := rows.Scan(
			&record.ID,
			&record.Name,
			&phaseRaw,
			&record.CreatedAt,
			&record.UpdatedAt,
			&record.ServiceMetadataJSON,
			&record.RootConfigJSON,
		); err != nil {
			return nil, fmt.Errorf("list migrations scan: %w", err)
		}
		record.Phase, err = ParsePhase(phaseRaw)
		if err != nil {
			return nil, fmt.Errorf("parse phase for migration %s: %w", record.ID, err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list migrations rows: %w", err)
	}
	return records, nil
}

func (s *migrationStore) deleteMigration(id string) error {
	conn, err := s.db.GetDB()
	if err != nil {
		return err
	}
	_, err = conn.ExecContext(context.Background(), `DELETE FROM migrations WHERE migration_id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete migration %s: %w", id, err)
	}
	return nil
}

func (s *migrationStore) updatePhase(id string, phase Phase) error {
	conn, err := s.db.GetDB()
	if err != nil {
		return err
	}
	_, err = conn.ExecContext(
		context.Background(),
		`UPDATE migrations SET phase = $1, updated_at = $2 WHERE migration_id = $3`,
		phase.String(),
		time.Now().UTC(),
		id,
	)
	if err != nil {
		return fmt.Errorf("update migration %s phase: %w", id, err)
	}
	return nil
}

func marshalJSONOrEmpty(value any) string {
	if value == nil {
		return "{}"
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(raw)
}
