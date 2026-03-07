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
	// db is used only in legacy single-DB mode. When nil, callers pass per-migration db to each method.
	db *db.DB
}

func newMigrationStore(database *db.DB) *migrationStore {
	return &migrationStore{db: database}
}

func (s *migrationStore) createMigration(database *db.DB, record migrationRecord) error {
	if database == nil {
		database = s.db
	}
	conn, err := database.GetDB()
	if err != nil {
		return err
	}
	_, err = conn.ExecContext(
		context.Background(),
		`INSERT INTO `+db.TableMigrations+` (
			migration_id,
			name,
			phase,
			created_at,
			updated_at,
			service_metadata_json,
			root_config_json
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (migration_id) DO NOTHING`,
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

func (s *migrationStore) getMigration(database *db.DB, id string) (*migrationRecord, error) {
	if database == nil {
		database = s.db
	}
	conn, err := database.GetDB()
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
		 FROM `+db.TableMigrations+` WHERE migration_id = $1`,
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

func (s *migrationStore) listMigrationsFromDB(database *db.DB) ([]migrationRecord, error) {
	if database == nil {
		database = s.db
	}
	conn, err := database.GetDB()
	if err != nil {
		return nil, err
	}
	rows, err := conn.QueryContext(
		context.Background(),
		`SELECT migration_id, name, phase, created_at, updated_at, service_metadata_json, root_config_json
		 FROM `+db.TableMigrations+` ORDER BY created_at DESC`,
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

func (s *migrationStore) listMigrations(database *db.DB) ([]migrationRecord, error) {
	if database == nil {
		database = s.db
	}
	return s.listMigrationsFromDB(database)
}

func (s *migrationStore) deleteMigration(database *db.DB, id string) error {
	if database == nil {
		database = s.db
	}
	conn, err := database.GetDB()
	if err != nil {
		return err
	}
	_, err = conn.ExecContext(context.Background(), `DELETE FROM `+db.TableMigrations+` WHERE migration_id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete migration %s: %w", id, err)
	}
	return nil
}

func (s *migrationStore) updatePhase(id string, phase Phase) error {
	if s.db == nil {
		return fmt.Errorf("updatePhase requires store db")
	}
	conn, err := s.db.GetDB()
	if err != nil {
		return err
	}
	_, err = conn.ExecContext(
		context.Background(),
		`UPDATE `+db.TableMigrations+` SET phase = $1, updated_at = $2 WHERE migration_id = $3`,
		phase.String(),
		time.Now().UTC(),
		id,
	)
	if err != nil {
		return fmt.Errorf("update migration %s phase: %w", id, err)
	}
	return nil
}

// updateUpdatedAt sets updated_at to now for the migration (e.g. after roots inserted or run ended).
func (s *migrationStore) updateUpdatedAt(id string) error {
	if s.db == nil {
		return fmt.Errorf("updateUpdatedAt requires store db")
	}
	conn, err := s.db.GetDB()
	if err != nil {
		return err
	}
	_, err = conn.ExecContext(
		context.Background(),
		`UPDATE `+db.TableMigrations+` SET updated_at = $1 WHERE migration_id = $2`,
		time.Now().UTC(),
		id,
	)
	if err != nil {
		return fmt.Errorf("update migration %s updated_at: %w", id, err)
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
