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
	Phase               string
	CreatedAt           time.Time
	UpdatedAt           time.Time
	ServiceMetadataJSON string
	RootConfigJSON      string
	RuntimeStateJSON    string
}

type migrationStore struct {
	db *db.DB // set on Migration-bound stores only
}

func newMigrationStore(database *db.DB) *migrationStore {
	return &migrationStore{db: database}
}

func (s *migrationStore) createMigration(database *db.DB, record migrationRecord) error {
	if database == nil {
		return fmt.Errorf("database required")
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
			root_config_json,
			runtime_state_json
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (migration_id) DO NOTHING`,
		record.ID,
		record.Name,
		record.Phase,
		record.CreatedAt,
		record.UpdatedAt,
		record.ServiceMetadataJSON,
		record.RootConfigJSON,
		record.RuntimeStateJSON,
	)
	if err != nil {
		return fmt.Errorf("create migration %s: %w", record.ID, err)
	}
	return nil
}

func (s *migrationStore) getMigration(database *db.DB, id string) (*migrationRecord, error) {
	if database == nil {
		return nil, fmt.Errorf("database required")
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
		`SELECT migration_id, name, phase, created_at, updated_at, service_metadata_json, root_config_json, COALESCE(runtime_state_json,'')
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
		&record.RuntimeStateJSON,
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

func (s *migrationStore) deleteMigration(database *db.DB, id string) error {
	if database == nil {
		return fmt.Errorf("database required")
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

func (s *migrationStore) withConn(op string, fn func(*sql.DB) error) error {
	if s.db == nil {
		return fmt.Errorf("%s requires store db", op)
	}
	conn, err := s.db.GetDB()
	if err != nil {
		return err
	}
	return fn(conn)
}

func (s *migrationStore) updatePhase(id string, phase string) error {
	return s.withConn("updatePhase", func(conn *sql.DB) error {
		_, err := conn.ExecContext(
			context.Background(),
			`UPDATE `+db.TableMigrations+` SET phase = $1, updated_at = $2 WHERE migration_id = $3`,
			phase,
			time.Now().UTC(),
			id,
		)
		if err != nil {
			return fmt.Errorf("update migration %s phase: %w", id, err)
		}
		return nil
	})
}

// updateUpdatedAt sets updated_at to now for the migration (e.g. after roots inserted or run ended).
func (s *migrationStore) updateUpdatedAt(id string) error {
	return s.withConn("updateUpdatedAt", func(conn *sql.DB) error {
		_, err := conn.ExecContext(
			context.Background(),
			`UPDATE `+db.TableMigrations+` SET updated_at = $1 WHERE migration_id = $2`,
			time.Now().UTC(),
			id,
		)
		if err != nil {
			return fmt.Errorf("update migration %s updated_at: %w", id, err)
		}
		return nil
	})
}

// updateRootConfig replaces root_config_json (serialized run knobs + roots; no FS adapters).
func (s *migrationStore) updateRootConfig(id string, rootConfigJSON string) error {
	return s.withConn("updateRootConfig", func(conn *sql.DB) error {
		_, err := conn.ExecContext(
			context.Background(),
			`UPDATE `+db.TableMigrations+` SET root_config_json = $1, updated_at = $2 WHERE migration_id = $3`,
			rootConfigJSON,
			time.Now().UTC(),
			id,
		)
		if err != nil {
			return fmt.Errorf("update migration %s root_config_json: %w", id, err)
		}
		return nil
	})
}

// updateRuntimeState merges stateJSON into existing runtime_state_json (suspend_v1 and other keys). Pass partial JSON to update only some keys.
func (s *migrationStore) updateRuntimeState(id string, stateJSON string) error {
	if s.db == nil || stateJSON == "" {
		return nil
	}
	conn, err := s.db.GetDB()
	if err != nil {
		return err
	}
	ctx := context.Background()
	var existing string
	err = conn.QueryRowContext(ctx, `SELECT COALESCE(runtime_state_json,'{}') FROM `+db.TableMigrations+` WHERE migration_id = $1`, id).Scan(&existing)
	if err != nil {
		return fmt.Errorf("read runtime_state %s: %w", id, err)
	}
	merged := make(map[string]any)
	if existing != "" && existing != "{}" {
		if err := json.Unmarshal([]byte(existing), &merged); err != nil {
			merged = make(map[string]any)
		}
	}
	var incoming map[string]any
	if err := json.Unmarshal([]byte(stateJSON), &incoming); err != nil {
		return fmt.Errorf("runtime_state JSON: %w", err)
	}
	for k, v := range incoming {
		merged[k] = v
	}
	out, err := json.Marshal(merged)
	if err != nil {
		return fmt.Errorf("runtime_state marshal: %w", err)
	}
	now := time.Now().UTC()
	_, err = conn.ExecContext(ctx, `UPDATE `+db.TableMigrations+` SET runtime_state_json = $1, updated_at = $2 WHERE migration_id = $3`, string(out), now, id)
	if err != nil {
		return fmt.Errorf("update migration %s runtime_state: %w", id, err)
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
