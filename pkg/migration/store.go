// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"encoding/json"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
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
	db       *db.DB
	tokenKey []byte
}

func newMigrationStore(database *db.DB, tokenKey []byte) *migrationStore {
	return &migrationStore{db: database, tokenKey: tokenKey}
}

func (s *migrationStore) setTokenKey(tokenKey []byte) {
	s.tokenKey = tokenKey
}

func (s *migrationStore) ops() *opsdb.Store {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Ops()
}

func migrationToOps(record migrationRecord) opsdb.MigrationMetaRecord {
	return opsdb.MigrationMetaRecord{
		MigrationID:         record.ID,
		Name:                record.Name,
		Phase:               record.Phase,
		CreatedAt:           record.CreatedAt.UnixNano(),
		UpdatedAt:           record.UpdatedAt.UnixNano(),
		ServiceMetadataJSON: record.ServiceMetadataJSON,
		RootConfigJSON:      record.RootConfigJSON,
		RuntimeStateJSON:    record.RuntimeStateJSON,
	}
}

func migrationFromOps(rec opsdb.MigrationMetaRecord) (*migrationRecord, error) {
	phase, err := ParsePhase(rec.Phase)
	if err != nil {
		return nil, fmt.Errorf("parse migration %s phase: %w", rec.MigrationID, err)
	}
	return &migrationRecord{
		ID:                  rec.MigrationID,
		Name:                rec.Name,
		Phase:               phase,
		CreatedAt:           time.Unix(0, rec.CreatedAt).UTC(),
		UpdatedAt:           time.Unix(0, rec.UpdatedAt).UTC(),
		ServiceMetadataJSON: rec.ServiceMetadataJSON,
		RootConfigJSON:      rec.RootConfigJSON,
		RuntimeStateJSON:    rec.RuntimeStateJSON,
	}, nil
}

func (s *migrationStore) createMigration(database *db.DB, record migrationRecord) error {
	if database == nil || database.Ops() == nil {
		return fmt.Errorf("database required")
	}
	existing, ok, err := database.Ops().GetMigrationMeta(record.ID)
	if err != nil {
		return err
	}
	if ok && existing.MigrationID != "" {
		return nil
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	if record.UpdatedAt.IsZero() {
		record.UpdatedAt = record.CreatedAt
	}
	if err := database.Ops().PutMigrationMeta(migrationToOps(record)); err != nil {
		return fmt.Errorf("create migration %s: %w", record.ID, err)
	}
	return nil
}

func (s *migrationStore) getMigration(database *db.DB, id string) (*migrationRecord, error) {
	if database == nil || database.Ops() == nil {
		return nil, fmt.Errorf("database required")
	}
	rec, ok, err := database.Ops().GetMigrationMeta(id)
	if err != nil {
		return nil, fmt.Errorf("get migration %s: %w", id, err)
	}
	if !ok {
		return nil, nil
	}
	return migrationFromOps(rec)
}

func (s *migrationStore) deleteMigration(database *db.DB, id string) error {
	if database == nil || database.Ops() == nil {
		return fmt.Errorf("database required")
	}
	if err := database.Ops().DeleteMigrationMeta(id); err != nil {
		return fmt.Errorf("delete migration %s: %w", id, err)
	}
	return nil
}

func (s *migrationStore) updateMigrationField(id, column, value string, op string) error {
	ops := s.ops()
	if ops == nil {
		return fmt.Errorf("%s requires store db", op)
	}
	rec, ok, err := ops.GetMigrationMeta(id)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("update migration %s %s: not found", id, column)
	}
	switch column {
	case "name":
		rec.Name = value
	case "phase":
		rec.Phase = value
	case "service_metadata_json":
		rec.ServiceMetadataJSON = value
	case "root_config_json":
		rec.RootConfigJSON = value
	case "runtime_state_json":
		rec.RuntimeStateJSON = value
	default:
		return fmt.Errorf("update migration %s: unknown column %s", id, column)
	}
	rec.UpdatedAt = time.Now().UTC().UnixNano()
	if err := ops.PutMigrationMeta(rec); err != nil {
		return fmt.Errorf("update migration %s %s: %w", id, column, err)
	}
	return nil
}

func (s *migrationStore) updateUpdatedAt(id string) error {
	ops := s.ops()
	if ops == nil {
		return fmt.Errorf("updateUpdatedAt requires store db")
	}
	rec, ok, err := ops.GetMigrationMeta(id)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("update migration %s updated_at: not found", id)
	}
	rec.UpdatedAt = time.Now().UTC().UnixNano()
	if err := ops.PutMigrationMeta(rec); err != nil {
		return fmt.Errorf("update migration %s updated_at: %w", id, err)
	}
	return nil
}

func (s *migrationStore) updateRuntimeState(id string, stateJSON string) error {
	ops := s.ops()
	if ops == nil || stateJSON == "" {
		return nil
	}
	rec, ok, err := ops.GetMigrationMeta(id)
	if err != nil {
		return fmt.Errorf("read runtime_state %s: %w", id, err)
	}
	if !ok {
		return fmt.Errorf("read runtime_state %s: not found", id)
	}
	existing := rec.RuntimeStateJSON
	if existing == "" {
		existing = "{}"
	}
	merged := make(map[string]any)
	if existing != "{}" {
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
	rec.RuntimeStateJSON = string(out)
	rec.UpdatedAt = time.Now().UTC().UnixNano()
	if err := ops.PutMigrationMeta(rec); err != nil {
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
