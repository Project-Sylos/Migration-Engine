// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

// CreateMigrationConfig defines metadata persisted for a migration record.
type CreateMigrationConfig struct {
	Name            string
	ServiceMetadata any
	RootConfig      any
}

// MigrationSummary is a compact projection returned by ListMigrations.
type MigrationSummary struct {
	ID        string
	Name      string
	Phase     Phase
	CreatedAt time.Time
	UpdatedAt time.Time
}

// MigrationDetails is the full migration record from the DB, for API detail views.
// The engine owns all DB access; use this type via GetMigrationDetails so the API never touches the DB.
type MigrationDetails struct {
	ID                  string
	Name                string
	Phase               Phase
	CreatedAt           time.Time
	UpdatedAt           time.Time
	ServiceMetadataJSON string
	RootConfigJSON      string
}

// MigrationManager owns migration lifecycle authority and persistence access.
type MigrationManager struct {
	db         *db.DB
	store      *migrationStore
	migrations map[string]*Migration
	mu         sync.Mutex
	ownsDB     bool
}

var migrationIDCounter int64

func nextMigrationID() string {
	now := time.Now().UTC().UnixNano()
	seq := atomic.AddInt64(&migrationIDCounter, 1)
	return fmt.Sprintf("migration-%d-%d", now, seq)
}

// NewMigrationManager opens the migration DB and becomes the lifecycle owner.
func NewMigrationManager(cfg DatabaseConfig) (*MigrationManager, error) {
	database, _, err := SetupDatabase(cfg)
	if err != nil {
		return nil, err
	}
	return newMigrationManager(database, true), nil
}

func newMigrationManager(database *db.DB, ownsDB bool) *MigrationManager {
	manager := &MigrationManager{
		db:         database,
		store:      newMigrationStore(database),
		migrations: make(map[string]*Migration),
		ownsDB:     ownsDB,
	}
	return manager
}

// Close releases manager-owned resources.
func (m *MigrationManager) Close() error {
	if !m.ownsDB || m.db == nil {
		return nil
	}
	return m.db.Close()
}

// CreateMigration registers a migration record and returns a domain object handle.
func (m *MigrationManager) CreateMigration(cfg CreateMigrationConfig) (*Migration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	name := cfg.Name
	if name == "" {
		name = "migration"
	}
	now := time.Now().UTC()
	record := migrationRecord{
		ID:                  nextMigrationID(),
		Name:                name,
		Phase:               PhaseCreated,
		CreatedAt:           now,
		UpdatedAt:           now,
		ServiceMetadataJSON: marshalJSONOrEmpty(cfg.ServiceMetadata),
		RootConfigJSON:      marshalJSONOrEmpty(cfg.RootConfig),
	}
	if err := m.store.createMigration(record); err != nil {
		return nil, err
	}
	instance := newMigration(m, record)
	m.migrations[record.ID] = instance
	return instance, nil
}

// GetMigration loads or returns a cached migration domain object.
func (m *MigrationManager) GetMigration(id string) (*Migration, error) {
	m.mu.Lock()
	if existing := m.migrations[id]; existing != nil {
		m.mu.Unlock()
		return existing, nil
	}
	m.mu.Unlock()

	record, err := m.store.getMigration(id)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, nil
	}

	instance := newMigration(m, *record)
	m.mu.Lock()
	m.migrations[id] = instance
	m.mu.Unlock()
	return instance, nil
}

// ListMigrations returns persisted migration summaries.
func (m *MigrationManager) ListMigrations() ([]MigrationSummary, error) {
	records, err := m.store.listMigrations()
	if err != nil {
		return nil, err
	}
	out := make([]MigrationSummary, 0, len(records))
	for _, r := range records {
		out = append(out, MigrationSummary{
			ID:        r.ID,
			Name:      r.Name,
			Phase:     r.Phase,
			CreatedAt: r.CreatedAt,
			UpdatedAt: r.UpdatedAt,
		})
	}
	return out, nil
}

// GetMigrationDetails returns the full migration record from the DB.
// Use this for "GET migration by ID" detail views; the API should not query the DB directly.
func (m *MigrationManager) GetMigrationDetails(id string) (*MigrationDetails, error) {
	record, err := m.store.getMigration(id)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, nil
	}
	return &MigrationDetails{
		ID:                  record.ID,
		Name:                record.Name,
		Phase:               record.Phase,
		CreatedAt:           record.CreatedAt,
		UpdatedAt:           record.UpdatedAt,
		ServiceMetadataJSON: record.ServiceMetadataJSON,
		RootConfigJSON:      record.RootConfigJSON,
	}, nil
}

// DeleteMigration removes persisted metadata and in-memory runtime state.
func (m *MigrationManager) DeleteMigration(id string) error {
	m.mu.Lock()
	delete(m.migrations, id)
	m.mu.Unlock()
	return m.store.deleteMigration(id)
}

func migrationNameFromConfig(cfg Config) string {
	if cfg.Database.Path != "" {
		return filepath.Base(cfg.Database.Path)
	}
	return "migration"
}

