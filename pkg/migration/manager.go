// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

// CreateMigrationConfig defines metadata persisted for a migration record.
// When MigrationDir is set, the engine creates the DB at MigrationDir/{id}.db immediately.
// When MigrationDir is empty, the engine returns a migration with a generated ID and no DB yet; the API creates the folder, then passes that path to GetMigration so the engine can create/open the DB when first needed.
type CreateMigrationConfig struct {
	Name            string
	ServiceMetadata any
	RootConfig      any
	// MigrationDir is the absolute path to the folder for this migration (e.g. data/{id}). If set, the DB is created there immediately; MigrationID is optional (engine generates if empty).
	MigrationDir string
	// MigrationID is optional; when set with MigrationDir, this ID is used (API pre-generated ID and created the folder).
	MigrationID string
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
// Either Path is set (legacy single DB for all migrations) or the API passes the migration folder path per migration (CreateMigration with MigrationDir, or GetMigration(id, migrationDir)).
type MigrationManager struct {
	db             *db.DB              // legacy single DB; nil when using per-migration paths
	store          *migrationStore     // used for create/get/list/delete with explicit db
	migrations     map[string]*Migration
	pendingRecords map[string]migrationRecord // migrations created without a path (no DB yet); key = id
	mu             sync.Mutex
	ownsDB         bool
	openDBs        map[string]*db.DB   // cache: key = absolute DB file path
	openDBsMu      sync.Mutex
	// pendingLocks serializes "pending → persist" per migration id so only one goroutine runs openDB + createMigration + bindDB for a given id.
	pendingMu   sync.Mutex
	pendingLocks map[string]*sync.Mutex
}

var migrationIDCounter int64

func nextMigrationID() string {
	now := time.Now().UTC().UnixNano()
	seq := atomic.AddInt64(&migrationIDCounter, 1)
	return fmt.Sprintf("migration-%d-%d", now, seq)
}

// NewMigrationManager opens the migration DB (legacy single-DB when Path is set) or creates a manager that uses per-migration paths (when Path is empty and the API will pass MigrationDir / migrationDir per call).
func NewMigrationManager(cfg DatabaseConfig) (*MigrationManager, error) {
	if cfg.Path != "" {
		database, _, err := SetupDatabase(cfg)
		if err != nil {
			return nil, err
		}
		return newMigrationManager(database, true), nil
	}
	// No Path: API will pass migration folder path when creating or loading each migration.
	return newMigrationManager(nil, false), nil
}

func newMigrationManager(database *db.DB, ownsDB bool) *MigrationManager {
	m := &MigrationManager{
		store:      newMigrationStore(database),
		migrations: make(map[string]*Migration),
		openDBs:    make(map[string]*db.DB),
		ownsDB:     ownsDB,
	}
	if database != nil {
		m.db = database
	} else {
		m.pendingRecords = make(map[string]migrationRecord)
		m.pendingLocks = make(map[string]*sync.Mutex)
	}
	return m
}

func (m *MigrationManager) getPendingLock(id string) *sync.Mutex {
	m.pendingMu.Lock()
	defer m.pendingMu.Unlock()
	if m.pendingLocks[id] == nil {
		m.pendingLocks[id] = &sync.Mutex{}
	}
	return m.pendingLocks[id]
}

// Close releases manager-owned resources (single DB in legacy mode, or all open per-migration DBs).
func (m *MigrationManager) Close() error {
	if m.db == nil {
		m.openDBsMu.Lock()
		for _, database := range m.openDBs {
			_ = database.Close()
		}
		m.openDBs = make(map[string]*db.DB)
		m.openDBsMu.Unlock()
		return nil
	}
	if !m.ownsDB {
		return nil
	}
	return m.db.Close()
}

// openDB opens (or returns cached) the DB for the given migration folder path and ID. migrationDir is the absolute path to the folder for this migration (e.g. data/{id}); the DB file is migrationDir/id.db.
func (m *MigrationManager) openDB(migrationDir, id string) (*db.DB, error) {
	absDir, err := filepath.Abs(migrationDir)
	if err != nil {
		return nil, fmt.Errorf("migration dir: %w", err)
	}
	dbPath := MigrationDBPath(absDir, id)
	m.openDBsMu.Lock()
	defer m.openDBsMu.Unlock()
	if database := m.openDBs[dbPath]; database != nil {
		return database, nil
	}
	if err := os.MkdirAll(absDir, 0755); err != nil {
		return nil, fmt.Errorf("create migration dir: %w", err)
	}
	cfg := DatabaseConfig{Path: dbPath}
	database, _, err := SetupDatabase(cfg)
	if err != nil {
		return nil, err
	}
	m.openDBs[dbPath] = database
	return database, nil
}

// CreateMigration registers a migration and returns a domain object. The API can either:
// 1) Pass MigrationDir (path to the migration's folder): engine creates the DB there and returns the migration; MigrationID is optional (engine generates if empty).
// 2) Omit MigrationDir: engine generates an ID and returns a migration with no DB yet; the API creates the folder (e.g. data/{id}), then calls GetMigration(id, migrationDir) so the engine creates the DB when first needed.
func (m *MigrationManager) CreateMigration(cfg CreateMigrationConfig) (*Migration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	name := cfg.Name
	if name == "" {
		name = "migration"
	}
	now := time.Now().UTC()
	id := cfg.MigrationID
	if id == "" {
		id = nextMigrationID()
	}
	record := migrationRecord{
		ID:                  id,
		Name:                name,
		Phase:               PhaseCreated,
		CreatedAt:           now,
		UpdatedAt:           now,
		ServiceMetadataJSON: marshalJSONOrEmpty(cfg.ServiceMetadata),
		RootConfigJSON:      marshalJSONOrEmpty(cfg.RootConfig),
	}

	if cfg.MigrationDir != "" {
		database, err := m.openDB(cfg.MigrationDir, id)
		if err != nil {
			return nil, err
		}
		if err := m.store.createMigration(database, record); err != nil {
			_ = database.Close()
			absDir, _ := filepath.Abs(cfg.MigrationDir)
			m.openDBsMu.Lock()
			delete(m.openDBs, MigrationDBPath(absDir, id))
			m.openDBsMu.Unlock()
			return nil, err
		}
		persisted, err := m.store.getMigration(database, id)
		if err != nil {
			return nil, err
		}
		if persisted != nil {
			record = *persisted
		}
		instance := newMigration(m, record, database)
		m.migrations[id] = instance
		return instance, nil
	}

	// Single-DB mode (e.g. local test): use the manager's DB so the migration has a DB immediately.
	if m.db != nil {
		if err := m.store.createMigration(m.db, record); err != nil {
			return nil, err
		}
		persisted, err := m.store.getMigration(m.db, id)
		if err != nil {
			return nil, err
		}
		if persisted != nil {
			record = *persisted
		}
		instance := newMigration(m, record, m.db)
		m.migrations[id] = instance
		return instance, nil
	}

	// No path yet: return migration with generated ID; DB will be created when API calls GetMigration(id, migrationDir).
	if m.pendingRecords == nil {
		m.pendingRecords = make(map[string]migrationRecord)
	}
	m.pendingRecords[id] = record
	instance := newMigration(m, record, nil)
	m.migrations[id] = instance
	return instance, nil
}

// isDuplicateKey treats any error that looks like a duplicate/unique/primary-key violation as "already inserted", so callers can return the existing migration. Covers DuckDB and other drivers (e.g. "TransactionContext Error: ...").
func isDuplicateKey(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate key") ||
		strings.Contains(msg, "primary key") ||
		strings.Contains(msg, "unique constraint") ||
		strings.Contains(msg, "constraint violated") ||
		strings.Contains(msg, "constraint violation")
}

// GetMigration loads or returns a cached migration. When using per-migration DBs, pass the absolute path to that migration's folder (e.g. data/{id}); the engine creates or opens the DB at migrationDir/id.db. When migrationDir is empty, uses the legacy single DB (Path must have been set when creating the manager).
func (m *MigrationManager) GetMigration(id string, migrationDir string) (*Migration, error) {
	m.mu.Lock()
	existing := m.migrations[id]
	if existing != nil {
		if existing.DB != nil {
			record, err := m.store.getMigration(existing.DB, id)
			m.mu.Unlock()
			if err != nil {
				return nil, err
			}
			if record != nil {
				existing.syncRecord(*record)
			}
			return existing, nil
		}
		// Pending migration (no DB yet): need migrationDir to create the DB. Serialize per id so only one goroutine runs openDB + createMigration + bindDB.
		if migrationDir == "" {
			m.mu.Unlock()
			return nil, fmt.Errorf("migration %q has no database yet: pass the migration folder path (e.g. data/%s) so the engine can create the DB", id, id)
		}
		record, ok := m.pendingRecords[id]
		if !ok {
			m.mu.Unlock()
			return existing, nil
		}
		m.mu.Unlock()
		lock := m.getPendingLock(id)
		lock.Lock()
		defer lock.Unlock()
		m.mu.Lock()
		existing = m.migrations[id]
		if existing != nil && existing.DB != nil {
			m.mu.Unlock()
			if persisted, _ := m.store.getMigration(existing.DB, id); persisted != nil {
				existing.syncRecord(*persisted)
			}
			return existing, nil
		}
		if existing == nil {
			m.mu.Unlock()
			return nil, nil
		}
		record, _ = m.pendingRecords[id]
		m.mu.Unlock()
		database, err := m.openDB(migrationDir, id)
		if err != nil {
			return nil, err
		}
		if err := m.store.createMigration(database, record); err != nil {
			if isDuplicateKey(err) {
				m.mu.Lock()
				out := m.migrations[id]
				if out != nil {
					delete(m.pendingRecords, id)
					out.bindDB(database)
				}
				m.mu.Unlock()
				if out != nil {
					if persisted, _ := m.store.getMigration(database, id); persisted != nil {
						out.syncRecord(*persisted)
					}
				}
				return out, nil
			}
			return nil, err
		}
		persisted, err := m.store.getMigration(database, id)
		if err != nil {
			return nil, err
		}
		m.mu.Lock()
		delete(m.pendingRecords, id)
		existing.bindDB(database)
		m.mu.Unlock()
		if persisted != nil {
			existing.syncRecord(*persisted)
		}
		return existing, nil
	}
	m.mu.Unlock()

	if migrationDir == "" {
		// Legacy single DB
		if m.db == nil {
			return nil, nil
		}
		record, err := m.store.getMigration(nil, id)
		if err != nil || record == nil {
			return nil, err
		}
		instance := newMigration(m, *record, m.db)
		m.mu.Lock()
		m.migrations[id] = instance
		m.mu.Unlock()
		return instance, nil
	}

	// Per-migration: load from disk
	dbPath := MigrationDBPath(migrationDir, id)
	if absDir, err := filepath.Abs(migrationDir); err == nil {
		dbPath = MigrationDBPath(absDir, id)
	}
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil, nil
	}
	database, err := m.openDB(migrationDir, id)
	if err != nil {
		return nil, err
	}
	record, err := m.store.getMigration(database, id)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, nil
	}
	instance := newMigration(m, *record, database)
	m.mu.Lock()
	m.migrations[id] = instance
	m.mu.Unlock()
	return instance, nil
}

// ListMigrations returns migration summaries. When using a single DB (Path set), dataDir is ignored. When using per-migration DBs, pass dataDir to scan for migration folders (e.g. data); each subdir dataDir/{id} is expected to contain {id}.db. When dataDir is empty and no single DB, returns only in-memory migrations (current process).
func (m *MigrationManager) ListMigrations(dataDir string) ([]MigrationSummary, error) {
	if m.db != nil {
		records, err := m.store.listMigrations(nil)
		if err != nil {
			return nil, err
		}
		out := make([]MigrationSummary, 0, len(records))
		for _, r := range records {
			out = append(out, MigrationSummary{ID: r.ID, Name: r.Name, Phase: r.Phase, CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt})
		}
		return out, nil
	}
	if dataDir != "" {
		absDir, _ := filepath.Abs(dataDir)
		entries, err := os.ReadDir(absDir)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, nil
			}
			return nil, err
		}
		var out []MigrationSummary
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			id := e.Name()
			migrationDir := filepath.Join(absDir, id)
			dbPath := MigrationDBPath(migrationDir, id)
			if _, err := os.Stat(dbPath); os.IsNotExist(err) {
				continue
			}
			database, err := m.openDB(migrationDir, id)
			if err != nil {
				continue
			}
			rec, err := m.store.getMigration(database, id)
			if err != nil || rec == nil {
				continue
			}
			out = append(out, MigrationSummary{ID: rec.ID, Name: rec.Name, Phase: rec.Phase, CreatedAt: rec.CreatedAt, UpdatedAt: rec.UpdatedAt})
		}
		return out, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]MigrationSummary, 0, len(m.migrations))
	for id, mig := range m.migrations {
		if r, ok := m.pendingRecords[id]; ok {
			out = append(out, MigrationSummary{ID: r.ID, Name: r.Name, Phase: r.Phase, CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt})
		} else {
			out = append(out, MigrationSummary{ID: mig.ID, Name: mig.Name, Phase: mig.Phase(), CreatedAt: time.Time{}, UpdatedAt: time.Time{}})
		}
	}
	return out, nil
}

// GetMigrationDetails returns the full migration record. When using per-migration DBs, pass migrationDir (path to that migration's folder) if the migration is not already loaded in memory.
func (m *MigrationManager) GetMigrationDetails(id string, migrationDir string) (*MigrationDetails, error) {
	if m.db != nil {
		record, err := m.store.getMigration(nil, id)
		if err != nil || record == nil {
			return nil, err
		}
		return recordToDetails(record), nil
	}
	m.mu.Lock()
	pending, hasPending := m.pendingRecords[id]
	existing := m.migrations[id]
	m.mu.Unlock()
	if hasPending {
		return &MigrationDetails{ID: pending.ID, Name: pending.Name, Phase: pending.Phase, CreatedAt: pending.CreatedAt, UpdatedAt: pending.UpdatedAt, ServiceMetadataJSON: pending.ServiceMetadataJSON, RootConfigJSON: pending.RootConfigJSON}, nil
	}
	if existing != nil && existing.DB != nil {
		record, err := m.store.getMigration(existing.DB, id)
		if err != nil || record == nil {
			return nil, err
		}
		return recordToDetails(record), nil
	}
	if migrationDir != "" {
		database, err := m.openDB(migrationDir, id)
		if err != nil {
			return nil, err
		}
		record, err := m.store.getMigration(database, id)
		if err != nil || record == nil {
			return nil, err
		}
		return recordToDetails(record), nil
	}
	return nil, nil
}

func recordToDetails(r *migrationRecord) *MigrationDetails {
	return &MigrationDetails{
		ID:                  r.ID,
		Name:                r.Name,
		Phase:               r.Phase,
		CreatedAt:           r.CreatedAt,
		UpdatedAt:           r.UpdatedAt,
		ServiceMetadataJSON: r.ServiceMetadataJSON,
		RootConfigJSON:      r.RootConfigJSON,
	}
}

// DeleteMigration removes the migration from memory and, when a DB exists, deletes its record. When using per-migration DBs, pass migrationDir so the engine can open the DB, delete the row, and close it; otherwise only in-memory state is removed.
func (m *MigrationManager) DeleteMigration(id string, migrationDir string) error {
	m.mu.Lock()
	delete(m.migrations, id)
	delete(m.pendingRecords, id)
	m.mu.Unlock()
	if m.db != nil {
		return m.store.deleteMigration(m.db, id)
	}
	if migrationDir == "" {
		return nil
	}
	absDir, _ := filepath.Abs(migrationDir)
	dbPath := MigrationDBPath(absDir, id)
	database, err := m.openDB(migrationDir, id)
	if err != nil {
		return err
	}
	if err := m.store.deleteMigration(database, id); err != nil {
		return err
	}
	_ = database.Close()
	m.openDBsMu.Lock()
	delete(m.openDBs, dbPath)
	m.openDBsMu.Unlock()
	return nil
}

func migrationNameFromConfig(cfg Config) string {
	if cfg.Database.Path != "" {
		return filepath.Base(cfg.Database.Path)
	}
	return "migration"
}

