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
	Phase     string // From DB; merged with in-memory phase when this process has the migration loaded (see Live).
	CreatedAt time.Time
	UpdatedAt time.Time
	Live      bool // true when a run is active (traversal, copy, or retry); merged from in-memory Migration when loaded
}

// MigrationDetails is the full migration record from the DB, for API detail views.
// The engine owns all DB access; use this type via GetMigrationDetails so the API never touches the DB.
type MigrationDetails struct {
	ID                  string
	Name                string
	Phase               string // From DB; merged with in-memory phase when this process has the migration loaded (see Live).
	CreatedAt           time.Time
	UpdatedAt           time.Time
	ServiceMetadataJSON string
	RootConfigJSON      string
	Live                bool // true when a run is active (traversal, copy, or retry); merged from in-memory Migration when loaded
}

// MigrationManager owns migration lifecycle authority and persistence access.
// Each migration has its own DuckDB file at {migrationDir}/{id}.db.
type MigrationManager struct {
	store          *migrationStore
	migrations     map[string]*Migration
	pendingRecords map[string]migrationRecord // migrations created without a path (no DB yet); key = id
	mu             sync.Mutex
	openDBs        map[string]*db.DB // cache: key = absolute DB file path
	openDBsMu      sync.Mutex
	pendingMu      sync.Mutex
	pendingLocks   map[string]*sync.Mutex
}

var migrationIDCounter int64

func nextMigrationID() string {
	now := time.Now().UTC().UnixNano()
	seq := atomic.AddInt64(&migrationIDCounter, 1)
	return fmt.Sprintf("migration-%d-%d", now, seq)
}

// NewMigrationManager creates a manager that opens one DuckDB file per migration folder.
func NewMigrationManager() *MigrationManager {
	return &MigrationManager{
		store:          &migrationStore{},
		migrations:     make(map[string]*Migration),
		pendingRecords: make(map[string]migrationRecord),
		pendingLocks:   make(map[string]*sync.Mutex),
		openDBs:        make(map[string]*db.DB),
	}
}

func (m *MigrationManager) getPendingLock(id string) *sync.Mutex {
	m.pendingMu.Lock()
	defer m.pendingMu.Unlock()
	if m.pendingLocks[id] == nil {
		m.pendingLocks[id] = &sync.Mutex{}
	}
	return m.pendingLocks[id]
}

// Close releases all open per-migration DB handles owned by this manager.
func (m *MigrationManager) Close() error {
	m.openDBsMu.Lock()
	defer m.openDBsMu.Unlock()
	for _, database := range m.openDBs {
		_ = database.Close()
	}
	m.openDBs = make(map[string]*db.DB)
	return nil
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

// GetMigration loads or returns a cached migration. Pass the absolute path to that migration's folder (e.g. data/{id}); the engine creates or opens the DB at migrationDir/id.db.
func (m *MigrationManager) GetMigration(id string, migrationDir string) (*Migration, error) {
	snap := m.snapshotMigrationEntry(id)
	if snap.Migration != nil {
		if snap.HasDB {
			if snap.Migration.IsLive() {
				if _, ok := snap.Migration.cachedMigrationDetailsForLiveAPI(); ok {
					return snap.Migration, nil
				}
			}
			record, err := m.store.getMigration(snap.Migration.DB, id)
			if err != nil {
				return nil, err
			}
			if record != nil {
				snap.Migration.syncRecord(*record)
			}
			return snap.Migration, nil
		}
		if migrationDir == "" {
			return nil, fmt.Errorf("migration %q has no database yet: pass the migration folder path (e.g. data/%s) so the engine can create the DB", id, id)
		}
		if !snap.HasPending {
			return snap.Migration, nil
		}
		record := snap.Pending
		lock := m.getPendingLock(id)
		lock.Lock()
		defer lock.Unlock()

		after := m.snapshotMigrationEntry(id)
		if after.Migration != nil && after.HasDB {
			if after.Migration.IsLive() {
				if _, ok := after.Migration.cachedMigrationDetailsForLiveAPI(); ok {
					return after.Migration, nil
				}
			}
			if persisted, _ := m.store.getMigration(after.Migration.DB, id); persisted != nil {
				after.Migration.syncRecord(*persisted)
			}
			return after.Migration, nil
		}
		if after.Migration == nil {
			return nil, nil
		}
		existing := after.Migration

		database, err := m.openDB(migrationDir, id)
		if err != nil {
			return nil, err
		}
		if err := m.store.createMigration(database, record); err != nil {
			if isDuplicateKey(err) {
				out := m.tryBindDuplicateMigration(id, database)
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
		m.bindPendingMigrationDB(id, existing, database)
		if persisted != nil {
			existing.syncRecord(*persisted)
		}
		return existing, nil
	}

	if migrationDir == "" {
		return nil, fmt.Errorf("migration %q not loaded: pass migrationDir to open its DB", id)
	}

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
	m.putMigration(id, instance)
	return instance, nil
}

// ListMigrations returns migration summaries. Pass dataDir to scan for migration folders (e.g. data); each subdir dataDir/{id} is expected to contain {id}.db. When dataDir is empty, returns only in-memory migrations for this process.
func (m *MigrationManager) ListMigrations(dataDir string) ([]MigrationSummary, error) {
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
			s := MigrationSummary{ID: rec.ID, Name: rec.Name, Phase: rec.Phase, CreatedAt: rec.CreatedAt, UpdatedAt: rec.UpdatedAt, Live: false}
			m.overlayRuntimeFromCache(rec.ID, &s.Live, &s.Phase)
			out = append(out, s)
		}
		return out, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]MigrationSummary, 0, len(m.migrations))
	for id, mig := range m.migrations {
		if r, ok := m.pendingRecords[id]; ok {
			out = append(out, MigrationSummary{ID: r.ID, Name: r.Name, Phase: r.Phase, CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt, Live: false})
		} else {
			out = append(out, MigrationSummary{ID: mig.ID, Name: mig.Name, Phase: mig.Phase(), CreatedAt: time.Time{}, UpdatedAt: time.Time{}, Live: mig.IsLive()})
		}
	}
	return out, nil
}

// GetMigrationDetails returns the full migration record. Pass migrationDir (path to that migration's folder) if the migration is not already loaded in memory.
func (m *MigrationManager) GetMigrationDetails(id string, migrationDir string) (*MigrationDetails, error) {
	m.mu.Lock()
	pending, hasPending := m.pendingRecords[id]
	existing := m.migrations[id]
	m.mu.Unlock()
	if hasPending {
		return &MigrationDetails{ID: pending.ID, Name: pending.Name, Phase: pending.Phase, CreatedAt: pending.CreatedAt, UpdatedAt: pending.UpdatedAt, ServiceMetadataJSON: pending.ServiceMetadataJSON, RootConfigJSON: pending.RootConfigJSON, Live: false}, nil
	}
	if existing != nil && existing.DB != nil {
		if existing.IsLive() {
			if d, ok := existing.cachedMigrationDetailsForLiveAPI(); ok {
				m.overlayRuntimeFromCache(id, &d.Live, &d.Phase)
				return d, nil
			}
		}
		record, err := m.store.getMigration(existing.DB, id)
		if err != nil || record == nil {
			return nil, err
		}
		d := recordToDetails(record)
		m.overlayRuntimeFromCache(id, &d.Live, &d.Phase)
		return d, nil
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
		d := recordToDetails(record)
		m.overlayRuntimeFromCache(id, &d.Live, &d.Phase)
		return d, nil
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
		Live:                false,
	}
}

// runtimeViewFromCache returns live run state and phase from an in-memory Migration, if this process has loaded it.
// Live exists only on the domain object; DB rows do not encode "running", so list/detail projections must merge when possible.
func (m *MigrationManager) runtimeViewFromCache(id string) (live bool, phase string, ok bool) {
	m.mu.Lock()
	mig := m.migrations[id]
	m.mu.Unlock()
	if mig == nil {
		return false, "", false
	}
	return mig.IsLive(), mig.Phase(), true
}

func (m *MigrationManager) overlayRuntimeFromCache(id string, live *bool, phase *string) {
	if l, p, ok := m.runtimeViewFromCache(id); ok {
		*live = l
		*phase = p
	}
}

// DeleteMigration removes the migration from memory and, when a DB exists, deletes its record. Pass migrationDir so the engine can open the DB, delete the row, and close it; otherwise only in-memory state is removed.
func (m *MigrationManager) DeleteMigration(id string, migrationDir string) error {
	m.mu.Lock()
	delete(m.migrations, id)
	delete(m.pendingRecords, id)
	m.mu.Unlock()
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
		_, id := MigrationDirAndIDFromDBPath(cfg.Database.Path)
		return id
	}
	return "migration"
}

type migrationEntrySnapshot struct {
	Migration  *Migration
	HasDB      bool
	HasPending bool
	Pending    migrationRecord
}

func (m *MigrationManager) snapshotMigrationEntry(id string) migrationEntrySnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	var snap migrationEntrySnapshot
	snap.Migration = m.migrations[id]
	if snap.Migration != nil {
		snap.HasDB = snap.Migration.DB != nil
	}
	if m.pendingRecords != nil {
		if r, ok := m.pendingRecords[id]; ok {
			snap.HasPending = true
			snap.Pending = r
		}
	}
	return snap
}

func (m *MigrationManager) putMigration(id string, instance *Migration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.migrations[id] = instance
}

func (m *MigrationManager) tryBindDuplicateMigration(id string, database *db.DB) *Migration {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := m.migrations[id]
	if out != nil && m.pendingRecords != nil {
		delete(m.pendingRecords, id)
	}
	if out != nil {
		out.bindDB(database)
	}
	return out
}

func (m *MigrationManager) bindPendingMigrationDB(id string, existing *Migration, database *db.DB) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pendingRecords != nil {
		delete(m.pendingRecords, id)
	}
	existing.bindDB(database)
}
