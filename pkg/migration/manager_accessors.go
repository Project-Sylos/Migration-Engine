// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

// ============================================================================
// Thread-safe accessors for MigrationManager.mu-protected state.
// Logic functions should use these instead of locking manually.
// ============================================================================

// migrationEntrySnapshot is a point-in-time view of cache + pending state for one id.
type migrationEntrySnapshot struct {
	Migration  *Migration
	HasDB      bool
	HasPending bool
	Pending    migrationRecord
}

// snapshotMigrationEntry returns cached migration, whether it already has a DB,
// and whether a pending record exists (copy under lock).
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

// getLegacyDB returns the manager's single DB, if any (legacy mode).
func (m *MigrationManager) getLegacyDB() *db.DB {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.db
}

// putMigration caches a migration instance by id.
func (m *MigrationManager) putMigration(id string, instance *Migration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.migrations[id] = instance
}

// tryBindDuplicateMigration handles createMigration duplicate key: if the migration
// is still cached, clears pending and binds database; returns the migration or nil.
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

// bindPendingMigrationDB clears pending and attaches database to the cached migration.
func (m *MigrationManager) bindPendingMigrationDB(id string, existing *Migration, database *db.DB) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pendingRecords != nil {
		delete(m.pendingRecords, id)
	}
	existing.bindDB(database)
}
