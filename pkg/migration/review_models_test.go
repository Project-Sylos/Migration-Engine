// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func TestReviewStatsRaw_ToPathReviewStats_PhaseAware(t *testing.T) {
	raw := ReviewStatsRaw{
		TraversalPending:     10,
		TraversalPendingRetry: 10,
		TraversalFailed:      2,
		CopyPending:         5,
		CopyFailed:          1,
		Excluded:            3,
		Folders:             100,
		Files:               200,
		SizeSrc:             1000,
		SizeDst:             800,
	}

	// PhaseReview: use traversal pending/failed
	gotReview := raw.ToPathReviewStats(PhaseReview)
	if gotReview.PendingCount != 10 || gotReview.FailedCount != 2 || gotReview.PendingRetriesCount != 10 {
		t.Errorf("PhaseReview: PendingCount=%d FailedCount=%d PendingRetriesCount=%d, want 10, 2, 10",
			gotReview.PendingCount, gotReview.FailedCount, gotReview.PendingRetriesCount)
	}
	if gotReview.ExcludedCount != 3 || gotReview.FoldersCount != 100 || gotReview.FilesCount != 200 {
		t.Errorf("PhaseReview: ExcludedCount=%d FoldersCount=%d FilesCount=%d", gotReview.ExcludedCount, gotReview.FoldersCount, gotReview.FilesCount)
	}
	if gotReview.TotalFileSize.Src != 1000 || gotReview.TotalFileSize.Dst != 800 {
		t.Errorf("PhaseReview: TotalFileSize want 1000/800 got %d/%d", gotReview.TotalFileSize.Src, gotReview.TotalFileSize.Dst)
	}

	// PhaseCopying: use copy pending/failed
	gotCopy := raw.ToPathReviewStats(PhaseCopying)
	if gotCopy.PendingCount != 5 || gotCopy.FailedCount != 1 || gotCopy.PendingRetriesCount != 5 {
		t.Errorf("PhaseCopying: PendingCount=%d FailedCount=%d PendingRetriesCount=%d, want 5, 1, 5",
			gotCopy.PendingCount, gotCopy.FailedCount, gotCopy.PendingRetriesCount)
	}

	// PhaseCompleted: same as copy
	gotDone := raw.ToPathReviewStats(PhaseCompleted)
	if gotDone.PendingCount != 5 || gotDone.FailedCount != 1 {
		t.Errorf("PhaseCompleted: PendingCount=%d FailedCount=%d", gotDone.PendingCount, gotDone.FailedCount)
	}

	// Default/other phase: traversal (pendingRetriesCount from TraversalPendingRetry)
	gotOther := raw.ToPathReviewStats(PhaseCreated)
	if gotOther.PendingCount != 10 || gotOther.FailedCount != 2 || gotOther.PendingRetriesCount != 10 {
		t.Errorf("PhaseCreated: PendingCount=%d FailedCount=%d PendingRetriesCount=%d", gotOther.PendingCount, gotOther.FailedCount, gotOther.PendingRetriesCount)
	}
}

func TestReviewStatsRaw_ToPathReviewStats_Ratios(t *testing.T) {
	raw := ReviewStatsRaw{Folders: 1, Files: 3}
	got := raw.ToPathReviewStats(PhaseReview)
	total := got.FoldersCount + got.FilesCount
	if total != 4 {
		t.Errorf("total want 4 got %d", total)
	}
	// 1/4 = 0.25, 3/4 = 0.75
	if got.FoldersRatio != 0.25 || got.FilesRatio != 0.75 {
		t.Errorf("FoldersRatio=%f FilesRatio=%f want 0.25, 0.75", got.FoldersRatio, got.FilesRatio)
	}
}

func TestPathReviewActionResult_ExcludeReturnsAffectedCount(t *testing.T) {
	dir := t.TempDir()
	id := "test-exclude-count"
	migrationDir := filepath.Join(dir, id)
	if err := os.MkdirAll(migrationDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	dbPath := MigrationDBPath(migrationDir, id)
	d, err := db.Open(db.Options{Path: dbPath})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	ctx := context.Background()
	conn, err := d.GetDB()
	if err != nil {
		d.Close()
		t.Fatalf("get conn: %v", err)
	}
	now := time.Now().UTC()
	_, err = conn.ExecContext(ctx, `INSERT INTO `+db.TableMigrations+` (migration_id, name, phase, created_at, updated_at, service_metadata_json, root_config_json) VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		id, "test", PhaseReview.String(), now, now, "", "")
	if err != nil {
		d.Close()
		t.Fatalf("insert migration: %v", err)
	}
	srcRoot := &db.NodeState{
		ID:              db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/"),
		Path:            "/",
		ParentPath:      "",
		Type:            db.NodeTypeFolder,
		Depth:           0,
		TraversalStatus: db.StatusPending,
		CopyStatus:      db.CopyStatusPending,
	}
	if err := db.InsertRootNode(d, "SRC", srcRoot); err != nil {
		d.Close()
		t.Fatalf("insert root: %v", err)
	}
	d.Close()

	m, err := NewMigrationManager(DatabaseConfig{})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer m.Close()
	mig, err := m.GetMigration(id, migrationDir)
	if err != nil {
		t.Fatalf("get migration: %v", err)
	}
	if mig == nil {
		t.Fatal("migration is nil")
	}
	if mig.Phase() != PhaseReview {
		t.Fatalf("phase want %s got %s", PhaseReview, mig.Phase())
	}

	res, err := mig.SetNodeExcluded("SRC", srcRoot.ID, true)
	if err != nil {
		t.Fatalf("SetNodeExcluded: %v", err)
	}
	if res.AffectedCount != 1 {
		t.Errorf("AffectedCount want 1 got %d", res.AffectedCount)
	}
	if res.Deltas["excluded"] != 1 || res.Deltas["pending"] != -1 {
		t.Errorf("Deltas want excluded=1 pending=-1 got %v", res.Deltas)
	}

	// Second call with same excluded=true should affect 0 (already excluded).
	res2, err := mig.SetNodeExcluded("SRC", srcRoot.ID, true)
	if err != nil {
		t.Fatalf("SetNodeExcluded again: %v", err)
	}
	if res2.AffectedCount != 0 {
		t.Errorf("AffectedCount (no-op) want 0 got %d", res2.AffectedCount)
	}
}
