// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"strings"
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func insertMigrationRow(t *testing.T, database *db.DB, id, phase, runtimeJSON string) {
	t.Helper()
	now := time.Now().UTC()
	if err := database.Ops().PutMigrationMeta(opsdb.MigrationMetaRecord{
		MigrationID: id, Name: id, Phase: phase,
		CreatedAt: now.UnixNano(), UpdatedAt: now.UnixNano(),
		ServiceMetadataJSON: "{}", RootConfigJSON: "{}", RuntimeStateJSON: runtimeJSON,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRepairTraversalSuspendV1FallbackRoundAndCursor(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/repair-suspend.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	seedTraversalFolder(t, database, "SRC", "folder-a", "/a", 7, db.StatusSuccessful)
	seedTraversalFolder(t, database, "SRC", "folder-b", "/b", 7, db.StatusPending)
	insertMigrationRow(t, database, "mig-1", PhaseTraversalSuspended, `{}`)

	s, err := RepairTraversalSuspendV1(database, 7, "mig-1")
	if err != nil {
		t.Fatal(err)
	}
	if s.LastRoundSrc != 7 {
		t.Fatalf("last_round_src=%d want 7", s.LastRoundSrc)
	}
	if s.SrcKeysetCursor != "folder-a" {
		t.Fatalf("src cursor=%q want folder-a", s.SrcKeysetCursor)
	}
	got, ok := parseRuntimeSuspendV1(mustRuntimeJSON(t, database, "mig-1"))
	if !ok {
		t.Fatal("expected suspend_v1 after repair")
	}
	if got.LastRoundSrc != 7 || got.SrcKeysetCursor != "folder-a" {
		t.Fatalf("stored %+v", got)
	}
}

func TestRepairTraversalSuspendV1KeepsLastRounds(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/repair-keep-round.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	insertMigrationRow(t, database, "mig-1", PhaseTraversalSuspended,
		`{"suspend_v1":{"version":1,"kind":"traversal","last_round_src":4,"last_round_dst":3}}`)

	s, err := RepairTraversalSuspendV1(database, 7, "mig-1")
	if err != nil {
		t.Fatal(err)
	}
	if s.LastRoundSrc != 4 || s.LastRoundDst != 3 {
		t.Fatalf("rounds src=%d dst=%d want 4/3", s.LastRoundSrc, s.LastRoundDst)
	}
}

func TestRepairTraversalSuspendV1RefusesLive(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/repair-live.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	insertMigrationRow(t, database, "mig-1", PhaseTraversing, `{}`)
	_, err = RepairTraversalSuspendV1(database, 7, "mig-1")
	if err == nil || !strings.Contains(err.Error(), "live") {
		t.Fatalf("want live error, got %v", err)
	}
}

func mustRuntimeJSON(t *testing.T, database *db.DB, id string) string {
	t.Helper()
	rec, ok, err := database.Ops().GetMigrationMeta(id)
	if err != nil || !ok {
		t.Fatalf("migration meta ok=%v err=%v", ok, err)
	}
	return rec.RuntimeStateJSON
}
