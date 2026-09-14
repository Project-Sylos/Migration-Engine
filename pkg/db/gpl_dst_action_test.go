// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func TestUpdateGPLIssueWithAction_Rename(t *testing.T) {
	database, err := Open(Options{Path: t.TempDir() + "/gpl-dst-action.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	now := time.Now().UnixNano()
	if err := database.Ops().BatchPutGPL([]opsdb.GPLRecord{{
		SrcID: "src-1", Status: GPLIssueStatusAccepted, ProposedName: "clean",
		UpdatedAt: now, DstAction: DstActionRename,
	}}); err != nil {
		t.Fatal(err)
	}

	rec, ok, err := database.Ops().GetGPL("src-1")
	if err != nil || !ok {
		t.Fatalf("get gpl ok=%v err=%v", ok, err)
	}
	if rec.Status != GPLIssueStatusAccepted || rec.ProposedName != "clean" || rec.DstAction != DstActionRename {
		t.Fatalf("got status=%q proposed=%q action=%q", rec.Status, rec.ProposedName, rec.DstAction)
	}
}
