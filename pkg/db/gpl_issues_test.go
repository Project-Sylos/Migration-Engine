// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func TestSealBufferAppendsSparseGPLIssueOnce(t *testing.T) {
	database, err := Open(Options{Path: t.TempDir() + "/gpl-issues.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	issue := GPLIssue{
		SrcID:        "src-1",
		Status:       GPLIssueStatusPending,
		ProposedName: "clean.txt",
		IssuesJSON:   `[{"category":"InvalidChar"}]`,
	}
	// Seal path is gated by GPLDisabled; write directly to ops.
	if err := database.Ops().BatchPutGPL([]opsdb.GPLRecord{{
		SrcID: issue.SrcID, Status: issue.Status, ProposedName: issue.ProposedName, IssuesJSON: issue.IssuesJSON,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := database.Ops().BatchPutGPL([]opsdb.GPLRecord{{
		SrcID: issue.SrcID, Status: issue.Status, ProposedName: issue.ProposedName, IssuesJSON: issue.IssuesJSON,
	}}); err != nil {
		t.Fatal(err)
	}

	rec, ok, err := database.Ops().GetGPL(issue.SrcID)
	if err != nil || !ok {
		t.Fatalf("get gpl ok=%v err=%v", ok, err)
	}
	if rec.Status != GPLIssueStatusPending || rec.ProposedName != "clean.txt" {
		t.Fatalf("status=%q proposed=%q", rec.Status, rec.ProposedName)
	}
}

func TestReviewCanUpdateSparseGPLIssue(t *testing.T) {
	database, err := Open(Options{Path: t.TempDir() + "/gpl-review.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	if err := database.Ops().BatchPutGPL([]opsdb.GPLRecord{{
		SrcID: "src-1", Status: GPLIssueStatusPending, ProposedName: "clean.txt", UpdatedAt: 1,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := database.Ops().BatchPutGPL([]opsdb.GPLRecord{{
		SrcID: "src-1", Status: GPLIssueStatusAccepted, ProposedName: "chosen.txt", UpdatedAt: 2,
	}}); err != nil {
		t.Fatal(err)
	}

	rec, ok, err := database.Ops().GetGPL("src-1")
	if err != nil || !ok {
		t.Fatalf("get gpl ok=%v err=%v", ok, err)
	}
	if rec.Status != GPLIssueStatusAccepted || rec.ProposedName != "chosen.txt" {
		t.Fatalf("status=%q proposed=%q", rec.Status, rec.ProposedName)
	}
}
