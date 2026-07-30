// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"testing"
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
	database.AppendGPLIssue(issue)
	database.AppendGPLIssue(issue) // retry/duplicate producer is ignored by the flush
	if err := database.Flush(); err != nil {
		t.Fatal(err)
	}

	conn, err := database.GetDB()
	if err != nil {
		t.Fatal(err)
	}
	var legacyTables int
	if err := conn.QueryRowContext(context.Background(), `
SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'path_events'`,
	).Scan(&legacyTables); err != nil {
		t.Fatal(err)
	}
	if legacyTables != 0 {
		t.Fatalf("path_events should not be created, found %d", legacyTables)
	}
	var count int
	var status, proposed string
	if err := conn.QueryRowContext(context.Background(), `
SELECT COUNT(*), min(status), min(proposed_name)
FROM gpl_issues
WHERE src_id = $1`, issue.SrcID).Scan(&count, &status, &proposed); err != nil {
		t.Fatal(err)
	}
	if count != 1 || status != GPLIssueStatusPending || proposed != "clean.txt" {
		t.Fatalf("issue count=%d status=%q proposed=%q", count, status, proposed)
	}
}

func TestReviewCanUpdateSparseGPLIssue(t *testing.T) {
	database, err := Open(Options{Path: t.TempDir() + "/gpl-review.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	err = database.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			if err := w.BatchInsertGPLIssues([]GPLIssue{{
				SrcID: "src-1", Status: GPLIssueStatusPending,
				ProposedName: "clean.txt", UpdatedAt: 1,
			}}); err != nil {
				return err
			}
			return w.UpdateGPLIssueWithAction("src-1", GPLIssueStatusAccepted, "chosen.txt", "", "", 2)
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	conn, err := database.GetDB()
	if err != nil {
		t.Fatal(err)
	}
	var status, proposed string
	if err := conn.QueryRowContext(context.Background(),
		`SELECT status, proposed_name FROM gpl_issues WHERE src_id = 'src-1'`,
	).Scan(&status, &proposed); err != nil {
		t.Fatal(err)
	}
	if status != GPLIssueStatusAccepted || proposed != "chosen.txt" {
		t.Fatalf("status=%q proposed=%q", status, proposed)
	}
}
