// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"testing"
	"time"
)

func TestUpdateGPLIssueWithAction_Rename(t *testing.T) {
	database, err := Open(Options{Path: t.TempDir() + "/gpl-dst-action.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	if err := database.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			return w.UpdateGPLIssueWithAction(
				"src-1", GPLIssueStatusAccepted, "clean", "", DstActionRename, time.Now().UnixNano(),
			)
		})
	}); err != nil {
		t.Fatal(err)
	}

	conn, err := database.GetDB()
	if err != nil {
		t.Fatal(err)
	}
	var status, proposed, action string
	if err := conn.QueryRowContext(context.Background(),
		`SELECT status, COALESCE(proposed_name,''), COALESCE(dst_action,'') FROM gpl_issues WHERE src_id = 'src-1'`,
	).Scan(&status, &proposed, &action); err != nil {
		t.Fatal(err)
	}
	if status != GPLIssueStatusAccepted || proposed != "clean" || action != DstActionRename {
		t.Fatalf("got status=%q proposed=%q action=%q", status, proposed, action)
	}

	if err := database.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			return w.ClearGPLIssueDstAction("src-1")
		})
	}); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRowContext(context.Background(),
		`SELECT COALESCE(dst_action,'') FROM gpl_issues WHERE src_id = 'src-1'`,
	).Scan(&action); err != nil {
		t.Fatal(err)
	}
	if action != "" {
		t.Fatalf("dst_action after clear = %q", action)
	}
}
