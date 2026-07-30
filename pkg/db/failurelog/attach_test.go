// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package failurelog_test

import (
	"context"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/failurelog"
	_ "codeberg.org/Sylos/Migration-Engine/pkg/db/seal"
)

func TestAttachTaskFailureLogAndFlush(t *testing.T) {
	ev := db.StatusEvent{
		ID:              "node-1",
		TraversalStatus: db.StatusFailed,
		EventTime:       1,
		Depth:           2,
	}
	failurelog.AttachTaskFailureLog(&ev, "traversal", "src", "node-1", "/foo", 3, "permission denied")
	if ev.ErrorLogID == "" {
		t.Fatal("expected error log id")
	}
	if ev.ErrorLogMessage == "" {
		t.Fatal("expected error log message")
	}

	database, err := db.Open(db.Options{Path: t.TempDir() + "/test.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.BatchInsertSrcStatusEvents([]db.StatusEvent{ev}); err != nil {
				return err
			}
			return db.FlushFailureLogsFromEvents(w, []db.StatusEvent{ev}, "src")
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	conn, err := database.GetDB()
	if err != nil {
		t.Fatal(err)
	}
	var logID, message string
	err = conn.QueryRowContext(context.Background(),
		`SELECT id, message FROM logs WHERE id = $1`, ev.ErrorLogID,
	).Scan(&logID, &message)
	if err != nil {
		t.Fatalf("query log: %v", err)
	}
	if logID != ev.ErrorLogID {
		t.Fatalf("log id=%s want %s", logID, ev.ErrorLogID)
	}
	if message != ev.ErrorLogMessage {
		t.Fatalf("message=%q want %q", message, ev.ErrorLogMessage)
	}

	var detail string
	err = conn.QueryRowContext(context.Background(),
		`SELECT detail FROM logs WHERE id = $1`, ev.ErrorLogID,
	).Scan(&detail)
	if err != nil {
		t.Fatalf("query log detail: %v", err)
	}
	if detail != "permission denied" {
		t.Fatalf("detail=%q want permission denied", detail)
	}

	var storedLogID string
	err = conn.QueryRowContext(context.Background(),
		`SELECT error_log_id FROM src_status_events WHERE id = $1`, ev.ID,
	).Scan(&storedLogID)
	if err != nil {
		t.Fatalf("query status event: %v", err)
	}
	if storedLogID != ev.ErrorLogID {
		t.Fatalf("event error_log_id=%s want %s", storedLogID, ev.ErrorLogID)
	}
}

func TestMigrateStatusEventErrorLogID(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/migrate.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	conn, err := database.GetDB()
	if err != nil {
		t.Fatal(err)
	}
	var count int64
	err = conn.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'src_status_events' AND column_name = 'error_log_id'`,
	).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("src_status_events.error_log_id count=%d want 1", count)
	}
}
