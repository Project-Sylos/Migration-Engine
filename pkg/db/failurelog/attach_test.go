// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package failurelog_test

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/failurelog"
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

	if err := db.FlushFailureLogEvent(database, ev, "src"); err != nil {
		t.Fatal(err)
	}

	logs, err := database.Ops().GetLogsByIDs([]string{ev.ErrorLogID})
	if err != nil {
		t.Fatal(err)
	}
	rec, ok := logs[ev.ErrorLogID]
	if !ok {
		t.Fatal("expected log in ops store")
	}
	if rec.Message != ev.ErrorLogMessage {
		t.Fatalf("message=%q want %q", rec.Message, ev.ErrorLogMessage)
	}
	if rec.Detail != "permission denied" {
		t.Fatalf("detail=%q want permission denied", rec.Detail)
	}
}
