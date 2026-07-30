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

func TestLatestFailureLogIDsByNodeIDsAndGetLogs(t *testing.T) {
	ev := db.StatusEvent{
		ID:              "node-a",
		TraversalStatus: db.StatusFailed,
		EventTime:       100,
		Depth:           1,
	}
	failurelog.AttachTaskFailureLog(&ev, "traversal", "src", "node-a", "/a", 2, "first error")

	ev2 := ev
	ev2.EventTime = 200
	ev2.ErrorLogID = ""
	ev2.ErrorLogMessage = ""
	failurelog.AttachTaskFailureLog(&ev2, "traversal", "src", "node-a", "/a", 3, "latest error")

	database, err := db.Open(db.Options{Path: t.TempDir() + "/read.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.BatchInsertSrcStatusEvents([]db.StatusEvent{ev, ev2}); err != nil {
				return err
			}
			return db.FlushFailureLogsFromEvents(w, []db.StatusEvent{ev, ev2}, "src")
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RebuildAllCurrent(); err != nil {
		t.Fatal(err)
	}

	byNode, err := failurelog.LatestFailureLogIDsByNodeIDs(context.Background(), database, db.TableSrcStatusEvents, []string{"node-a", "missing"})
	if err != nil {
		t.Fatal(err)
	}
	if len(byNode) != 1 {
		t.Fatalf("byNode=%v", byNode)
	}
	logID := byNode["node-a"]
	if logID != ev2.ErrorLogID {
		t.Fatalf("logID=%s want latest %s", logID, ev2.ErrorLogID)
	}

	logs, err := failurelog.GetFailureLogsByIDs(context.Background(), database, []string{logID})
	if err != nil {
		t.Fatal(err)
	}
	if logs[logID].Message != ev2.ErrorLogMessage {
		t.Fatalf("message=%q want %q", logs[logID].Message, ev2.ErrorLogMessage)
	}
	if logs[logID].Detail != "latest error" {
		t.Fatalf("detail=%q want latest error", logs[logID].Detail)
	}
	if failurelog.FailureLogDisplayText(logs[logID]) != "latest error" {
		t.Fatalf("display=%q", failurelog.FailureLogDisplayText(logs[logID]))
	}
}
