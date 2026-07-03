// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"testing"
)

func TestLatestFailureLogIDsByNodeIDsAndGetLogs(t *testing.T) {
	ev := StatusEvent{
		ID:              "node-a",
		TraversalStatus: StatusFailed,
		EventTime:       100,
		Depth:           1,
	}
	AttachTaskFailureLog(&ev, "traversal", "src", "node-a", "/a", 2, "first error")

	ev2 := ev
	ev2.EventTime = 200
	ev2.ErrorLogID = ""
	ev2.ErrorLogMessage = ""
	AttachTaskFailureLog(&ev2, "traversal", "src", "node-a", "/a", 3, "latest error")

	database, err := Open(Options{Path: t.TempDir() + "/read.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	err = database.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			if err := w.BatchInsertSrcStatusEvents([]StatusEvent{ev, ev2}); err != nil {
				return err
			}
			return flushFailureLogsFromEvents(w, []StatusEvent{ev, ev2}, "src")
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	byNode, err := LatestSrcFailureLogIDsByNodeIDs(context.Background(), database, []string{"node-a", "missing"})
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

	logs, err := GetFailureLogsByIDs(context.Background(), database, []string{logID})
	if err != nil {
		t.Fatal(err)
	}
	if logs[logID].Message != ev2.ErrorLogMessage {
		t.Fatalf("message=%q want %q", logs[logID].Message, ev2.ErrorLogMessage)
	}
	if logs[logID].Detail != "latest error" {
		t.Fatalf("detail=%q want latest error", logs[logID].Detail)
	}
	if FailureLogDisplayText(logs[logID]) != "latest error" {
		t.Fatalf("display=%q", FailureLogDisplayText(logs[logID]))
	}
}
