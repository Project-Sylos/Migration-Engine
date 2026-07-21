// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"testing"
	"time"
)

func TestExcludeUnexcludeCouplesGPLIgnore(t *testing.T) {
	database, err := Open(Options{Path: t.TempDir() + "/exclude-gpl.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	folderID := DeterministicNodeID("SRC", NodeTypeFolder, "/folder")
	fileID := DeterministicNodeID("SRC", NodeTypeFile, "/folder/bad:name.txt")
	t0 := time.Now().UnixNano()

	if err := database.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			nodes := []*NodeState{
				{ID: folderID, Path: "/folder", ParentPath: "/", Name: "folder", Type: NodeTypeFolder, Depth: 1},
				{ID: fileID, Path: "/folder/bad:name.txt", ParentPath: "/folder", ParentID: folderID, Name: "bad:name.txt", Type: NodeTypeFile, Depth: 2},
			}
			if err := w.AppenderInsert(tableSrcNodes, nodes); err != nil {
				return err
			}
			return w.BatchInsertSrcStatusEvents([]StatusEvent{
				{ID: folderID, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusPending, GPLStatus: GPLStatusSuccessful, EventTime: t0, Depth: 1},
				{ID: fileID, TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusPending, GPLStatus: GPLStatusFailed, EventTime: t0, Depth: 2},
			})
		})
	}); err != nil {
		t.Fatal(err)
	}

	currentGPL := func(id string) string {
		t.Helper()
		conn, err := database.GetDB()
		if err != nil {
			t.Fatal(err)
		}
		var s string
		if err := conn.QueryRowContext(context.Background(), `
SELECT COALESCE(arg_max(gpl_status, event_time), '')
FROM src_status_events
WHERE id = $1 AND COALESCE(gpl_status, '') <> ''`, id).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}

	if err := database.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			if err := w.InsertExclusionEventsForSubtree("SRC", "/folder"); err != nil {
				return err
			}
			return w.InsertGPLIgnoredEventsForSubtree("SRC", "/folder")
		})
	}); err != nil {
		t.Fatal(err)
	}
	if got := currentGPL(fileID); got != GPLStatusIgnored {
		t.Fatalf("after exclude gpl=%q want ignored", got)
	}

	if err := database.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			if err := w.InsertUnexcludeEventsForSubtree("SRC", "/folder"); err != nil {
				return err
			}
			return w.InsertGPLRestoredEventsForSubtree("SRC", "/folder")
		})
	}); err != nil {
		t.Fatal(err)
	}
	if got := currentGPL(fileID); got != GPLStatusFailed {
		t.Fatalf("after unexclude gpl=%q want failed", got)
	}
	if got := currentGPL(folderID); got != GPLStatusSuccessful {
		t.Fatalf("after unexclude folder gpl=%q want successful", got)
	}
}
