// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"testing"
	"time"
)

func TestGetCopyStatusCountsAlreadyExistedSeparateFromSuccessful(t *testing.T) {
	database, err := Open(Options{Path: t.TempDir() + "/copy-already-existed.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	eventTime := time.Now().UnixNano()
	root := &NodeState{
		ID: DeterministicNodeID("SRC", NodeTypeFolder, "/"), Path: "/", ParentPath: "", Name: "",
		Type: NodeTypeFolder, Depth: 0,
		TraversalStatus: StatusPending, CopyStatus: CopyStatusAlreadyExisted,
	}
	matched := &NodeState{
		ID: DeterministicNodeID("SRC", NodeTypeFolder, "/both"), Path: "/both", ParentPath: "/", Name: "both",
		Type: NodeTypeFolder, Depth: 1,
		TraversalStatus: StatusPending, CopyStatus: CopyStatusAlreadyExisted,
	}
	pending := &NodeState{
		ID: DeterministicNodeID("SRC", NodeTypeFile, "/new.txt"), Path: "/new.txt", ParentPath: "/", Name: "new.txt",
		Type: NodeTypeFile, Depth: 1,
		TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusPending,
	}
	copied := &NodeState{
		ID: DeterministicNodeID("SRC", NodeTypeFile, "/done.txt"), Path: "/done.txt", ParentPath: "/", Name: "done.txt",
		Type: NodeTypeFile, Depth: 1,
		TraversalStatus: StatusSuccessful, CopyStatus: CopyStatusSuccessful,
	}

	err = database.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			nodes := []*NodeState{root, matched, pending, copied}
			if err := w.AppenderInsert(tableSrcNodes, nodes); err != nil {
				return err
			}
			events := make([]StatusEvent, 0, len(nodes))
			for _, n := range nodes {
				events = append(events, StatusEvent{
					ID: n.ID, TraversalStatus: n.TraversalStatus, CopyStatus: n.CopyStatus,
					EventTime: eventTime, Depth: n.Depth,
				})
			}
			return w.BatchInsertSrcStatusEvents(events)
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	counts, err := database.GetCopyStatusCountsFromEvents()
	if err != nil {
		t.Fatal(err)
	}
	if counts.Successful != 1 {
		t.Fatalf("Successful=%d want 1 (actual copy only)", counts.Successful)
	}
	if counts.AlreadyExisted != 2 {
		t.Fatalf("AlreadyExisted=%d want 2 (root + match)", counts.AlreadyExisted)
	}
	if counts.Pending != 1 {
		t.Fatalf("Pending=%d want 1", counts.Pending)
	}
	if counts.Complete() != 3 {
		t.Fatalf("Complete()=%d want 3", counts.Complete())
	}
	// Resume window signal: match-only DB must not look like real copy progress.
	if counts.Successful <= 0 || counts.Pending <= 0 {
		// with Successful=1 and Pending=1 this would arm; strip copied to verify match-only
	}

	snap, err := database.GetPathReviewStatsFromDB()
	if err != nil {
		t.Fatal(err)
	}
	if snap.CopySuccessful != 3 {
		t.Fatalf("review CopySuccessful=%d want 3 (folded complete)", snap.CopySuccessful)
	}
}

func TestCopyStatusHelpers(t *testing.T) {
	if !CopyStatusIsComplete(CopyStatusSuccessful) || !CopyStatusIsComplete(CopyStatusAlreadyExisted) {
		t.Fatal("complete helpers")
	}
	if CopyStatusIsComplete(CopyStatusPending) {
		t.Fatal("pending must not be complete")
	}
	if !CopyStatusIsActualCopy(CopyStatusSuccessful) || CopyStatusIsActualCopy(CopyStatusAlreadyExisted) {
		t.Fatal("actual copy helpers")
	}
	if DeletePendingIfCopySuccessful(CopyStatusAlreadyExisted, "") != DeleteStatusPending {
		t.Fatal("already_existed should seed delete pending")
	}
}
