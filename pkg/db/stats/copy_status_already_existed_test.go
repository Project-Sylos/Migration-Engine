// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package stats

import (
	"context"
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	_ "codeberg.org/Sylos/Migration-Engine/pkg/db/seal"
)

func TestGetCopyStatusCountsAlreadyExistedSeparateFromSuccessful(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/copy-already-existed.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	eventTime := time.Now().UnixNano()
	root := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/"), Path: "/", ParentPath: "", Name: "",
		Type: db.NodeTypeFolder, Depth: 0,
		TraversalStatus: db.StatusPending, CopyStatus: db.CopyStatusAlreadyExisted,
	}
	matched := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/both"), Path: "/both", ParentPath: "/", Name: "both",
		Type: db.NodeTypeFolder, Depth: 1,
		TraversalStatus: db.StatusPending, CopyStatus: db.CopyStatusAlreadyExisted,
	}
	pending := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/new.txt"), Path: "/new.txt", ParentPath: "/", Name: "new.txt",
		Type: db.NodeTypeFile, Depth: 1,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}
	copied := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/done.txt"), Path: "/done.txt", ParentPath: "/", Name: "done.txt",
		Type: db.NodeTypeFile, Depth: 1,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful,
	}

	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			nodes := []*db.NodeState{root, matched, pending, copied}
			if err := w.AppenderInsert(db.TableSrcNodes, nodes); err != nil {
				return err
			}
			events := make([]db.StatusEvent, 0, len(nodes))
			for _, n := range nodes {
				events = append(events, db.StatusEvent{
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

	counts, err := GetCopyStatusCountsFromEvents(database)
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

	snap, err := GetPathReviewStatsFromDB(database)
	if err != nil {
		t.Fatal(err)
	}
	if snap.CopySuccessful != 3 {
		t.Fatalf("review CopySuccessful=%d want 3 (folded complete)", snap.CopySuccessful)
	}
}

func TestCopyStatusHelpers(t *testing.T) {
	if !db.CopyStatusIsComplete(db.CopyStatusSuccessful) || !db.CopyStatusIsComplete(db.CopyStatusAlreadyExisted) {
		t.Fatal("complete helpers")
	}
	if db.CopyStatusIsComplete(db.CopyStatusPending) {
		t.Fatal("pending must not be complete")
	}
	if !db.CopyStatusIsActualCopy(db.CopyStatusSuccessful) || db.CopyStatusIsActualCopy(db.CopyStatusAlreadyExisted) {
		t.Fatal("actual copy helpers")
	}
	if db.DeletePendingIfCopySuccessful(db.CopyStatusAlreadyExisted, "") != db.DeleteStatusPending {
		t.Fatal("already_existed should seed delete pending")
	}
}
