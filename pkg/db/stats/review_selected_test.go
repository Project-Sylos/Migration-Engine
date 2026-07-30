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

func TestOverlayReviewSelected_copyEligibleVsPending(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/overlay-copy.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	eventTime := time.Now().UnixNano()
	folderPending := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/dir"), Path: "/dir", ParentPath: "/", Name: "dir",
		Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}
	filePending := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt"), Path: "/a.txt", ParentPath: "/", Name: "a.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 100,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}
	fileOK := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/b.txt"), Path: "/b.txt", ParentPath: "/", Name: "b.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 250,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful,
	}
	fileFailed := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/c.txt"), Path: "/c.txt", ParentPath: "/", Name: "c.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 50,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusFailed,
	}
	fileExcluded := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/d.txt"), Path: "/d.txt", ParentPath: "/", Name: "d.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 999,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusExcludedExplicit,
	}

	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			nodes := []*db.NodeState{folderPending, filePending, fileOK, fileFailed, fileExcluded}
			if err := w.AppenderInsert(db.TableSrcNodes, nodes); err != nil {
				return err
			}
			events := make([]db.StatusEvent, 0, len(nodes))
			for _, n := range nodes {
				events = append(events, db.StatusEvent{
					ID: n.ID, TraversalStatus: n.TraversalStatus, CopyStatus: n.CopyStatus,
					EventTime: eventTime, Depth: 1,
				})
			}
			return w.BatchInsertSrcStatusEvents(events)
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	pending, err := OverlayReviewSelected(database, ReviewSelectedSpec{Kind: db.StatsKindCopy, Population: SelectedPending})
	if err != nil {
		t.Fatal(err)
	}
	if pending.Folders != 1 || pending.Files != 1 || pending.SelectedBytes != 100 {
		t.Fatalf("pending overlay: %+v want folders=1 files=1 bytes=100", pending)
	}

	eligible, err := OverlayReviewSelected(database, ReviewSelectedSpec{Kind: db.StatsKindCopy, Population: SelectedEligible})
	if err != nil {
		t.Fatal(err)
	}
	if eligible.Folders != 1 || eligible.Files != 3 || eligible.SelectedBytes != 400 {
		t.Fatalf("eligible overlay: %+v want folders=1 files=3 bytes=400", eligible)
	}
}

func TestOverlayReviewSelected_deletePending(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/overlay-delete.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	eventTime := time.Now().UnixNano()
	folderPending := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/dir"), Path: "/dir", ParentPath: "/", Name: "dir",
		Type: db.NodeTypeFolder, Depth: 1,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPending,
	}
	filePending := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt"), Path: "/a.txt", ParentPath: "/", Name: "a.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 100,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusPending,
	}
	fileDeleted := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/b.txt"), Path: "/b.txt", ParentPath: "/", Name: "b.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 250,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusDeleted,
	}
	fileSkipped := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/c.txt"), Path: "/c.txt", ParentPath: "/", Name: "c.txt",
		Type: db.NodeTypeFile, Depth: 1, Size: 50,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, DeleteStatus: db.DeleteStatusSkipped,
	}

	err = database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			nodes := []*db.NodeState{folderPending, filePending, fileDeleted, fileSkipped}
			if err := w.AppenderInsert(db.TableSrcNodes, nodes); err != nil {
				return err
			}
			events := make([]db.StatusEvent, 0, len(nodes))
			for _, n := range nodes {
				events = append(events, db.StatusEvent{
					ID: n.ID, TraversalStatus: n.TraversalStatus, CopyStatus: n.CopyStatus,
					DeleteStatus: n.DeleteStatus, EventTime: eventTime, Depth: 1,
				})
			}
			return w.BatchInsertSrcStatusEvents(events)
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	pending, err := OverlayReviewSelected(database, ReviewSelectedSpec{Kind: db.StatsKindDelete, Population: SelectedPending})
	if err != nil {
		t.Fatal(err)
	}
	if pending.Folders != 1 || pending.Files != 1 || pending.SelectedBytes != 100 {
		t.Fatalf("delete pending overlay: %+v want folders=1 files=1 bytes=100", pending)
	}
}
