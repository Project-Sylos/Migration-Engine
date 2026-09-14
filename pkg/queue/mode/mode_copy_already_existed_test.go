// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package mode

import (
	"context"
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func TestCompleteCopyTaskPersistsAlreadyExisted(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/already.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.SeedDiscoveredNodes([]db.InsertOperation{{
		QueueType: "SRC",
		Level:     1,
		Status:    db.StatusPending,
		State: &db.NodeState{
			ID: "n-ae", ServiceID: "s", Path: "/f.txt", ParentPath: "/", Name: "f.txt",
			Type: "file", Size: 10, MTime: "t0", Depth: 1, CopyStatus: db.CopyStatusPending,
		},
	}}); err != nil {
		t.Fatal(err)
	}

	q := queue.NewQueue("copy", 3, 1, nil, nil)
	q.SetDatabase(database)
	q.SetMode(queue.QueueModeCopy)
	task := &queue.TaskBase{
		ID:                    "n-ae",
		Round:                 1,
		CopyPass:              2,
		Type:                  queue.TaskTypeCopyFile,
		ProgressAlreadyExists: true,
		SrcParentDeleteStatus: db.DeleteStatusPendingExplicit,
		File: types.File{
			ServiceID: "s", DisplayName: "f.txt", LocationPath: "/f.txt",
			Size: 10, Type: types.NodeTypeFile, LastUpdated: "t0",
		},
	}
	q.AddInProgress(task.ID, task)
	q.BindLeaseOwner(task, "w1")

	CompleteCopyTask(q, task, time.Millisecond)
	_ = database.Flush(context.Background())

	st, ok, err := database.Ops().GetStatus(opsdb.SideSRC, "n-ae")
	if err != nil || !ok {
		t.Fatalf("get status: ok=%v err=%v", ok, err)
	}
	if st.CopyStatus != db.CopyStatusAlreadyExisted {
		t.Fatalf("want copy_status=%s, got %s", db.CopyStatusAlreadyExisted, st.CopyStatus)
	}
	if st.DeleteStatus != db.DeleteStatusPendingInherited {
		t.Fatalf("want delete_status=%s from prefetched parent, got %s", db.DeleteStatusPendingInherited, st.DeleteStatus)
	}
	copyIDs, err := database.Ops().ListSchedAtDepth(opsdb.SideSRC, opsdb.PhaseCopy, 1, db.NodeTypeFile, "", 10)
	if err != nil || len(copyIDs) != 0 {
		t.Fatalf("copy pend after already_existed %+v err=%v", copyIDs, err)
	}
}

func TestCompleteCopyTaskDropsCopyFrontierWhenPrevAlreadyExisted(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/ae-stale-prev.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.SeedDiscoveredNodes([]db.InsertOperation{{
		QueueType: "SRC",
		Level:     1,
		Status:    db.StatusSuccessful,
		State: &db.NodeState{
			ID: "n-stale", ServiceID: "s", Path: "/a", ParentPath: "/", Name: "a",
			Type: db.NodeTypeFolder, Depth: 1, CopyStatus: db.CopyStatusPending,
			TraversalStatus: db.StatusSuccessful,
		},
	}}); err != nil {
		t.Fatal(err)
	}

	q := queue.NewQueue("copy", 3, 1, nil, nil)
	q.SetDatabase(database)
	q.SetMode(queue.QueueModeCopy)
	q.SetCopyPass(1)
	task := &queue.TaskBase{
		ID:                    "n-stale",
		Round:                 1,
		CopyPass:              1,
		Type:                  queue.TaskTypeCopyFolder,
		CopyStatus:            db.CopyStatusAlreadyExisted,
		SrcTraversalStatus:    db.StatusSuccessful,
		ProgressAlreadyExists: true,
		Folder: types.Folder{
			ServiceID: "s", DisplayName: "a", LocationPath: "/a",
			Type: types.NodeTypeFolder,
		},
	}
	q.AddInProgress(task.ID, task)
	q.BindLeaseOwner(task, "w1")

	CompleteCopyTask(q, task, time.Millisecond)
	_ = database.Flush(context.Background())

	copyIDs, err := database.Ops().ListSchedAtDepth(opsdb.SideSRC, opsdb.PhaseCopy, 1, db.NodeTypeFolder, "", 10)
	if err != nil || len(copyIDs) != 0 {
		t.Fatalf("copy pend after stale prev complete %+v err=%v", copyIDs, err)
	}
}
