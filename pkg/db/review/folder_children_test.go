// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"context"
	"fmt"
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func TestListFolderChildrenPageHasMoreNoCount(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/folder-children-hasmore.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	srcRoot := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/"), Path: "/", Name: "/",
		Type: db.NodeTypeFolder, Depth: 0, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusAlreadyExisted,
	}
	dstRoot := &db.NodeState{
		ID: db.DeterministicNodeID("DST", db.NodeTypeFolder, "/"), Path: "/", Name: "/",
		Type: db.NodeTypeFolder, Depth: 0, TraversalStatus: db.StatusSuccessful,
	}
	src := []*db.NodeState{
		{
			ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt"), Path: "/a.txt", ParentPath: "/", ParentID: srcRoot.ID, Name: "a.txt",
			Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
		},
		{
			ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/b.txt"), Path: "/b.txt", ParentPath: "/", ParentID: srcRoot.ID, Name: "b.txt",
			Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
		},
		{
			ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/c.txt"), Path: "/c.txt", ParentPath: "/", ParentID: srcRoot.ID, Name: "c.txt",
			Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
		},
	}
	dst := []*db.NodeState{
		dstRoot,
		{
			ID: db.DeterministicNodeID("DST", db.NodeTypeFile, "/orphan.txt"), Path: "/orphan.txt", ParentPath: "/", ParentID: dstRoot.ID, Name: "orphan.txt",
			Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful,
		},
	}
	seedReviewTree(t, database, append([]*db.NodeState{srcRoot}, src...), dst, nil)

	page, hasMore, err := ListFolderChildrenPage(database, ReviewFilter{ParentPath: "/"}, "path ASC", 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !hasMore {
		t.Fatal("hasMore=false want true")
	}
	if len(page) != 2 {
		t.Fatalf("page len=%d want 2, paths=%v", len(page), pathsOf(page))
	}

	rest, hasMoreRest, err := ListFolderChildrenPage(database, ReviewFilter{ParentPath: "/"}, "path ASC", 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if hasMoreRest {
		t.Fatal("hasMore=true want false on last page")
	}
	if len(rest) != 2 {
		t.Fatalf("rest len=%d want 2, paths=%v", len(rest), pathsOf(rest))
	}
}

func TestListFolderChildrenRecordsOpsWithoutBlocking(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/folder-ops-core.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	srcRoot := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/"), Path: "/", Name: "/",
		Type: db.NodeTypeFolder, Depth: 0, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusAlreadyExisted,
	}
	n := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt"), Path: "/a.txt", ParentPath: "/", ParentID: srcRoot.ID, Name: "a.txt",
		Type: db.NodeTypeFile, Depth: 1, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}
	seedReviewTree(t, database, []*db.NodeState{srcRoot, n}, nil, nil)

	held := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_ = database.RunWrite(context.Background(), func() error {
			close(held)
			<-release
			return nil
		})
	}()
	<-held
	listDone := make(chan error, 1)
	go func() {
		page, _, err := ListFolderChildrenPage(database, ReviewFilter{ParentPath: "/"}, "path ASC", 10, 0)
		if err == nil && len(page) != 1 {
			err = fmt.Errorf("folder page len=%d want 1", len(page))
		}
		listDone <- err
	}()
	select {
	case err := <-listDone:
		if err != nil {
			close(release)
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("folder page blocked on db write lock")
	}
	close(release)
}
