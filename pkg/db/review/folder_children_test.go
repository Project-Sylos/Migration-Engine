// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"testing"

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

func TestListFolderChildrenPage_MergedFolderShowsDSTOnlyPeers(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/merged-folder-children.db"})
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
	srcFolder := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/Extra space "), Path: "/src-extra", ParentPath: "/", ParentID: srcRoot.ID,
		Name: "Extra space ", Type: db.NodeTypeFolder, Depth: 1,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusAlreadyExisted,
	}
	dstFolder := &db.NodeState{
		ID: db.DeterministicNodeID("DST", db.NodeTypeFolder, "/Extra Space"), Path: "/dst-extra", ParentPath: "/", ParentID: dstRoot.ID,
		Name: "Extra Space", Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful,
	}
	srcChild := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/Extra space /src-only.txt"), Path: "/src-extra/src-only.txt",
		ParentPath: "/src-extra", ParentID: srcFolder.ID, Name: "src-only.txt",
		Type: db.NodeTypeFile, Depth: 2, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}
	dstChild := &db.NodeState{
		ID: db.DeterministicNodeID("DST", db.NodeTypeFile, "/Extra Space/dst-only.txt"), Path: "/dst-extra/dst-only.txt",
		ParentPath: "/dst-extra", ParentID: dstFolder.ID, Name: "dst-only.txt",
		Type: db.NodeTypeFile, Depth: 2, TraversalStatus: db.StatusSuccessful,
		CopyStatus: db.CopyStatusDestinationOnly,
	}
	pairedSrc := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/Extra space /shared.txt"), Path: "/src-extra/shared.txt",
		ParentPath: "/src-extra", ParentID: srcFolder.ID, Name: "shared.txt",
		Type: db.NodeTypeFile, Depth: 2, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusAlreadyExisted,
	}
	pairedDst := &db.NodeState{
		ID: db.DeterministicNodeID("DST", db.NodeTypeFile, "/Extra Space/shared.txt"), Path: "/dst-extra/shared.txt",
		ParentPath: "/dst-extra", ParentID: dstFolder.ID, Name: "shared.txt",
		Type: db.NodeTypeFile, Depth: 2, TraversalStatus: db.StatusSuccessful,
	}

	seedReviewTree(t, database,
		[]*db.NodeState{srcRoot, srcFolder, srcChild, pairedSrc},
		[]*db.NodeState{dstRoot, dstFolder, dstChild, pairedDst},
		[]db.IDMapEvent{
			{SrcInternalID: srcFolder.ID, DstInternalID: dstFolder.ID, Status: db.IDMapStatusActive},
			{SrcInternalID: pairedSrc.ID, DstInternalID: pairedDst.ID, Status: db.IDMapStatusActive},
		},
	)

	page, _, err := ListFolderChildrenPage(database, ReviewFilter{ParentPath: "/src-extra"}, "path ASC", 20, 0)
	if err != nil {
		t.Fatal(err)
	}
	var sawSrcOnly, sawDstOnly, sawPaired bool
	for _, row := range page {
		switch {
		case row.SrcNodeID == srcChild.ID && row.DstNodeID == "":
			sawSrcOnly = true
		case row.SrcNodeID == "" && row.DstNodeID == dstChild.ID:
			sawDstOnly = true
		case row.SrcNodeID == pairedSrc.ID && row.DstNodeID == pairedDst.ID:
			sawPaired = true
		}
	}
	if !sawSrcOnly || !sawDstOnly || !sawPaired {
		t.Fatalf("merged browse want src-only+dst-only+paired; got paths=%v srcOnly=%v dstOnly=%v paired=%v",
			pathsOf(page), sawSrcOnly, sawDstOnly, sawPaired)
	}
}

func TestListFolderChildrenPage_SideSrcAndDst(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/side-folder-children.db"})
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
	srcFolder := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/A"), Path: "/src-a", ParentPath: "/", ParentID: srcRoot.ID,
		Name: "A", Type: db.NodeTypeFolder, Depth: 1,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusAlreadyExisted,
	}
	dstFolder := &db.NodeState{
		ID: db.DeterministicNodeID("DST", db.NodeTypeFolder, "/A"), Path: "/dst-a", ParentPath: "/", ParentID: dstRoot.ID,
		Name: "A", Type: db.NodeTypeFolder, Depth: 1, TraversalStatus: db.StatusSuccessful,
	}
	srcOnly := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/A/s.txt"), Path: "/src-a/s.txt",
		ParentPath: "/src-a", ParentID: srcFolder.ID, Name: "s.txt",
		Type: db.NodeTypeFile, Depth: 2, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}
	dstOnly := &db.NodeState{
		ID: db.DeterministicNodeID("DST", db.NodeTypeFile, "/A/d.txt"), Path: "/dst-a/d.txt",
		ParentPath: "/dst-a", ParentID: dstFolder.ID, Name: "d.txt",
		Type: db.NodeTypeFile, Depth: 2, TraversalStatus: db.StatusSuccessful,
		CopyStatus: db.CopyStatusDestinationOnly,
	}
	pairedSrc := &db.NodeState{
		ID: db.DeterministicNodeID("SRC", db.NodeTypeFile, "/A/p.txt"), Path: "/src-a/p.txt",
		ParentPath: "/src-a", ParentID: srcFolder.ID, Name: "p.txt",
		Type: db.NodeTypeFile, Depth: 2, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusAlreadyExisted,
	}
	pairedDst := &db.NodeState{
		ID: db.DeterministicNodeID("DST", db.NodeTypeFile, "/A/p.txt"), Path: "/dst-a/p.txt",
		ParentPath: "/dst-a", ParentID: dstFolder.ID, Name: "p.txt",
		Type: db.NodeTypeFile, Depth: 2, TraversalStatus: db.StatusSuccessful,
	}
	seedReviewTree(t, database,
		[]*db.NodeState{srcRoot, srcFolder, srcOnly, pairedSrc},
		[]*db.NodeState{dstRoot, dstFolder, dstOnly, pairedDst},
		[]db.IDMapEvent{
			{SrcInternalID: srcFolder.ID, DstInternalID: dstFolder.ID, Status: db.IDMapStatusActive},
			{SrcInternalID: pairedSrc.ID, DstInternalID: pairedDst.ID, Status: db.IDMapStatusActive},
		},
	)

	srcPage, _, err := ListFolderChildrenPage(database, ReviewFilter{ParentPath: "/src-a", Side: "src"}, "path ASC", 20, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(srcPage) != 2 {
		t.Fatalf("side=src want 2 rows (no dst-only), got %d paths=%v", len(srcPage), pathsOf(srcPage))
	}
	for _, row := range srcPage {
		if row.SrcNodeID == "" {
			t.Fatalf("side=src unexpected dst-only row %#v", row)
		}
	}

	dstPage, _, err := ListFolderChildrenPage(database, ReviewFilter{ParentPath: "/dst-a", Side: "dst"}, "path ASC", 20, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(dstPage) != 2 {
		t.Fatalf("side=dst want 2 rows (dst-only+paired), got %d paths=%v", len(dstPage), pathsOf(dstPage))
	}
	var sawDstOnly, sawPaired bool
	for _, row := range dstPage {
		if row.DstPath == "" {
			t.Fatalf("side=dst missing DstPath %#v", row)
		}
		if row.DstNodeID == dstOnly.ID && row.SrcNodeID == "" {
			sawDstOnly = true
		}
		if row.DstNodeID == pairedDst.ID && row.SrcNodeID == pairedSrc.ID {
			sawPaired = true
			if row.SrcPath != pairedSrc.Path {
				t.Fatalf("paired SrcPath=%q want %q", row.SrcPath, pairedSrc.Path)
			}
		}
	}
	if !sawDstOnly || !sawPaired {
		t.Fatalf("side=dst want dst-only+paired; dstOnly=%v paired=%v paths=%v", sawDstOnly, sawPaired, pathsOf(dstPage))
	}
}
