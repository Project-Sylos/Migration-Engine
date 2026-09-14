// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"path/filepath"
	"testing"
)

func TestApplyTraversalRetryMarkEnrollsFolderFrontier(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: filepath.Join(dir, "ops")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	folderID := "folder1"
	childID := "child1"
	writes := []SealNodeWrite{
		{
			Side: SideSRC,
			Node: NodeRecord{ID: folderID, Path: "/locked", Name: "locked", Type: NodeTypeFolder, Depth: 1},
			Status: StatusRecord{
				TraversalStatus: traversalStatusFailed,
				CopyStatus:      copyStatusPending,
			},
			Depth:      1,
			InsertOnly: true,
		},
		{
			Side: SideSRC,
			Node: NodeRecord{ID: childID, Path: "/locked/a", Name: "a", Type: NodeTypeFolder, Depth: 2, ParentID: folderID},
			Status: StatusRecord{
				TraversalStatus: traversalStatusFailed,
				CopyStatus:      copyStatusPending,
			},
			Depth:      2,
			InsertOnly: true,
		},
	}
	if _, err := s.WriteSealBatch(writes, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	res, err := s.ApplyTraversalRetryMark(folderID, "")
	if err != nil {
		t.Fatal(err)
	}
	if res.Affected != 2 || res.FromFailed != 2 {
		t.Fatalf("res=%+v", res)
	}
	for _, id := range []string{folderID, childID} {
		st, ok, err := s.GetStatus(SideSRC, id)
		if err != nil || !ok {
			t.Fatal(err)
		}
		if st.TraversalStatus != traversalStatusPending {
			t.Fatalf("%s status=%q want pending", id, st.TraversalStatus)
		}
	}
	ids, err := s.ListSchedAtDepth(SideSRC, PhaseTrav, 1, NodeTypeFolder, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, id := range ids {
		if id == folderID {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected folder on pend:trav after mark")
	}
	ids2, err := s.ListSchedAtDepth(SideSRC, PhaseTrav, 2, NodeTypeFolder, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	foundChild := false
	for _, id := range ids2 {
		if id == childID {
			foundChild = true
			break
		}
	}
	if !foundChild {
		t.Fatal("expected child folder on pend:trav after path-prefix mark")
	}

	ures, err := s.ApplyTraversalRetryUnmark(folderID, "")
	if err != nil {
		t.Fatal(err)
	}
	if ures.Affected != 2 || ures.FromFailed != 2 {
		t.Fatalf("unmark=%+v", ures)
	}
	st, _, _ := s.GetStatus(SideSRC, folderID)
	if st.TraversalStatus != traversalStatusFailed {
		t.Fatalf("after unmark status=%q want failed", st.TraversalStatus)
	}
	ids, _ = s.ListSchedAtDepth(SideSRC, PhaseTrav, 1, NodeTypeFolder, "", 10)
	for _, id := range ids {
		if id == folderID {
			t.Fatal("folder should not remain on pend:trav after unmark")
		}
	}
}
