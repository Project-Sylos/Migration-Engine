// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"testing"
)

func TestScanSubtreeIDsChunked(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	writes := []SealNodeWrite{
		{Side: SideSRC, Node: NodeRecord{ID: "a", Path: "/a", Name: "a", Type: NodeTypeFolder}, InsertOnly: true},
		{Side: SideSRC, Node: NodeRecord{ID: "b", Path: "/a/b", Name: "b", Type: NodeTypeFile, Size: 3, ParentID: "a"}, InsertOnly: true},
		{Side: SideSRC, Node: NodeRecord{ID: "c", Path: "/a/c", Name: "c", Type: NodeTypeFile, Size: 5, ParentID: "a"}, InsertOnly: true},
	}
	if _, err := s.WriteSealBatch(writes, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	var all []string
	after := ""
	for {
		ids, next, done, err := s.ScanSubtreeIDs(SideSRC, "/a", after, 1, SubtreeScanOpts{})
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, ids...)
		if done {
			break
		}
		after = next
	}
	if len(all) != 3 {
		t.Fatalf("chunked scan got %v", all)
	}
}

func TestScanSubtreeIDsExcludeRoot(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	writes := []SealNodeWrite{
		{Side: SideSRC, Node: NodeRecord{ID: "root", Path: "/folder", Name: "folder", Type: NodeTypeFolder}, InsertOnly: true},
		{Side: SideSRC, Node: NodeRecord{ID: "kid", Path: "/folder/kid", Name: "kid", Type: NodeTypeFile, ParentID: "root"}, InsertOnly: true},
	}
	if _, err := s.WriteSealBatch(writes, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	ids, _, done, err := s.ScanSubtreeIDs(SideSRC, "/folder", "", 10, SubtreeScanOpts{ExcludeRoot: true})
	if err != nil || !done || len(ids) != 1 || ids[0] != "kid" {
		t.Fatalf("exclude root ids=%v done=%v err=%v", ids, done, err)
	}
}

func TestPropagateCopyFailureUnderPath(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	parent := SealNodeWrite{
		Side:       SideSRC,
		Node:       NodeRecord{ID: "p", Path: "/p", Name: "p", Type: NodeTypeFolder},
		Status:     StatusRecord{TraversalStatus: "successful", CopyStatus: "pending"},
		Depth:      1,
		InsertOnly: true,
	}
	child := SealNodeWrite{
		Side:       SideSRC,
		Node:       NodeRecord{ID: "c", Path: "/p/c", Name: "c", Type: NodeTypeFile, Size: 1, ParentID: "p"},
		Status:     StatusRecord{TraversalStatus: "successful", CopyStatus: "pending"},
		Depth:      2,
		InsertOnly: true,
	}
	done := SealNodeWrite{
		Side:       SideSRC,
		Node:       NodeRecord{ID: "d", Path: "/p/d", Name: "d", Type: NodeTypeFile, Size: 1, ParentID: "p"},
		Status:     StatusRecord{TraversalStatus: "successful", CopyStatus: "successful"},
		Depth:      2,
		InsertOnly: true,
	}
	if _, err := s.WriteSealBatch([]SealNodeWrite{parent, child, done}, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	mut, err := s.PropagateCopyFailureUnderPath("/p")
	if err != nil {
		t.Fatal(err)
	}
	if mut.Affected != 1 {
		t.Fatalf("affected=%d want 1", mut.Affected)
	}
	st, ok, err := s.GetStatus(SideSRC, "c")
	if err != nil || !ok || st.CopyStatus != copyStatusFailed {
		t.Fatalf("child status %+v ok=%v err=%v", st, ok, err)
	}
	st, ok, err = s.GetStatus(SideSRC, "d")
	if err != nil || !ok || st.CopyStatus != copyStatusSuccessful {
		t.Fatalf("done child status %+v", st)
	}
}

func TestCascadeDeleteUnderPath(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	writes := []SealNodeWrite{
		{
			Side:       SideSRC,
			Node:       NodeRecord{ID: "p", Path: "/p", Name: "p", Type: NodeTypeFolder},
			Status:     StatusRecord{TraversalStatus: "successful", CopyStatus: copyStatusSuccessful, DeleteStatus: deleteStatusPendingExplicit},
			Depth:      1,
			InsertOnly: true,
		},
		{
			Side:       SideSRC,
			Node:       NodeRecord{ID: "c", Path: "/p/c", Name: "c", Type: NodeTypeFile, ParentID: "p"},
			Status:     StatusRecord{TraversalStatus: "successful", CopyStatus: copyStatusSuccessful, DeleteStatus: deleteStatusPendingInherited},
			Depth:      2,
			InsertOnly: true,
		},
	}
	if _, err := s.WriteSealBatch(writes, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	mut, err := s.CascadeDeleteUnderPath("/p")
	if err != nil {
		t.Fatal(err)
	}
	if mut.Affected != 1 {
		t.Fatalf("affected=%d want 1", mut.Affected)
	}
	st, ok, err := s.GetStatus(SideSRC, "c")
	if err != nil || !ok || st.DeleteStatus != deleteStatusDeleted {
		t.Fatalf("child delete %+v ok=%v err=%v", st, ok, err)
	}
}

func TestApplySubtreeCopyExclusionPendingOnly(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	writes := []SealNodeWrite{
		{
			Side: SideSRC, Node: NodeRecord{ID: "p", Path: "/p", Name: "p", Type: NodeTypeFolder, Depth: 1},
			Status: StatusRecord{CopyStatus: copyStatusPending}, Depth: 1, InsertOnly: true,
			Deltas: []PendingDelta{{Phase: PhaseCopy, NodeType: NodeTypeFolder, Add: true}},
		},
		{
			Side: SideSRC, Node: NodeRecord{ID: "c", Path: "/p/c", Name: "c", Type: NodeTypeFile, ParentID: "p", Size: 9, Depth: 2},
			Status: StatusRecord{CopyStatus: copyStatusSuccessful}, Depth: 2, InsertOnly: true,
		},
		{
			Side: SideSRC, Node: NodeRecord{ID: "d", Path: "/p/d", Name: "d", Type: NodeTypeFile, ParentID: "p", Size: 4, Depth: 2},
			Status: StatusRecord{CopyStatus: copyStatusPending}, Depth: 2, InsertOnly: true,
			Deltas: []PendingDelta{{Phase: PhaseCopy, NodeType: NodeTypeFile, Add: true}},
		},
	}
	if _, err := s.WriteSealBatch(writes, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	mut, err := s.ApplySubtreeCopyExclusion("/p", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if mut.Affected != 2 {
		t.Fatalf("affected=%d want 2", mut.Affected)
	}
	st, ok, err := s.GetStatus(SideSRC, "p")
	if err != nil || !ok || st.CopyStatus != copyStatusExcludedExplicit {
		t.Fatalf("parent status %+v ok=%v err=%v", st, ok, err)
	}
	st, ok, err = s.GetStatus(SideSRC, "d")
	if err != nil || !ok || st.CopyStatus != copyStatusExcludedInherited {
		t.Fatalf("pending child status %+v ok=%v err=%v", st, ok, err)
	}
	st, ok, err = s.GetStatus(SideSRC, "c")
	if err != nil || !ok || st.CopyStatus != copyStatusSuccessful {
		t.Fatalf("successful child should stay: %+v ok=%v err=%v", st, ok, err)
	}
	if ids, err := s.ListSchedAtDepth(SideSRC, PhaseCopy, 1, NodeTypeFolder, "", 10); err != nil || len(ids) != 0 {
		t.Fatalf("folder pend after exclude %+v err=%v", ids, err)
	}
	if ids, err := s.ListSchedAtDepth(SideSRC, PhaseCopy, 2, NodeTypeFile, "", 10); err != nil || len(ids) != 0 {
		t.Fatalf("file pend after exclude %+v err=%v", ids, err)
	}

	mut, err = s.ApplySubtreeCopyExclusion("/p", false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if mut.Affected != 2 {
		t.Fatalf("unexclude affected=%d want 2", mut.Affected)
	}
	st, ok, err = s.GetStatus(SideSRC, "p")
	if err != nil || !ok || st.CopyStatus != copyStatusPending {
		t.Fatalf("parent after unexclude %+v ok=%v err=%v", st, ok, err)
	}
	st, ok, err = s.GetStatus(SideSRC, "d")
	if err != nil || !ok || st.CopyStatus != copyStatusPending {
		t.Fatalf("child after unexclude %+v ok=%v err=%v", st, ok, err)
	}
	st, ok, err = s.GetStatus(SideSRC, "c")
	if err != nil || !ok || st.CopyStatus != copyStatusSuccessful {
		t.Fatalf("successful child after unexclude %+v ok=%v err=%v", st, ok, err)
	}
	if ids, err := s.ListSchedAtDepth(SideSRC, PhaseCopy, 1, NodeTypeFolder, "", 10); err != nil || len(ids) != 1 || ids[0] != "p" {
		t.Fatalf("folder pend after unexclude %+v err=%v", ids, err)
	}
	if ids, err := s.ListSchedAtDepth(SideSRC, PhaseCopy, 2, NodeTypeFile, "", 10); err != nil || len(ids) != 1 || ids[0] != "d" {
		t.Fatalf("file pend after unexclude %+v err=%v", ids, err)
	}
}
