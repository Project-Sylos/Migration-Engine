// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
)

func TestFoldChildSizesNestedAndRoot(t *testing.T) {
	s := openFoldStore(t)
	putFolder(t, s, "root", "", "/", 0)
	putFolder(t, s, "a", "root", "/a", 1)
	putFile(t, s, "f", "a", "/a/f", 1, 10)
	if _, err := s.WriteSealBatchTrusted(nil, nil, []SealKidsReplace{{
		Side: SideSRC, ParentID: "a",
		Kids: []KidRecord{{ID: "f", Type: NodeTypeFile, Size: 10, Depth: 1}},
	}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteSealBatchTrusted(nil, nil, []SealKidsReplace{{
		Side: SideSRC, ParentID: "root",
		Kids: []KidRecord{{ID: "a", Type: NodeTypeFolder, Depth: 1}},
	}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.AddPending(SideSRC, PhaseTrav, 1, NodeTypeFolder, "a"); err != nil {
		t.Fatal(err)
	}
	if err := s.AddPending(SideSRC, PhaseTrav, 0, NodeTypeFolder, "root"); err != nil {
		t.Fatal(err)
	}
	if err := s.FoldChildSizes(SideSRC, FoldKindTrav, FoldHooks{}); err != nil {
		t.Fatal(err)
	}
	if got := mustChildSize(t, s, "a"); got != 10 {
		t.Fatalf("a child_size=%d want 10", got)
	}
	if got := mustChildSize(t, s, "root"); got != 10 {
		t.Fatalf("root child_size=%d want 10", got)
	}
	ids, err := s.ListSchedAtDepth(SideSRC, PhaseTrav, 1, NodeTypeFolder, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Fatalf("pend left after fold: %v", ids)
	}
}

func TestFoldRetryTouchesAncestorsNotSiblings(t *testing.T) {
	s := openFoldStore(t)
	putFolder(t, s, "root", "", "/", 0)
	putFolder(t, s, "a", "root", "/a", 1)
	putFolder(t, s, "b", "a", "/a/b", 2)
	putFolder(t, s, "sib", "a", "/a/sib", 2)
	putFile(t, s, "cfile", "b", "/a/b/c", 3, 7)
	if _, err := s.AssignChildSize(SideSRC, "sib", 99); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteSealBatchTrusted(nil, nil, []SealKidsReplace{{
		Side: SideSRC, ParentID: "b",
		Kids: []KidRecord{{ID: "cfile", Type: NodeTypeFile, Size: 7}},
	}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteSealBatchTrusted(nil, nil, []SealKidsReplace{{
		Side: SideSRC, ParentID: "a",
		Kids: []KidRecord{
			{ID: "b", Type: NodeTypeFolder, Depth: 2},
			{ID: "sib", Type: NodeTypeFolder, Depth: 2},
		},
	}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteSealBatchTrusted(nil, nil, []SealKidsReplace{{
		Side: SideSRC, ParentID: "root",
		Kids: []KidRecord{{ID: "a", Type: NodeTypeFolder, Depth: 1}},
	}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	// Only the retried folder is still on the frontier.
	if err := s.AddPending(SideSRC, PhaseTrav, 2, NodeTypeFolder, "b"); err != nil {
		t.Fatal(err)
	}
	if err := s.FoldChildSizes(SideSRC, FoldKindTrav, FoldHooks{}); err != nil {
		t.Fatal(err)
	}
	if got := mustChildSize(t, s, "b"); got != 7 {
		t.Fatalf("b=%d want 7", got)
	}
	if got := mustChildSize(t, s, "a"); got != 106 {
		t.Fatalf("a=%d want 106 (b's new size plus sibling's existing child_size)", got)
	}
	if got := mustChildSize(t, s, "sib"); got != 99 {
		t.Fatalf("sibling rewritten: %d", got)
	}
}

func TestFoldAssignNotAddOnReplay(t *testing.T) {
	s := openFoldStore(t)
	putFolder(t, s, "root", "", "/", 0)
	putFolder(t, s, "a", "root", "/a", 1)
	if _, err := s.WriteSealBatchTrusted(nil, nil, []SealKidsReplace{{
		Side: SideSRC, ParentID: "a",
		Kids: []KidRecord{{ID: "f", Type: NodeTypeFile, Size: 4}},
	}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteSealBatchTrusted(nil, nil, []SealKidsReplace{{
		Side: SideSRC, ParentID: "root",
		Kids: []KidRecord{{ID: "a", Type: NodeTypeFolder, Depth: 1}},
	}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AssignChildSize(SideSRC, "a", 4); err != nil {
		t.Fatal(err)
	}
	if err := s.AddPending(SideSRC, PhaseTrav, 1, NodeTypeFolder, "a"); err != nil {
		t.Fatal(err)
	}
	if err := s.writeSealedDepth(SideSRC, 2); err != nil {
		t.Fatal(err)
	}
	if err := s.FoldChildSizes(SideSRC, FoldKindTrav, FoldHooks{}); err != nil {
		t.Fatal(err)
	}
	if got := mustChildSize(t, s, "a"); got != 4 {
		t.Fatalf("replay doubled or cleared a: %d", got)
	}
	if got := mustChildSize(t, s, "root"); got != 4 {
		t.Fatalf("root not enqueued from skipped write: %d", got)
	}
}

func TestFoldResumeUsesPersistedNext(t *testing.T) {
	s := openFoldStore(t)
	putFolder(t, s, "root", "", "/", 0)
	putFolder(t, s, "a", "root", "/a", 1)
	if _, err := s.WriteSealBatchTrusted(nil, nil, []SealKidsReplace{{
		Side: SideSRC, ParentID: "root",
		Kids: []KidRecord{{ID: "a", Type: NodeTypeFolder, Depth: 1}},
	}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AssignChildSize(SideSRC, "a", 8); err != nil {
		t.Fatal(err)
	}
	if err := s.update(func(txn *badger.Txn) error {
		return txn.Set(foldNextKey(SideSRC, "root"), []byte{1})
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.writeSealedDepth(SideSRC, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.FoldChildSizes(SideSRC, FoldKindTrav, FoldHooks{}); err != nil {
		t.Fatal(err)
	}
	if got := mustChildSize(t, s, "a"); got != 8 {
		t.Fatalf("deeper rewritten: %d", got)
	}
	if got := mustChildSize(t, s, "root"); got != 8 {
		t.Fatalf("fold:next parent skipped: %d", got)
	}
}

func TestMergeKidTicketsKeepsSiblings(t *testing.T) {
	out, err := MergeKidTickets(nil, []KidTicket{
		{Side: SideDST, ParentID: "p", ParentDepth: 1, Kid: KidRecord{ID: "a", Type: NodeTypeFile, Size: 3}},
		{Side: SideDST, ParentID: "p", ParentDepth: 1, Kid: KidRecord{ID: "b", Type: NodeTypeFile, Size: 4}},
	}, func(side, parentID string) ([]KidRecord, error) {
		if side != SideDST || parentID != "p" {
			t.Fatalf("existing %s %s", side, parentID)
		}
		return []KidRecord{{ID: "a", Type: NodeTypeFile, Size: 1}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || !out[0].Ticket || out[0].TicketDepth != 1 {
		t.Fatalf("replace %+v", out)
	}
	if len(out[0].Kids) != 2 || out[0].Kids[0].Size != 3 || out[0].Kids[1].ID != "b" {
		t.Fatalf("kids %+v", out[0].Kids)
	}
}

func TestListPendingSkipsSuccessfulRetainedKey(t *testing.T) {
	s := openFoldStore(t)
	putFolder(t, s, "done", "", "/done", 1)
	if err := s.PutStatus(SideSRC, "done", StatusRecord{TraversalStatus: traversalStatusSuccessful}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddPending(SideSRC, PhaseTrav, 1, NodeTypeFolder, "done"); err != nil {
		t.Fatal(err)
	}
	ids, err := s.ListPending(SideSRC, PhaseTrav, 1, NodeTypeFolder, "", traversalStatusPending, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Fatalf("successful folder pulled: %v", ids)
	}
	left, err := s.ListSchedAtDepth(SideSRC, PhaseTrav, 1, NodeTypeFolder, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 1 {
		t.Fatalf("retained key missing: %v", left)
	}
}

func TestCopyFoldFromTickets(t *testing.T) {
	s := openFoldStore(t)
	if err := s.PutNode(SideDST, NodeRecord{ID: "dstroot", Path: "/", Type: NodeTypeFolder, Depth: 0}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutStatus(SideDST, "dstroot", StatusRecord{TraversalStatus: traversalStatusSuccessful}); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendKidTicket(SideDST, "dstroot", 0, KidRecord{
		ID: "f", Type: NodeTypeFile, Size: 15, Depth: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.FoldChildSizes(SideDST, FoldKindCopy, FoldHooks{}); err != nil {
		t.Fatal(err)
	}
	if got := mustChildSize(t, s, "dstroot"); got != 15 {
		t.Fatalf("dst child_size=%d want 15", got)
	}
}

func TestStatusWriteDoesNotClearChildSize(t *testing.T) {
	s := openFoldStore(t)
	putFolder(t, s, "a", "", "/a", 1)
	if _, err := s.AssignChildSize(SideSRC, "a", 20); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteSealBatch(nil, []SealStatusWrite{{
		Side: SideSRC, ID: "a", Depth: 1,
		Status:     StatusRecord{CopyStatus: copyStatusExcludedExplicit, TraversalStatus: traversalStatusSuccessful},
		PrevStatus: StatusRecord{CopyStatus: copyStatusPending, TraversalStatus: traversalStatusSuccessful},
	}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if got := mustChildSize(t, s, "a"); got != 20 {
		t.Fatalf("exclude cleared child_size: %d", got)
	}
	if _, err := s.WriteSealBatch(nil, []SealStatusWrite{{
		Side: SideSRC, ID: "a", Depth: 1,
		Status:     StatusRecord{DeleteStatus: deleteStatusDeleted, TraversalStatus: traversalStatusSuccessful, CopyStatus: copyStatusSuccessful},
		PrevStatus: StatusRecord{DeleteStatus: deleteStatusPendingExplicit, TraversalStatus: traversalStatusSuccessful, CopyStatus: copyStatusSuccessful},
	}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if got := mustChildSize(t, s, "a"); got != 20 {
		t.Fatalf("delete status cleared child_size: %d", got)
	}
}

func openFoldStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func putFolder(t *testing.T, s *Store, id, parent, path string, depth int) {
	t.Helper()
	if err := s.PutNode(SideSRC, NodeRecord{ID: id, ParentID: parent, Path: path, Type: NodeTypeFolder, Depth: depth}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutStatus(SideSRC, id, StatusRecord{TraversalStatus: traversalStatusSuccessful}); err != nil {
		t.Fatal(err)
	}
}

func mustChildSize(t *testing.T, s *Store, id string) int64 {
	t.Helper()
	st, ok, err := s.GetStatus(SideSRC, id)
	if err != nil || !ok {
		st, ok, err = s.GetStatus(SideDST, id)
	}
	if err != nil || !ok {
		t.Fatalf("status %s: ok=%v err=%v", id, ok, err)
	}
	return st.ChildSize
}

func putFile(t *testing.T, s *Store, id, parent, path string, depth int, size int64) {
	t.Helper()
	if err := s.PutNode(SideSRC, NodeRecord{ID: id, ParentID: parent, Path: path, Type: NodeTypeFile, Size: size, Depth: depth}); err != nil {
		t.Fatal(err)
	}
}
