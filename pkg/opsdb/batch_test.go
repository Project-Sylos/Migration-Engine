// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"fmt"
	"testing"
)

const (
	testStatusPending  = "pending"
	testNodeTypeFile   = "file"
	testNodeTypeFolder = "folder"
)

func testID(side, parent, typ, name string) string {
	if parent == "" {
		return fmt.Sprintf("%s-%s-%s", side, typ, name)
	}
	return fmt.Sprintf("%s-%s-%s-%s", side, parent, typ, name)
}

func BenchmarkBatchGetStatus(b *testing.B) {
	dir := b.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	const n = 1000
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		id := testID("SRC", "", testNodeTypeFile, fmt.Sprintf("f%d", i))
		ids[i] = id
		if err := s.PutStatus(SideSRC, id, StatusRecord{TraversalStatus: testStatusPending, CopyStatus: testStatusPending}); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for b.Loop() {
		if _, err := s.BatchGetStatus(SideSRC, ids); err != nil {
			b.Fatal(err)
		}
	}
}

func TestBatchGetStatus(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	id := testID("SRC", "", testNodeTypeFile, "bench")
	if err := s.PutStatus(SideSRC, id, StatusRecord{CopyStatus: testStatusPending}); err != nil {
		t.Fatal(err)
	}
	got, err := s.BatchGetStatus(SideSRC, []string{id, "missing"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 got %d", len(got))
	}
	if got[id].CopyStatus != testStatusPending {
		t.Fatalf("copy status %q", got[id].CopyStatus)
	}
}

func TestSchedTypePrefixesIsolated(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	folderID := testID("SRC", "", testNodeTypeFolder, "dir")
	fileID := testID("SRC", "", testNodeTypeFile, "blob")
	if err := s.AddPending(SideSRC, PhaseCopy, 2, testNodeTypeFolder, folderID); err != nil {
		t.Fatal(err)
	}
	if err := s.AddPending(SideSRC, PhaseCopy, 2, testNodeTypeFile, fileID); err != nil {
		t.Fatal(err)
	}
	folders, err := s.ListSchedAtDepth(SideSRC, PhaseCopy, 2, testNodeTypeFolder, "", 10)
	if err != nil || len(folders) != 1 || folders[0] != folderID {
		t.Fatalf("folder sched %+v err=%v", folders, err)
	}
	files, err := s.ListSchedAtDepth(SideSRC, PhaseCopy, 2, testNodeTypeFile, "", 10)
	if err != nil || len(files) != 1 || files[0] != fileID {
		t.Fatalf("file sched %+v err=%v", files, err)
	}
	fn, err := s.GetSchedCountAtDepth(SideSRC, PhaseCopy, 2, testNodeTypeFolder)
	if err != nil || fn != 1 {
		t.Fatalf("folder count=%d err=%v", fn, err)
	}
	en, err := s.GetSchedCountAtDepth(SideSRC, PhaseCopy, 2, testNodeTypeFile)
	if err != nil || en != 1 {
		t.Fatalf("file count=%d err=%v", en, err)
	}
	if err := s.DropPendingPrefix(SideSRC, PhaseCopy, 2, testNodeTypeFolder); err != nil {
		t.Fatal(err)
	}
	if n, err := s.GetSchedCountAtDepth(SideSRC, PhaseCopy, 2, testNodeTypeFolder); err != nil || n != 0 {
		t.Fatalf("folder after drop count=%d err=%v", n, err)
	}
	files, err = s.ListSchedAtDepth(SideSRC, PhaseCopy, 2, testNodeTypeFile, "", 10)
	if err != nil || len(files) != 1 || files[0] != fileID {
		t.Fatalf("file prefix survived folder drop: %+v err=%v", files, err)
	}
}

func TestListChildrenAndPending(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	parent := testID("SRC", "", testNodeTypeFolder, "root")
	child := testID("SRC", parent, testNodeTypeFile, "a")
	if err := s.PutNode(SideSRC, NodeRecord{ID: parent, Type: testNodeTypeFolder, Path: "/"}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutNode(SideSRC, NodeRecord{ID: child, ParentID: parent, Type: testNodeTypeFile, Path: "/a"}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutStatus(SideSRC, child, StatusRecord{TraversalStatus: testStatusPending}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddPending(SideSRC, PhaseTrav, 1, testNodeTypeFile, child); err != nil {
		t.Fatal(err)
	}

	children, err := s.ListChildren(SideSRC, parent, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 || children[0] != child {
		t.Fatalf("children %+v", children)
	}

	pending, err := s.ListPending(SideSRC, PhaseTrav, 1, testNodeTypeFile, "", testStatusPending, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0] != child {
		t.Fatalf("pending %+v", pending)
	}
}

func TestSchedPendingLifecycle(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	id := testID("SRC", "", testNodeTypeFolder, "f")
	if err := s.AddPending(SideSRC, PhaseTrav, 2, testNodeTypeFolder, id); err != nil {
		t.Fatal(err)
	}
	ids, err := s.ListSchedAtDepth(SideSRC, PhaseTrav, 2, testNodeTypeFolder, "", 10)
	if err != nil || len(ids) != 1 {
		t.Fatalf("list sched %+v err=%v", ids, err)
	}
	if err := s.DeletePending(SideSRC, PhaseTrav, 2, testNodeTypeFolder, id); err != nil {
		t.Fatal(err)
	}
	ids, err = s.ListSchedAtDepth(SideSRC, PhaseTrav, 2, testNodeTypeFolder, "", 10)
	if err != nil || len(ids) != 0 {
		t.Fatalf("after delete want empty got %+v err=%v", ids, err)
	}
	n, err := s.GetSchedCountAtDepth(SideSRC, PhaseTrav, 2, testNodeTypeFolder)
	if err != nil || n != 0 {
		t.Fatalf("count=%d err=%v", n, err)
	}
}

func TestSchedCountDeltaOnAddDelete(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	id := testID("SRC", "", testNodeTypeFolder, "c")
	if n, err := s.GetSchedCountAtDepth(SideSRC, PhaseTrav, 3, testNodeTypeFolder); err != nil || n != 0 {
		t.Fatalf("initial count=%d err=%v", n, err)
	}
	if err := s.AddPending(SideSRC, PhaseTrav, 3, testNodeTypeFolder, id); err != nil {
		t.Fatal(err)
	}
	if n, err := s.GetSchedCountAtDepth(SideSRC, PhaseTrav, 3, testNodeTypeFolder); err != nil || n != 1 {
		t.Fatalf("after add count=%d err=%v", n, err)
	}
	if err := s.AddPending(SideSRC, PhaseTrav, 3, testNodeTypeFolder, id); err != nil {
		t.Fatal(err)
	}
	if n, err := s.GetSchedCountAtDepth(SideSRC, PhaseTrav, 3, testNodeTypeFolder); err != nil || n != 1 {
		t.Fatalf("re-add must not double count: count=%d err=%v", n, err)
	}
	if err := s.DeletePending(SideSRC, PhaseTrav, 3, testNodeTypeFolder, id); err != nil {
		t.Fatal(err)
	}
	if n, err := s.GetSchedCountAtDepth(SideSRC, PhaseTrav, 3, testNodeTypeFolder); err != nil || n != 0 {
		t.Fatalf("after delete count=%d err=%v", n, err)
	}
}

func TestSchedCountZeroOnDropPrefix(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for i := 0; i < 5; i++ {
		id := testID("SRC", "", testNodeTypeFolder, fmt.Sprintf("d%d", i))
		if err := s.AddPending(SideSRC, PhaseCopy, 4, testNodeTypeFolder, id); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := s.GetSchedCountAtDepth(SideSRC, PhaseCopy, 4, testNodeTypeFolder); err != nil || n != 5 {
		t.Fatalf("count=%d err=%v", n, err)
	}
	if err := s.DropPendingPrefix(SideSRC, PhaseCopy, 4, testNodeTypeFolder); err != nil {
		t.Fatal(err)
	}
	if n, err := s.GetSchedCountAtDepth(SideSRC, PhaseCopy, 4, testNodeTypeFolder); err != nil || n != 0 {
		t.Fatalf("after drop count=%d err=%v", n, err)
	}
}

func TestListSchedAtDepthNoStatusFilter(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	id := testID("SRC", "", testNodeTypeFolder, "stale")
	if err := s.PutStatus(SideSRC, id, StatusRecord{TraversalStatus: "successful", CopyStatus: testStatusPending}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddPending(SideSRC, PhaseTrav, 2, testNodeTypeFolder, id); err != nil {
		t.Fatal(err)
	}
	ids, err := s.ListSchedAtDepth(SideSRC, PhaseTrav, 2, testNodeTypeFolder, "", 10)
	if err != nil || len(ids) != 1 || ids[0] != id {
		t.Fatalf("sched pull uses pend keys only: %+v err=%v", ids, err)
	}
}

func TestQueuePositionRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	want := QueuePosition{Round: 7, Cursor: "folder-abc"}
	if err := s.PutQueuePosition("src", "traversal", want); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.GetQueuePosition("src", "traversal")
	if err != nil || !ok || got.Round != want.Round || got.Cursor != want.Cursor {
		t.Fatalf("got %+v ok=%v err=%v", got, ok, err)
	}
}

func TestPutNodeStatusPendingWritesNodeStatusAndPend(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	parent := testID("SRC", "", testNodeTypeFolder, "p")
	id := testID("SRC", parent, testNodeTypeFolder, "c")
	if _, err := s.PutNodeStatusPending(SideSRC, NodeRecord{
		ID: id, ParentID: parent, Type: testNodeTypeFolder, Path: "/p/c", Name: "c",
	}, StatusRecord{TraversalStatus: testStatusPending, CopyStatus: testStatusPending}, 3, []PendingDelta{
		{Phase: PhaseTrav, NodeType: testNodeTypeFolder, Add: true},
		{Phase: PhaseCopy, NodeType: testNodeTypeFolder, Add: true},
	}); err != nil {
		t.Fatal(err)
	}

	n, ok, err := s.GetNode(SideSRC, id)
	if err != nil || !ok || n.ParentID != parent {
		t.Fatalf("node %+v ok=%v err=%v", n, ok, err)
	}
	st, ok, err := s.GetStatus(SideSRC, id)
	if err != nil || !ok || st.TraversalStatus != testStatusPending {
		t.Fatalf("status %+v ok=%v err=%v", st, ok, err)
	}
	trav, err := s.ListSchedAtDepth(SideSRC, PhaseTrav, 3, testNodeTypeFolder, "", 10)
	if err != nil || len(trav) != 1 || trav[0] != id {
		t.Fatalf("trav pend %+v err=%v", trav, err)
	}
	copyIDs, err := s.ListSchedAtDepth(SideSRC, PhaseCopy, 3, testNodeTypeFolder, "", 10)
	if err != nil || len(copyIDs) != 1 || copyIDs[0] != id {
		t.Fatalf("copy pend %+v err=%v", copyIDs, err)
	}
	sched, err := s.PutNodeStatusPending(SideSRC, NodeRecord{
		ID: id, ParentID: parent, Type: testNodeTypeFolder, Path: "/p/c", Name: "c",
	}, StatusRecord{TraversalStatus: testStatusPending, CopyStatus: testStatusPending}, 3, []PendingDelta{
		{Phase: PhaseTrav, NodeType: testNodeTypeFolder, Add: true},
		{Phase: PhaseCopy, NodeType: testNodeTypeFolder, Add: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sched) != 0 {
		t.Fatalf("idempotent re-add should yield no sched deltas, got %+v", sched)
	}
	if _, err := s.PutNodeStatusPending(SideSRC, NodeRecord{}, StatusRecord{}, 0, nil); err == nil {
		t.Fatal("empty id should fail")
	}
}

func TestApplySchedCountDeltas(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	id := testID("SRC", "", testNodeTypeFolder, "c")
	sched, err := s.PutNodeStatusPending(SideSRC, NodeRecord{ID: id, Type: testNodeTypeFolder, Path: "/c"}, StatusRecord{TraversalStatus: testStatusPending}, 2, []PendingDelta{
		{Phase: PhaseTrav, NodeType: testNodeTypeFolder, Add: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sched) != 1 || sched[0].Delta != 1 {
		t.Fatalf("sched deltas %+v", sched)
	}
	if n, err := s.GetSchedCountAtDepth(SideSRC, PhaseTrav, 2, testNodeTypeFolder); err != nil || n != 0 {
		t.Fatalf("before apply count=%d err=%v", n, err)
	}
	if err := s.ApplySchedCountDeltas(sched); err != nil {
		t.Fatal(err)
	}
	if n, err := s.GetSchedCountAtDepth(SideSRC, PhaseTrav, 2, testNodeTypeFolder); err != nil || n != 1 {
		t.Fatalf("after apply count=%d err=%v", n, err)
	}
}

func TestWalkStatusPagesOverlay(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ids := []string{"aaa", "bbb", "ccc"}
	for i, id := range ids {
		st := StatusRecord{TraversalStatus: testStatusPending}
		if i == 1 {
			st.TraversalStatus = "successful"
		}
		if _, err := s.PutNodeStatusPending(SideSRC, NodeRecord{ID: id, Type: testNodeTypeFolder, Path: "/" + id}, st, 1, nil); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	err = s.WalkStatus(SideSRC, "", false, func(id string, st StatusRecord) (bool, error) {
		if st.TraversalStatus == testStatusPending {
			got = append(got, id)
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "aaa" || got[1] != "ccc" {
		t.Fatalf("walk pending %+v", got)
	}
	got = nil
	err = s.WalkStatus(SideSRC, "aaa", false, func(id string, st StatusRecord) (bool, error) {
		if st.TraversalStatus == testStatusPending {
			got = append(got, id)
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "ccc" {
		t.Fatalf("walk after aaa %+v", got)
	}
}

func TestWriteSealBatch(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	parent := testID("SRC", "", testNodeTypeFolder, "p")
	const n = 50
	ids := make([]string, n)
	writes := make([]SealNodeWrite, 0, n+1)
	writes = append(writes, SealNodeWrite{
		Side:       SideSRC,
		Node:       NodeRecord{ID: parent, Type: testNodeTypeFolder, Path: "/p", Name: "p"},
		Status:     StatusRecord{TraversalStatus: "successful"},
		Depth:      0,
		InsertOnly: true,
	})
	for i := 0; i < n; i++ {
		id := testID("SRC", parent, testNodeTypeFile, fmt.Sprintf("f%d", i))
		ids[i] = id
		writes = append(writes, SealNodeWrite{
			Side:       SideSRC,
			Node:       NodeRecord{ID: id, ParentID: parent, Type: testNodeTypeFile, Path: "/p/" + id, Name: fmt.Sprintf("f%d", i)},
			Status:     StatusRecord{TraversalStatus: "successful", CopyStatus: testStatusPending},
			Depth:      1,
			InsertOnly: true,
			Deltas:     []PendingDelta{{Phase: PhaseCopy, NodeType: testNodeTypeFile, Add: true, PendWasSet: false}},
		})
	}
	kids := make([]KidRecord, 0, n)
	for i := 0; i < n; i++ {
		kids = append(kids, KidRecord{
			ID:              ids[i],
			Path:            "/p/" + ids[i],
			Name:            fmt.Sprintf("f%d", i),
			Type:            testNodeTypeFile,
			Depth:           1,
			TraversalStatus: "successful",
			CopyStatus:      testStatusPending,
		})
	}
	sched, err := s.WriteSealBatchTrusted(writes, nil, []SealKidsReplace{{ParentID: parent, Kids: kids}}, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(sched) != n {
		t.Fatalf("sched deltas=%d want %d", len(sched), n)
	}
	if err := s.ApplySchedCountDeltas(sched); err != nil {
		t.Fatal(err)
	}
	if got, err := s.GetSchedCountAtDepth(SideSRC, PhaseCopy, 1, testNodeTypeFile); err != nil || got != n {
		t.Fatalf("copy count=%d err=%v want %d", got, err, n)
	}

	recs, sts, err := s.BatchGetNodeStatus(SideSRC, append([]string{parent, "missing"}, ids...))
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != n+1 {
		t.Fatalf("nodes=%d want %d", len(recs), n+1)
	}
	if sts[ids[0]].CopyStatus != testStatusPending {
		t.Fatalf("child status %+v", sts[ids[0]])
	}

	childIDs, err := s.ListChildrenMany(SideSRC, []string{parent, "missing"}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(childIDs[parent]) != n {
		t.Fatalf("children=%d want %d", len(childIDs[parent]), n)
	}
	packs, err := s.BatchGetKids(SideSRC, []string{parent, "missing"})
	if err != nil {
		t.Fatal(err)
	}
	if len(packs[parent]) != n {
		t.Fatalf("kids pack=%d want %d", len(packs[parent]), n)
	}
	if packs[parent][0].CopyStatus != testStatusPending || packs[parent][0].Name == "" {
		t.Fatalf("kid snapshot %+v", packs[parent][0])
	}

	readd := writes[1]
	readd.Deltas = []PendingDelta{{Phase: PhaseCopy, NodeType: testNodeTypeFile, Add: true, PendWasSet: true}}
	sched2, err := s.WriteSealBatch([]SealNodeWrite{readd}, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(sched2) != 0 {
		t.Fatalf("idempotent re-add sched %+v", sched2)
	}

	id := ids[0]
	sched3, err := s.WriteSealBatch(nil, []SealStatusWrite{{
		Side:   SideSRC,
		ID:     id,
		Status: StatusRecord{TraversalStatus: "successful", CopyStatus: "successful"},
		Depth:  1,
		Deltas: []PendingDelta{{Phase: PhaseCopy, NodeType: testNodeTypeFile, Add: false, PendWasSet: true}},
	}}, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(sched3) != 1 || sched3[0].Delta != -1 {
		t.Fatalf("delete overlay sched %+v", sched3)
	}
	if err := s.ApplySchedCountDeltas(sched3); err != nil {
		t.Fatal(err)
	}
	if got, err := s.GetSchedCountAtDepth(SideSRC, PhaseCopy, 1, testNodeTypeFile); err != nil || got != n-1 {
		t.Fatalf("after overlay delete count=%d err=%v", got, err)
	}
	pending, err := s.ListSchedAtDepth(SideSRC, PhaseCopy, 1, testNodeTypeFile, "", n+1)
	if err != nil {
		t.Fatal(err)
	}
	for _, pid := range pending {
		if pid == id {
			t.Fatalf("deleted pend key still listed: %+v", pending)
		}
	}

	if _, err := s.WriteSealBatch([]SealNodeWrite{{Side: SideSRC, Node: NodeRecord{}}}, nil, nil, nil, nil); err == nil {
		t.Fatal("empty node id should fail")
	}
}

func TestWriteSealBatchTrustedDropPendingIgnoresStalePendWasSet(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	id := testID("SRC", "", testNodeTypeFolder, "ae")
	if _, err := s.WriteSealBatchTrusted([]SealNodeWrite{{
		Side:       SideSRC,
		Node:       NodeRecord{ID: id, Type: testNodeTypeFolder, Path: "/ae", Name: "ae"},
		Status:     StatusRecord{TraversalStatus: "successful", CopyStatus: testStatusPending},
		Depth:      5,
		InsertOnly: true,
		Deltas:     []PendingDelta{{Phase: PhaseCopy, NodeType: testNodeTypeFolder, Add: true, PendWasSet: false}},
	}}, nil, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	sched, err := s.WriteSealBatchTrusted(nil, []SealStatusWrite{{
		Side:   SideSRC,
		ID:     id,
		Status: StatusRecord{CopyStatus: "already_existed"},
		Depth:  5,
		Deltas: []PendingDelta{{Phase: PhaseCopy, NodeType: testNodeTypeFolder, Add: false, PendWasSet: false}},
	}}, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(sched) != 0 {
		t.Fatalf("stale prev should not move schedcnt, got %+v", sched)
	}
	pending, err := s.ListSchedAtDepth(SideSRC, PhaseCopy, 5, testNodeTypeFolder, "", 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pend key must drop when leaving pending, got %+v err=%v", pending, err)
	}
}

func TestWriteSealBatchAddThenDeleteSameDrain(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	id := testID("SRC", "", testNodeTypeFolder, "n")
	sched, err := s.WriteSealBatch([]SealNodeWrite{{
		Side:       SideSRC,
		Node:       NodeRecord{ID: id, Type: testNodeTypeFolder, Path: "/n", Name: "n"},
		Status:     StatusRecord{TraversalStatus: testStatusPending, CopyStatus: testStatusPending},
		Depth:      2,
		InsertOnly: true,
		Deltas:     []PendingDelta{{Phase: PhaseTrav, NodeType: testNodeTypeFolder, Add: true, PendWasSet: false}},
	}}, []SealStatusWrite{{
		Side:   SideSRC,
		ID:     id,
		Status: StatusRecord{TraversalStatus: "successful", CopyStatus: testStatusPending},
		Depth:  2,
		Deltas: []PendingDelta{{Phase: PhaseTrav, NodeType: testNodeTypeFolder, Add: false, PendWasSet: true}},
	}}, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var net int64
	for _, d := range sched {
		net += d.Delta
	}
	if net != 0 {
		t.Fatalf("same-drain add then delete net=%d sched=%+v", net, sched)
	}
	if err := s.ApplySchedCountDeltas(sched); err != nil {
		t.Fatal(err)
	}
	ids, err := s.ListSchedAtDepth(SideSRC, PhaseTrav, 2, testNodeTypeFolder, "", 10)
	if err != nil || len(ids) != 0 {
		t.Fatalf("pend after overlay %+v err=%v", ids, err)
	}
	if got, err := s.GetSchedCountAtDepth(SideSRC, PhaseTrav, 2, testNodeTypeFolder); err != nil || got != 0 {
		t.Fatalf("count=%d err=%v", got, err)
	}
}

func TestBatchGetMap(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	src := testID("SRC", "", testNodeTypeFolder, "a")
	dst := testID("DST", "", testNodeTypeFolder, "a")
	if _, err := s.WriteSealBatch(nil, nil, []SealMapWrite{{
		Map:   IDMapRecord{SrcID: src, DstID: dst, Status: "active"},
		Depth: 1,
	}}, nil, nil); err != nil {
		t.Fatal(err)
	}
	got, err := s.BatchGetMap([]string{dst, "missing"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[dst].SrcID != src {
		t.Fatalf("by dst %+v", got)
	}
	got, err = s.BatchGetMap([]string{src, "missing"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[src].DstID != dst {
		t.Fatalf("by src %+v", got)
	}
}

func TestKidsPackMergesAcrossDrains(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	parent := testID("SRC", "", testNodeTypeFolder, "p")
	first := testID("SRC", parent, testNodeTypeFile, "a")
	second := testID("SRC", parent, testNodeTypeFile, "b")
	if _, err := s.WriteSealBatch([]SealNodeWrite{{
		Side:       SideSRC,
		Node:       NodeRecord{ID: parent, Type: testNodeTypeFolder, Path: "/p", Name: "p"},
		Status:     StatusRecord{TraversalStatus: "successful"},
		InsertOnly: true,
	}, {
		Side:       SideSRC,
		Node:       NodeRecord{ID: first, ParentID: parent, Type: testNodeTypeFile, Path: "/p/a", Name: "a"},
		Status:     StatusRecord{TraversalStatus: "successful", CopyStatus: testStatusPending},
		Depth:      1,
		InsertOnly: true,
	}}, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteSealBatchTrusted(nil, nil, []SealKidsReplace{{
		ParentID: parent,
		Kids:     []KidRecord{{ID: first, Path: "/p/a", Name: "a", Type: testNodeTypeFile, Depth: 1, TraversalStatus: "successful", CopyStatus: testStatusPending}},
	}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteSealBatch([]SealNodeWrite{{
		Side:       SideSRC,
		Node:       NodeRecord{ID: second, ParentID: parent, Type: testNodeTypeFile, Path: "/p/b", Name: "b"},
		Status:     StatusRecord{TraversalStatus: "successful", CopyStatus: testStatusPending},
		Depth:      1,
		InsertOnly: true,
	}}, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteSealBatchTrusted(nil, nil, []SealKidsReplace{{
		ParentID: parent,
		Kids: []KidRecord{
			{ID: first, Path: "/p/a", Name: "a", Type: testNodeTypeFile, Depth: 1, TraversalStatus: "successful", CopyStatus: testStatusPending},
			{ID: second, Path: "/p/b", Name: "b", Type: testNodeTypeFile, Depth: 1, TraversalStatus: "successful", CopyStatus: testStatusPending},
		},
	}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	packs, err := s.BatchGetKids(SideSRC, []string{parent})
	if err != nil {
		t.Fatal(err)
	}
	if len(packs[parent]) != 2 {
		t.Fatalf("kids=%d want 2 %+v", len(packs[parent]), packs[parent])
	}
	got := map[string]bool{}
	for _, k := range packs[parent] {
		got[k.Name] = true
	}
	if !got["a"] || !got["b"] {
		t.Fatalf("names %+v", packs[parent])
	}
}

func TestListNodeIDsPagesFromCursor(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ids := []string{"n1", "n2", "n3", "n4"}
	writes := make([]SealNodeWrite, len(ids))
	for i, id := range ids {
		writes[i] = SealNodeWrite{
			Side:       SideSRC,
			Node:       NodeRecord{ID: id, Type: testNodeTypeFile, Path: "/" + id, Name: id},
			Status:     StatusRecord{TraversalStatus: testStatusPending},
			Depth:      1,
			InsertOnly: true,
		}
	}
	if _, err := s.WriteSealBatch(writes, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	first, err := s.ListNodeIDs(SideSRC, "", 2)
	if err != nil || len(first) != 2 {
		t.Fatalf("first %+v err=%v", first, err)
	}
	second, err := s.ListNodeIDs(SideSRC, first[len(first)-1], 10)
	if err != nil || len(second) != 2 {
		t.Fatalf("second %+v err=%v", second, err)
	}
	seen := map[string]bool{}
	for _, id := range append(append([]string{}, first...), second...) {
		if seen[id] {
			t.Fatalf("duplicate %s", id)
		}
		seen[id] = true
	}
	if len(seen) != 4 {
		t.Fatalf("seen=%d", len(seen))
	}
	if err := s.PutCatalogCursor("node", SideSRC, second[len(second)-1]); err != nil {
		t.Fatal(err)
	}
	if err := s.PutCatalogCursor("node", SideSRC, first[0]); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetCatalogCursor("node", SideSRC)
	if err != nil || got != first[0] {
		t.Fatalf("cursor=%q err=%v", got, err)
	}
}

func TestWriteSealBatchStatusMergePreservesTraversal(t *testing.T) {
	s, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	id := testID("SRC", "", testNodeTypeFolder, "folder")
	if _, err := s.WriteSealBatch([]SealNodeWrite{{
		Side:       SideSRC,
		Node:       NodeRecord{ID: id, Type: testNodeTypeFolder, Path: "/folder", Name: "folder"},
		Status:     StatusRecord{TraversalStatus: "successful", CopyStatus: testStatusPending},
		Depth:      1,
		InsertOnly: true,
	}}, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	// Partial copy-match event must not wipe traversal_status.
	if _, err := s.WriteSealBatch(nil, []SealStatusWrite{{
		Side:   SideSRC,
		ID:     id,
		Status: StatusRecord{CopyStatus: "already_existed", DeleteStatus: "pending_explicit"},
		Depth:  1,
	}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	st, ok, err := s.GetStatus(SideSRC, id)
	if err != nil || !ok {
		t.Fatalf("get status: ok=%v err=%v", ok, err)
	}
	if st.TraversalStatus != "successful" {
		t.Fatalf("traversal=%q want successful", st.TraversalStatus)
	}
	if st.CopyStatus != "already_existed" {
		t.Fatalf("copy=%q want already_existed", st.CopyStatus)
	}
	if st.DeleteStatus != "pending_explicit" {
		t.Fatalf("delete=%q want pending_explicit", st.DeleteStatus)
	}
}
