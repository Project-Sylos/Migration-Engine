// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"path/filepath"
	"testing"
)

func sealSRC(id, path, parentID, typ string, depth int, del string, size int64) SealNodeWrite {
	n := NodeRecord{ID: id, Path: path, Name: path[stringsLastSlash(path)+1:], Type: typ, ParentID: parentID, Size: size, Depth: depth}
	if path == "/" {
		n.Name = ""
	}
	nt := NodeTypeFolder
	if typ == NodeTypeFile {
		nt = NodeTypeFile
	}
	var deltas []PendingDelta
	if deleteStatusOnFrontier(del) {
		deltas = []PendingDelta{{Phase: PhaseDel, NodeType: nt, Add: true, PendWasSet: false}}
	}
	return SealNodeWrite{
		Side: SideSRC,
		Node: n,
		Status: StatusRecord{
			TraversalStatus: "successful",
			CopyStatus:      "successful",
			DeleteStatus:    del,
		},
		Depth:      depth,
		Deltas:     deltas,
		InsertOnly: true,
	}
}

func stringsLastSlash(p string) int {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return i
		}
	}
	return -1
}

func mustStatus(t *testing.T, s *Store, id string) StatusRecord {
	t.Helper()
	st, ok, err := s.GetStatus(SideSRC, id)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("missing status for %s", id)
	}
	return st
}

func hasPendDel(t *testing.T, s *Store, depth int, nodeType, id string) bool {
	t.Helper()
	ids, err := s.ListSchedAtDepth(SideSRC, PhaseDel, depth, nodeType, "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, got := range ids {
		if got == id {
			return true
		}
	}
	return false
}

func TestDeleteForestSkipUnskipSiblingPromotion(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: filepath.Join(dir, "ops")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	rootID := "root"
	aID := "a"
	bID := "b"
	cID := "c"
	dID := "d"
	eID := "e"

	writes := []SealNodeWrite{
		sealSRC(rootID, "/", "", NodeTypeFolder, 0, "", 0),
		sealSRC(aID, "/A", rootID, NodeTypeFolder, 1, deleteStatusPendingExplicit, 0),
		sealSRC(bID, "/A/B", aID, NodeTypeFolder, 2, deleteStatusPendingInherited, 0),
		sealSRC(cID, "/A/C", aID, NodeTypeFile, 2, deleteStatusPendingInherited, 5),
		sealSRC(dID, "/A/B/D", bID, NodeTypeFile, 3, deleteStatusPendingInherited, 1),
		sealSRC(eID, "/A/B/E", bID, NodeTypeFile, 3, deleteStatusPendingInherited, 1),
	}
	if _, err := s.WriteSealBatch(writes, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	if _, err := s.ApplyDeleteForestSkip("/A/B"); err != nil {
		t.Fatal(err)
	}

	if got := mustStatus(t, s, bID).DeleteStatus; got != deleteStatusSkipped {
		t.Fatalf("B=%q want skipped", got)
	}
	if got := mustStatus(t, s, dID).DeleteStatus; got != deleteStatusSkipped {
		t.Fatalf("D=%q want skipped", got)
	}
	if got := mustStatus(t, s, eID).DeleteStatus; got != deleteStatusSkipped {
		t.Fatalf("E=%q want skipped", got)
	}
	if got := mustStatus(t, s, aID).DeleteStatus; got != deleteStatusSkipped {
		t.Fatalf("A=%q want skipped", got)
	}
	if got := mustStatus(t, s, cID).DeleteStatus; got != deleteStatusPendingExplicit {
		t.Fatalf("C=%q want pending_explicit", got)
	}
	if mustStatus(t, s, aID).SkippedDescendantCount != 3 {
		t.Fatalf("A count=%d want 3", mustStatus(t, s, aID).SkippedDescendantCount)
	}
	if mustStatus(t, s, rootID).SkippedDescendantCount != 3 {
		t.Fatalf("root count=%d want 3", mustStatus(t, s, rootID).SkippedDescendantCount)
	}
	if mustStatus(t, s, rootID).DeleteStatus != "" {
		t.Fatalf("root delete_status=%q want empty", mustStatus(t, s, rootID).DeleteStatus)
	}
	if hasPendDel(t, s, 1, NodeTypeFolder, aID) {
		t.Fatal("A should not be on pend:del")
	}
	if !hasPendDel(t, s, 2, NodeTypeFile, cID) {
		t.Fatal("C should be on pend:del")
	}
	if hasPendDel(t, s, 2, NodeTypeFolder, bID) {
		t.Fatal("B should not be on pend:del")
	}

	// Idempotent re-skip must not double-count.
	if _, err := s.ApplyDeleteForestSkip("/A/B"); err != nil {
		t.Fatal(err)
	}
	if mustStatus(t, s, aID).SkippedDescendantCount != 3 {
		t.Fatalf("A count after re-skip=%d want 3", mustStatus(t, s, aID).SkippedDescendantCount)
	}

	if _, err := s.ApplyDeleteForestUnskip("/A/B"); err != nil {
		t.Fatal(err)
	}
	if got := mustStatus(t, s, aID).DeleteStatus; got != deleteStatusPendingExplicit {
		t.Fatalf("A after unskip=%q want pending_explicit", got)
	}
	if got := mustStatus(t, s, bID).DeleteStatus; got != deleteStatusPendingInherited {
		t.Fatalf("B after unskip=%q want pending_inherited", got)
	}
	if got := mustStatus(t, s, cID).DeleteStatus; got != deleteStatusPendingInherited {
		t.Fatalf("C after unskip=%q want pending_inherited", got)
	}
	if mustStatus(t, s, aID).SkippedDescendantCount != 0 {
		t.Fatalf("A count after unskip=%d want 0", mustStatus(t, s, aID).SkippedDescendantCount)
	}
	if mustStatus(t, s, rootID).SkippedDescendantCount != 0 {
		t.Fatalf("root count after unskip=%d want 0", mustStatus(t, s, rootID).SkippedDescendantCount)
	}
	if !hasPendDel(t, s, 1, NodeTypeFolder, aID) {
		t.Fatal("A should be on pend:del after unskip")
	}
	if hasPendDel(t, s, 2, NodeTypeFile, cID) {
		t.Fatal("C should not be on pend:del after demotion")
	}
}

func TestDeleteForestDisjointSkipKeepsAncestorPoisoned(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: filepath.Join(dir, "ops")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	rootID := "root"
	aID := "a"
	bID := "b"
	cID := "c"
	dID := "d"

	writes := []SealNodeWrite{
		sealSRC(rootID, "/", "", NodeTypeFolder, 0, "", 0),
		sealSRC(aID, "/A", rootID, NodeTypeFolder, 1, deleteStatusPendingExplicit, 0),
		sealSRC(bID, "/A/B", aID, NodeTypeFolder, 2, deleteStatusPendingInherited, 0),
		sealSRC(cID, "/A/C", aID, NodeTypeFile, 2, deleteStatusPendingInherited, 5),
		sealSRC(dID, "/A/B/D", bID, NodeTypeFile, 3, deleteStatusPendingInherited, 1),
	}
	if _, err := s.WriteSealBatch(writes, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	if _, err := s.ApplyDeleteForestSkip("/A/B/D"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyDeleteForestSkip("/A/C"); err != nil {
		t.Fatal(err)
	}
	if mustStatus(t, s, aID).DeleteStatus != deleteStatusSkipped {
		t.Fatal("A should stay skipped")
	}
	if mustStatus(t, s, aID).SkippedDescendantCount != 2 {
		t.Fatalf("A count=%d want 2 (D and C)", mustStatus(t, s, aID).SkippedDescendantCount)
	}

	if _, err := s.ApplyDeleteForestUnskip("/A/B/D"); err != nil {
		t.Fatal(err)
	}
	if mustStatus(t, s, aID).DeleteStatus != deleteStatusSkipped {
		t.Fatalf("A after unskip D=%q want skipped (C still skipped)", mustStatus(t, s, aID).DeleteStatus)
	}
	if mustStatus(t, s, aID).SkippedDescendantCount != 1 {
		t.Fatalf("A count after unskip D=%d want 1", mustStatus(t, s, aID).SkippedDescendantCount)
	}
	// B was only poisoned by D; unskip restores B as a new explicit root covering D.
	if mustStatus(t, s, bID).DeleteStatus != deleteStatusPendingExplicit {
		t.Fatalf("B=%q want pending_explicit", mustStatus(t, s, bID).DeleteStatus)
	}
	if mustStatus(t, s, dID).DeleteStatus != deleteStatusPendingInherited {
		t.Fatalf("D=%q want pending_inherited under restored B", mustStatus(t, s, dID).DeleteStatus)
	}
}

func TestDeleteForestInheritedNotOnFrontier(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: filepath.Join(dir, "ops")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	parentID := "p"
	childID := "c"
	writes := []SealNodeWrite{
		sealSRC(parentID, "/P", "", NodeTypeFolder, 1, deleteStatusPendingExplicit, 0),
		sealSRC(childID, "/P/c.txt", parentID, NodeTypeFile, 2, deleteStatusPendingInherited, 3),
	}
	sched, err := s.WriteSealBatch(writes, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ApplySchedCountDeltas(sched); err != nil {
		t.Fatal(err)
	}
	if !hasPendDel(t, s, 1, NodeTypeFolder, parentID) {
		t.Fatal("explicit parent should be on pend:del")
	}
	if hasPendDel(t, s, 2, NodeTypeFile, childID) {
		t.Fatal("inherited child must not be on pend:del")
	}
}
