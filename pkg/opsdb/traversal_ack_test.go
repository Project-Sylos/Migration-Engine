// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"path/filepath"
	"testing"
)

func TestApplyTraversalAcknowledgeFailed(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: filepath.Join(dir, "ops")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	folderID := "folder1"
	childID := "child1"
	okID := "ok1"
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
		{
			Side: SideSRC,
			Node: NodeRecord{ID: okID, Path: "/locked/b", Name: "b", Type: NodeTypeFile, Depth: 2, ParentID: folderID, Size: 10},
			Status: StatusRecord{
				TraversalStatus: traversalStatusSuccessful,
				CopyStatus:      copyStatusPending,
			},
			Depth:      2,
			InsertOnly: true,
		},
	}
	if _, err := s.WriteSealBatch(writes, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	res, err := ApplyRetryAtID(s, SideSRC, folderID, true, s.ApplyTraversalAckChunk)
	if err != nil {
		t.Fatal(err)
	}
	if res.Affected != 2 {
		t.Fatalf("ack affected=%d want 2 (failed only)", res.Affected)
	}
	for _, id := range []string{folderID, childID} {
		st, ok, err := s.GetStatus(SideSRC, id)
		if err != nil || !ok {
			t.Fatal(err)
		}
		if st.TraversalStatus != traversalStatusExcluded {
			t.Fatalf("%s status=%q want excluded", id, st.TraversalStatus)
		}
		if !IsTraversalAckFromFailed(st.ExclusionSource) {
			t.Fatalf("%s ExclusionSource=%q", id, st.ExclusionSource)
		}
	}
	stOK, _, _ := s.GetStatus(SideSRC, okID)
	if stOK.TraversalStatus != traversalStatusSuccessful {
		t.Fatalf("successful node mutated: %q", stOK.TraversalStatus)
	}

	// Acknowledged nodes must not appear on discovery frontier.
	ids, err := s.ListSchedAtDepth(SideSRC, PhaseTrav, 1, NodeTypeFolder, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if id == folderID {
			t.Fatal("acknowledged folder must not be on pend:trav")
		}
	}

	ures, err := ApplyRetryAtID(s, SideSRC, folderID, false, s.ApplyTraversalAckChunk)
	if err != nil {
		t.Fatal(err)
	}
	if ures.Affected != 2 {
		t.Fatalf("unack affected=%d want 2", ures.Affected)
	}
	st, _, _ := s.GetStatus(SideSRC, folderID)
	if st.TraversalStatus != traversalStatusFailed || st.ExclusionSource != "" {
		t.Fatalf("unack status=%+v", st)
	}
}

func TestApplyTraversalAckPairsFailedDST(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: filepath.Join(dir, "ops")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	srcID := "src1"
	dstID := "dst1"
	writes := []SealNodeWrite{
		{
			Side: SideSRC,
			Node: NodeRecord{ID: srcID, Path: "/pair", Name: "pair", Type: NodeTypeFile, Depth: 1, Size: 1},
			Status: StatusRecord{
				TraversalStatus: traversalStatusFailed,
				CopyStatus:      copyStatusPending,
			},
			Depth:      1,
			InsertOnly: true,
		},
		{
			Side: SideDST,
			Node: NodeRecord{ID: dstID, Path: "/pair", Name: "pair", Type: NodeTypeFile, Depth: 1, Size: 1},
			Status: StatusRecord{
				TraversalStatus: traversalStatusFailed,
				CopyStatus:      copyStatusPending,
			},
			Depth:      1,
			InsertOnly: true,
		},
	}
	if _, err := s.WriteSealBatch(writes, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	res, err := s.ApplyTraversalAck(srcID, dstID, true)
	if err != nil {
		t.Fatal(err)
	}
	if res.Affected != 1 {
		t.Fatalf("ack affected=%d want 1 (SRC count)", res.Affected)
	}
	for _, pair := range []struct {
		side string
		id   string
	}{{SideSRC, srcID}, {SideDST, dstID}} {
		st, ok, err := s.GetStatus(pair.side, pair.id)
		if err != nil || !ok {
			t.Fatalf("%s %s: %v", pair.side, pair.id, err)
		}
		if st.TraversalStatus != traversalStatusExcluded || !IsTraversalAckFromFailed(st.ExclusionSource) {
			t.Fatalf("%s %s status=%+v", pair.side, pair.id, st)
		}
	}

	ures, err := s.ApplyTraversalAck(srcID, dstID, false)
	if err != nil {
		t.Fatal(err)
	}
	if ures.Affected != 1 {
		t.Fatalf("unack affected=%d want 1", ures.Affected)
	}
	for _, pair := range []struct {
		side string
		id   string
	}{{SideSRC, srcID}, {SideDST, dstID}} {
		st, _, _ := s.GetStatus(pair.side, pair.id)
		if st.TraversalStatus != traversalStatusFailed || st.ExclusionSource != "" {
			t.Fatalf("%s %s after unack=%+v", pair.side, pair.id, st)
		}
	}
}

func TestApplyTraversalAckLeavesSuccessfulDST(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: filepath.Join(dir, "ops")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	srcID := "src1"
	dstID := "dst1"
	writes := []SealNodeWrite{
		{
			Side: SideSRC,
			Node: NodeRecord{ID: srcID, Path: "/pair", Name: "pair", Type: NodeTypeFile, Depth: 1, Size: 1},
			Status: StatusRecord{
				TraversalStatus: traversalStatusFailed,
				CopyStatus:      copyStatusPending,
			},
			Depth:      1,
			InsertOnly: true,
		},
		{
			Side: SideDST,
			Node: NodeRecord{ID: dstID, Path: "/pair", Name: "pair", Type: NodeTypeFile, Depth: 1, Size: 1},
			Status: StatusRecord{
				TraversalStatus: traversalStatusSuccessful,
				CopyStatus:      copyStatusPending,
			},
			Depth:      1,
			InsertOnly: true,
		},
	}
	if _, err := s.WriteSealBatch(writes, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	if _, err := s.ApplyTraversalAck(srcID, dstID, true); err != nil {
		t.Fatal(err)
	}
	stDST, _, _ := s.GetStatus(SideDST, dstID)
	if stDST.TraversalStatus != traversalStatusSuccessful {
		t.Fatalf("successful DST mutated: %+v", stDST)
	}
	stSRC, _, _ := s.GetStatus(SideSRC, srcID)
	if stSRC.TraversalStatus != traversalStatusExcluded {
		t.Fatalf("SRC not acked: %+v", stSRC)
	}
}

func TestAcknowledgeThenMarkRetry(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: filepath.Join(dir, "ops")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	id := "f1"
	writes := []SealNodeWrite{{
		Side: SideSRC,
		Node: NodeRecord{ID: id, Path: "/x", Name: "x", Type: NodeTypeFolder, Depth: 1},
		Status: StatusRecord{
			TraversalStatus: traversalStatusFailed,
			CopyStatus:      copyStatusPending,
		},
		Depth:      1,
		InsertOnly: true,
	}}
	if _, err := s.WriteSealBatch(writes, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := ApplyRetryAtID(s, SideSRC, id, true, s.ApplyTraversalAckChunk); err != nil {
		t.Fatal(err)
	}
	res, err := s.ApplyTraversalRetry(id, "", true)
	if err != nil {
		t.Fatal(err)
	}
	if res.Affected != 1 || res.FromFailed != 1 || res.FromExcluded != 0 {
		t.Fatalf("mark after ack=%+v", res)
	}
	st, _, _ := s.GetStatus(SideSRC, id)
	if st.TraversalStatus != traversalStatusPending || st.ExclusionSource != ExclusionRetryParkCopy {
		t.Fatalf("after mark=%+v", st)
	}
}
