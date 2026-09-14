// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"strings"
	"testing"
)

func TestExtractTrigramsPadsShort(t *testing.T) {
	g := ExtractTrigrams("ab")
	if len(g) == 0 {
		t.Fatal("expected padded trigrams for short string")
	}
	g3 := ExtractTrigrams("folder")
	if len(g3) < 3 {
		t.Fatalf("folder grams %+v", g3)
	}
}

func TestScanTrigramIntersectPathAndName(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	writes := []SealNodeWrite{
		{Side: SideSRC, Node: NodeRecord{
			ID: "f1", Path: "/folder_4/file_12.txt", DisplayPath: "/folder_4/file_12.txt",
			Name: "file_12.txt", Type: NodeTypeFile, Size: 1,
		}, InsertOnly: true},
		{Side: SideSRC, Node: NodeRecord{
			ID: "f2", Path: "/other/readme.md", DisplayPath: "/other/readme.md",
			Name: "readme.md", Type: NodeTypeFile, Size: 1,
		}, InsertOnly: true},
		{Side: SideDST, Node: NodeRecord{
			ID: "d1", Path: "/folder_4/file_12.txt", DisplayPath: "/folder_4/file_12.txt",
			Name: "file_12.txt", Type: NodeTypeFile, Size: 1,
		}, InsertOnly: true},
	}
	if _, err := s.WriteSealBatch(writes, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	ids, err := s.ScanTrigramIntersect(SideSRC, "folder_4", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "f1" {
		t.Fatalf("path gram intersect %+v", ids)
	}
	ids, err = s.ScanTrigramIntersect(SideSRC, "file", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "f1" {
		t.Fatalf("name gram intersect %+v", ids)
	}
	ids, err = s.ScanTrigramIntersect(SideDST, "file", 100)
	if err != nil || len(ids) != 1 || ids[0] != "d1" {
		t.Fatalf("dst trigrams %+v err=%v", ids, err)
	}
}

func TestTrigramRenameDropsOldGrams(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	prev := NodeRecord{ID: "n1", Path: "/oldname.txt", Name: "oldname.txt", Type: NodeTypeFile, Size: 1}
	if _, err := s.WriteSealBatch([]SealNodeWrite{{
		Side: SideSRC, Node: prev, InsertOnly: true,
	}}, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	next := NodeRecord{ID: "n1", Path: "/newname.txt", Name: "newname.txt", Type: NodeTypeFile, Size: 1}
	if _, err := s.WriteSealBatch([]SealNodeWrite{{
		Side: SideSRC, Node: next, PrevNode: &prev,
	}}, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	oldIDs, err := s.ScanTrigramIntersect(SideSRC, "old", 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range oldIDs {
		if id == "n1" {
			t.Fatalf("old grams still list n1: %+v", oldIDs)
		}
	}
	newIDs, err := s.ScanTrigramIntersect(SideSRC, "new", 100)
	if err != nil || len(newIDs) != 1 || newIDs[0] != "n1" {
		t.Fatalf("new grams %+v err=%v", newIDs, err)
	}
}

func TestEnsureTriIndexV1Backfill(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	n := NodeRecord{ID: "x", Path: "/alpha/beta", DisplayPath: "/alpha/beta", Name: "beta", Type: NodeTypeFile}
	if err := s.PutNode(SideSRC, n); err != nil {
		t.Fatal(err)
	}
	// Simulate pre-trigram store: drop tri keys and clear meta.
	wb := s.db.NewWriteBatch()
	for _, g := range NodeTrigrams(n.Path, n.DisplayPath, n.Name) {
		if err := wb.Delete(TriIndexKey(SideSRC, g, n.ID)); err != nil {
			t.Fatal(err)
		}
	}
	if err := wb.Delete([]byte(metaTriIndexV1Key)); err != nil {
		t.Fatal(err)
	}
	if err := wb.Flush(); err != nil {
		t.Fatal(err)
	}
	ids, err := s.ScanTrigramIntersect(SideSRC, "beta", 10)
	if err != nil || len(ids) != 0 {
		t.Fatalf("expected empty before backfill %+v err=%v", ids, err)
	}
	if err := s.EnsureTriIndexV1(); err != nil {
		t.Fatal(err)
	}
	ids, err = s.ScanTrigramIntersect(SideSRC, "beta", 10)
	if err != nil || len(ids) != 1 || ids[0] != "x" {
		t.Fatalf("after backfill %+v err=%v", ids, err)
	}
	ok, err := s.HasTriIndexV1()
	if err != nil || !ok {
		t.Fatalf("meta flag ok=%v err=%v", ok, err)
	}
}

func TestNodeTrigramsPerSegmentNotAcrossSlash(t *testing.T) {
	grams := NodeTrigrams("/ab/cd", "", "ef")
	joined := strings.Join(grams, ",")
	if strings.Contains(joined, "b/c") || strings.Contains(joined, "b\\") {
		t.Fatalf("unexpected cross-slash gram in %+v", grams)
	}
}
