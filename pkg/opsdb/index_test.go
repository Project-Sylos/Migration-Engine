// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"bytes"
	"testing"
)

func TestPathIndexTrailingSlashDoesNotMatchSibling(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	foo := SealNodeWrite{
		Side:       SideSRC,
		Node:       NodeRecord{ID: "id-foo", Path: "/foo", Name: "foo", Type: NodeTypeFolder, Size: 0},
		InsertOnly: true,
	}
	foobar := SealNodeWrite{
		Side:       SideSRC,
		Node:       NodeRecord{ID: "id-foobar", Path: "/foobar", Name: "foobar", Type: NodeTypeFolder},
		InsertOnly: true,
	}
	child := SealNodeWrite{
		Side:       SideSRC,
		Node:       NodeRecord{ID: "id-child", Path: "/foo/bar", Name: "bar", Type: NodeTypeFile, Size: 10, ParentID: "id-foo"},
		InsertOnly: true,
	}
	if _, err := s.WriteSealBatch([]SealNodeWrite{foo, foobar, child}, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	id, err := s.GetNodeIDByPath(SideSRC, "/foo")
	if err != nil || id != "id-foo" {
		t.Fatalf("exact /foo id=%q err=%v", id, err)
	}
	ids, err := s.ListSubtreeIDs(SideSRC, "/foo", 100)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, id := range ids {
		got[id] = true
	}
	if !got["id-foo"] || !got["id-child"] {
		t.Fatalf("subtree %+v", ids)
	}
	if got["id-foobar"] {
		t.Fatalf("sibling /foobar leaked into /foo subtree: %+v", ids)
	}
}

func TestIndexRenameUpdatesPathKey(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := s.WriteSealBatch([]SealNodeWrite{{
		Side:       SideSRC,
		Node:       NodeRecord{ID: "n1", Path: "/old", Name: "old", Type: NodeTypeFile, Size: 5},
		InsertOnly: true,
	}}, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	prev := NodeRecord{ID: "n1", Path: "/old", Name: "old", Type: NodeTypeFile, Size: 5}
	if _, err := s.WriteSealBatch([]SealNodeWrite{{
		Side:     SideSRC,
		Node:     NodeRecord{ID: "n1", Path: "/new", Name: "new", Type: NodeTypeFile, Size: 5},
		PrevNode: &prev,
	}}, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if id, err := s.GetNodeIDByPath(SideSRC, "/old"); err != nil || id != "" {
		t.Fatalf("old path still indexed id=%q err=%v", id, err)
	}
	if id, err := s.GetNodeIDByPath(SideSRC, "/new"); err != nil || id != "n1" {
		t.Fatalf("new path id=%q err=%v", id, err)
	}
}

func TestSizeAndNameIndexScan(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	writes := []SealNodeWrite{
		{Side: SideSRC, Node: NodeRecord{ID: "a", Path: "/a", Name: "Alpha", Type: NodeTypeFile, Size: 100}, InsertOnly: true},
		{Side: SideSRC, Node: NodeRecord{ID: "b", Path: "/b", Name: "beta", Type: NodeTypeFile, Size: 200}, InsertOnly: true},
		{Side: SideSRC, Node: NodeRecord{ID: "c", Path: "/c", Name: "gamma", Type: NodeTypeFile, Size: 50}, InsertOnly: true},
	}
	if _, err := s.WriteSealBatch(writes, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	ids, err := s.ScanSizeRange(SideSRC, 100, 200, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("size range %+v", ids)
	}
	names, err := s.ScanNamePrefix(SideSRC, "al", 10)
	if err != nil || len(names) != 1 || names[0] != "a" {
		t.Fatalf("name prefix %+v err=%v", names, err)
	}
	segs, err := s.ScanSegToken(SideSRC, "beta", 10)
	if err != nil || len(segs) != 1 || segs[0] != "b" {
		t.Fatalf("seg %+v err=%v", segs, err)
	}
}

func TestPathIndexKeyEncoding(t *testing.T) {
	k := PathIndexKey(SideSRC, "/foo")
	if !bytes.HasSuffix(k, []byte("/foo/")) {
		t.Fatalf("key %q", k)
	}
	sib := PathIndexKey(SideSRC, "/foobar")
	if bytes.HasPrefix(sib, append(k[:len(k)-1], '/')) {
		// /foo/ should not be prefix of /foobar/
	}
	if bytes.HasPrefix(sib, k) {
		t.Fatalf("%q has prefix %q", sib, k)
	}
}
