// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"encoding/json"
	"testing"
)

func TestNormalizeLocalRootPath(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{`C:\data\foo`, "C:/data/foo"},
		{"/mnt/a/", "/mnt/a"},
		{"C:/", "C:/"},
		{"", ""},
	}
	for _, tc := range tests {
		if got := NormalizeLocalRootPath(tc.in); got != tc.want {
			t.Fatalf("NormalizeLocalRootPath(%q)=%q want %q", tc.in, got, tc.want)
		}
	}
}

func TestRemapPathPrefixBoundary(t *testing.T) {
	// /mnt/a must not match /mnt/ab
	if got, ok := RemapPathPrefix("/mnt/ab/x", "/mnt/a", "/new"); ok {
		t.Fatalf("false prefix match: got %q", got)
	}
	got, ok := RemapPathPrefix("/mnt/a/x", "/mnt/a", "/new")
	if !ok || got != "/new/x" {
		t.Fatalf("got %q ok=%v", got, ok)
	}
	got, ok = RemapPathPrefix("/mnt/a", "/mnt/a", "/new")
	if !ok || got != "/new" {
		t.Fatalf("root remap got %q ok=%v", got, ok)
	}
}

func TestRemapLocalServiceIDsRewritesNodesKidsIncludeOnly(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	oldRoot := "/old/root"
	newRoot := "/new/root"
	include, _ := json.Marshal([]string{oldRoot + "/keep", oldRoot + "/skip"})

	root := NodeRecord{
		ID: "root", Path: "/", DisplayPath: "/", Name: "root", Type: NodeTypeFolder,
		ServiceID: oldRoot, IncludeOnly: string(include),
	}
	child := NodeRecord{
		ID: "child", ParentID: "root", Path: "/c", DisplayPath: "/c", Name: "c", Type: NodeTypeFile,
		ServiceID: oldRoot + "/c", ParentServiceID: oldRoot,
	}
	if err := s.PutNode(SideSRC, root); err != nil {
		t.Fatal(err)
	}
	if err := s.PutNode(SideSRC, child); err != nil {
		t.Fatal(err)
	}
	if err := s.PutStatus(SideSRC, "root", StatusRecord{IncludeOnly: string(include)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteSealBatchTrusted(nil, nil, []SealKidsReplace{{
		Side: SideSRC, ParentID: "root",
		Kids: []KidRecord{
			{ID: "child", ServiceID: oldRoot + "/c", ParentServiceID: oldRoot, Name: "c", Type: NodeTypeFile, Path: "/c"},
		},
	}}, nil, nil); err != nil {
		t.Fatal(err)
	}

	n, err := s.RemapLocalServiceIDs(SideSRC, oldRoot, newRoot)
	if err != nil {
		t.Fatal(err)
	}
	if n < 2 {
		t.Fatalf("rewritten=%d want >=2", n)
	}

	gotRoot, ok, err := s.GetNode(SideSRC, "root")
	if err != nil || !ok {
		t.Fatalf("root: ok=%v err=%v", ok, err)
	}
	if gotRoot.ServiceID != newRoot {
		t.Fatalf("root ServiceID=%q", gotRoot.ServiceID)
	}
	if gotRoot.Path != "/" || gotRoot.DisplayPath != "/" {
		t.Fatalf("path fields changed: path=%q display=%q", gotRoot.Path, gotRoot.DisplayPath)
	}
	var incl []string
	if err := json.Unmarshal([]byte(gotRoot.IncludeOnly), &incl); err != nil {
		t.Fatal(err)
	}
	if len(incl) != 2 || incl[0] != newRoot+"/keep" || incl[1] != newRoot+"/skip" {
		t.Fatalf("include_only=%v", incl)
	}

	gotChild, ok, err := s.GetNode(SideSRC, "child")
	if err != nil || !ok {
		t.Fatalf("child: ok=%v err=%v", ok, err)
	}
	if gotChild.ServiceID != newRoot+"/c" || gotChild.ParentServiceID != newRoot {
		t.Fatalf("child ids: %q / %q", gotChild.ServiceID, gotChild.ParentServiceID)
	}

	kids, err := s.BatchGetKids(SideSRC, []string{"root"})
	if err != nil {
		t.Fatal(err)
	}
	pack := kids["root"]
	if len(pack) != 1 || pack[0].ServiceID != newRoot+"/c" || pack[0].ParentServiceID != newRoot {
		t.Fatalf("kids=%+v", pack)
	}

	st, ok, err := s.GetStatus(SideSRC, "root")
	if err != nil || !ok {
		t.Fatalf("status: ok=%v err=%v", ok, err)
	}
	if err := json.Unmarshal([]byte(st.IncludeOnly), &incl); err != nil {
		t.Fatal(err)
	}
	if incl[0] != newRoot+"/keep" {
		t.Fatalf("status include_only=%v", incl)
	}
}

func TestRemapLocalServiceIDsRewritesXferDstRef(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	oldDST := "/dst/old"
	newDST := "/dst/new"
	if err := s.PutNode(SideDST, NodeRecord{
		ID: "droot", Path: "/", Type: NodeTypeFolder, ServiceID: oldDST,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutNode(SideSRC, NodeRecord{
		ID: "sfile", Path: "/f", Type: NodeTypeFile, ServiceID: "/src/f",
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutStatus(SideSRC, "sfile", StatusRecord{
		XferDstRef: oldDST + "/f",
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := s.RemapLocalServiceIDs(SideDST, oldDST, newDST); err != nil {
		t.Fatal(err)
	}
	st, ok, err := s.GetStatus(SideSRC, "sfile")
	if err != nil || !ok {
		t.Fatalf("status: ok=%v err=%v", ok, err)
	}
	if st.XferDstRef != newDST+"/f" {
		t.Fatalf("XferDstRef=%q", st.XferDstRef)
	}
	dn, ok, err := s.GetNode(SideDST, "droot")
	if err != nil || !ok {
		t.Fatal(err)
	}
	if dn.ServiceID != newDST {
		t.Fatalf("dst ServiceID=%q", dn.ServiceID)
	}
}

func TestDeleteAllOAuthCreds(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.PutOAuthCred("a", "enc:v1:xxx"); err != nil {
		t.Fatal(err)
	}
	if err := s.PutOAuthCred("b", `{"tok":1}`); err != nil {
		t.Fatal(err)
	}
	n, err := s.DeleteAllOAuthCreds()
	if err != nil || n != 2 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	ids, err := s.ListOAuthConnectionIDs()
	if err != nil || len(ids) != 0 {
		t.Fatalf("ids=%v err=%v", ids, err)
	}
}
