// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

// Smoke: hover/rename data path — validate → force-ignore → queue clear;
// accept-all; ignore-remaining leaves ListPathIssues empty for copy gate.
func TestPathNameReview_smokeFlows(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/path_smoke.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	srcRoot := db.MintNodeID("SRC", "", db.NodeTypeFolder, "/")
	aID := db.MintNodeID("SRC", srcRoot, db.NodeTypeFile, "a*1.txt")
	bID := db.MintNodeID("SRC", srcRoot, db.NodeTypeFile, "b*2.txt")
	cID := db.MintNodeID("SRC", srcRoot, db.NodeTypeFile, "c*3.txt")

	if err := database.RunWrite(t.Context(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert("src_nodes", []*db.NodeState{
				{ID: srcRoot, ServiceID: "/", Path: "/", Type: db.NodeTypeFolder, Depth: 0,
					GPLState: `{"valid":true,"part":{"valid":true},"path":{"valid":true},"parts":[]}`},
				{ID: aID, ServiceID: "a", ParentID: srcRoot, Path: "/a*1.txt", ParentPath: "/",
					Name: "a*1.txt", Type: db.NodeTypeFile, Depth: 1},
				{ID: bID, ServiceID: "b", ParentID: srcRoot, Path: "/b*2.txt", ParentPath: "/",
					Name: "b*2.txt", Type: db.NodeTypeFile, Depth: 1},
				{ID: cID, ServiceID: "c", ParentID: srcRoot, Path: "/c*3.txt", ParentPath: "/",
					Name: "c*3.txt", Type: db.NodeTypeFile, Depth: 1},
			}); err != nil {
				return err
			}
			return w.BatchInsertPathEvents([]db.PathEvent{
				{ID: aID, EventTime: time.Now().UnixNano(), Category: db.PathEventCategoryGPLClean,
					ProposedPath: "a_1.txt", Status: db.PathEventStatusPending},
				{ID: bID, EventTime: time.Now().UnixNano() + 1, Category: db.PathEventCategoryGPLClean,
					ProposedPath: "b_2.txt", Status: db.PathEventStatusPending},
				{ID: cID, EventTime: time.Now().UnixNano() + 2, Category: db.PathEventCategoryGPLClean,
					ProposedPath: "c_3.txt", Status: db.PathEventStatusPending},
			})
		})
	}); err != nil {
		t.Fatal(err)
	}

	m := &Migration{DB: database}
	m.setLastRunConfig(Config{Destination: Service{ProviderID: "windows"}})

	// Rename editor dry-run
	res, err := m.ValidatePathProposal(aID, "a_1.txt")
	if err != nil || !res.Valid {
		t.Fatalf("validate ok name: valid=%v err=%v messages=%v", res.Valid, err, res.Messages)
	}
	bad, err := m.ValidatePathProposal(aID, "still*bad.txt")
	if err != nil || bad.Valid || len(bad.Messages) == 0 {
		t.Fatalf("validate bad name: %+v err=%v", bad, err)
	}

	// Accept-all for a+b; leave c for ignore-remaining
	n, err := m.AcceptAllPathProposals()
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("accept-all accepted=%d want 3", n)
	}
	issues, err := m.ListPathIssues(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 0 {
		t.Fatalf("after accept-all want empty, got %+v", issues)
	}

	// Re-seed one issue; force-ignore via remap
	dID := db.MintNodeID("SRC", srcRoot, db.NodeTypeFile, "d*4.txt")
	if err := database.RunWrite(t.Context(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert("src_nodes", []*db.NodeState{
				{ID: dID, ServiceID: "d", ParentID: srcRoot, Path: "/d*4.txt", ParentPath: "/",
					Name: "d*4.txt", Type: db.NodeTypeFile, Depth: 1},
			}); err != nil {
				return err
			}
			return w.BatchInsertPathEvents([]db.PathEvent{{
				ID: dID, EventTime: time.Now().UnixNano(), Category: db.PathEventCategoryGPLClean,
				ProposedPath: "d_4.txt", Status: db.PathEventStatusPending,
			}})
		})
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.AcceptPathChange(dID, "still*bad.txt", true); err != nil {
		t.Fatal(err)
	}
	issues, err = m.ListPathIssues(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 0 {
		t.Fatalf("after force-ignore want empty, got %+v", issues)
	}

	// Ignore-remaining → copy gate clear
	eID := db.MintNodeID("SRC", srcRoot, db.NodeTypeFile, "e*5.txt")
	if err := database.RunWrite(t.Context(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert("src_nodes", []*db.NodeState{
				{ID: eID, ServiceID: "e", ParentID: srcRoot, Path: "/e*5.txt", ParentPath: "/",
					Name: "e*5.txt", Type: db.NodeTypeFile, Depth: 1},
			}); err != nil {
				return err
			}
			return w.BatchInsertPathEvents([]db.PathEvent{{
				ID: eID, EventTime: time.Now().UnixNano(), Category: db.PathEventCategoryGPLClean,
				ProposedPath: "e_5.txt", Status: db.PathEventStatusPending,
			}})
		})
	}); err != nil {
		t.Fatal(err)
	}
	ignored, err := m.IgnoreAllPathIssues()
	if err != nil || ignored != 1 {
		t.Fatalf("ignore-remaining ignored=%d err=%v", ignored, err)
	}
	issues, err = m.ListPathIssues(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ActivePathIssues(issues)) != 0 {
		t.Fatalf("copy gate should see empty active queue, got %+v", issues)
	}
}
