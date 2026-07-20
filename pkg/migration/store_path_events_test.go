// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func TestPathEventsReviewAccept(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/path_events.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	srcRoot := db.RootNodeID("SRC")
	nodeID := db.MintNodeID("SRC", srcRoot, db.NodeTypeFile, "bad*name.txt")
	if err := database.RunWrite(t.Context(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert("src_nodes", []*db.NodeState{{
				ID: nodeID, ServiceID: "s1", ParentID: srcRoot, Path: "/bad*name.txt", ParentPath: "/",
				Name: "bad*name.txt", Type: db.NodeTypeFile, Depth: 1,
			}}); err != nil {
				return err
			}
			return w.BatchInsertPathEvents([]db.PathEvent{{
				ID: nodeID, EventTime: time.Now().UnixNano(), Category: db.PathEventCategoryGPLClean,
				ProposedPath: "bad_name.txt", Status: db.PathEventStatusPending, GPLIssues: `[{"category":"InvalidChar"}]`,
			}})
		})
	}); err != nil {
		t.Fatal(err)
	}

	m := &Migration{DB: database}
	issues, err := m.ListPathIssues(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 1 || issues[0].ProposedPath != "bad_name.txt" {
		t.Fatalf("issues=%+v", issues)
	}
	if err := m.AcceptPathProposal(nodeID, "bad_name.txt"); err != nil {
		t.Fatal(err)
	}
	issues, err = m.ListPathIssues(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 0 {
		t.Fatalf("expected no pending after accept, got %+v", issues)
	}
}

func TestValidatePathProposal_andForceIgnore(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/path_validate.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	srcRoot := db.RootNodeID("SRC")
	nodeID := db.MintNodeID("SRC", srcRoot, db.NodeTypeFile, "bad*name.txt")
	childID := db.MintNodeID("SRC", nodeID, db.NodeTypeFile, "nested.txt")
	if err := database.RunWrite(t.Context(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert("src_nodes", []*db.NodeState{
				{ID: srcRoot, ServiceID: "/", Path: "/", Type: db.NodeTypeFolder, Depth: 0,
					GPLState: `{"valid":true,"part":{"valid":true},"path":{"valid":true},"parts":[]}`},
				{ID: nodeID, ServiceID: "s1", ParentID: srcRoot, Path: "/bad*name.txt", ParentPath: "/",
					Name: "bad*name.txt", Type: db.NodeTypeFile, Depth: 1},
				{ID: childID, ServiceID: "c1", ParentID: nodeID, Path: "/bad*name.txt/nested.txt", ParentPath: "/bad*name.txt",
					Name: "nested.txt", Type: db.NodeTypeFile, Depth: 2},
			}); err != nil {
				return err
			}
			return w.BatchInsertPathEvents([]db.PathEvent{{
				ID: nodeID, EventTime: time.Now().UnixNano(), Category: db.PathEventCategoryGPLClean,
				ProposedPath: "bad_name.txt", Status: db.PathEventStatusPending, GPLIssues: `[{"category":"InvalidChar"}]`,
			}})
		})
	}); err != nil {
		t.Fatal(err)
	}

	m := &Migration{DB: database}
	m.setLastRunConfig(Config{
		Destination: Service{ProviderID: "windows"},
	})
	valid, err := m.ValidatePathProposal(nodeID, "ok_name.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !valid.Valid {
		t.Fatalf("expected valid proposal, got issues=%+v", valid.Issues)
	}
	invalid, err := m.ValidatePathProposal(nodeID, "still*bad.txt")
	if err != nil {
		t.Fatal(err)
	}
	if invalid.Valid || len(invalid.Issues) == 0 {
		t.Fatalf("expected invalid proposal with issues, got %+v", invalid)
	}

	if err := m.RemapPathManual(nodeID, "still*bad.txt", true); err != nil {
		t.Fatal(err)
	}
	issues, err := m.ListPathIssues(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 0 {
		t.Fatalf("force remap should hide ignored subtree issues, got %+v", issues)
	}

	conn, err := database.GetDB()
	if err != nil {
		t.Fatal(err)
	}
	var nodeGPL, childGPL string
	_ = conn.QueryRowContext(t.Context(),
		`SELECT COALESCE(arg_max(gpl_status, event_time),'') FROM src_status_events WHERE id = $1 AND COALESCE(gpl_status,'') <> ''`, nodeID,
	).Scan(&nodeGPL)
	_ = conn.QueryRowContext(t.Context(),
		`SELECT COALESCE(arg_max(gpl_status, event_time),'') FROM src_status_events WHERE id = $1 AND COALESCE(gpl_status,'') <> ''`, childID,
	).Scan(&childGPL)
	if nodeGPL != db.GPLStatusIgnored {
		t.Fatalf("node gpl_status=%q want ignored", nodeGPL)
	}
	if childGPL != db.GPLStatusIgnored {
		t.Fatalf("child gpl_status=%q want ignored", childGPL)
	}
}

func TestIgnoreAllAndAcceptAllPathProposals(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/path_bulk.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	srcRoot := db.RootNodeID("SRC")
	aID := db.MintNodeID("SRC", srcRoot, db.NodeTypeFile, "a*1.txt")
	bID := db.MintNodeID("SRC", srcRoot, db.NodeTypeFile, "b*2.txt")
	if err := database.RunWrite(t.Context(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert("src_nodes", []*db.NodeState{
				{ID: srcRoot, ServiceID: "/", Path: "/", Type: db.NodeTypeFolder, Depth: 0,
					GPLState: `{"valid":true,"part":{"valid":true},"path":{"valid":true},"parts":[]}`},
				{ID: aID, ServiceID: "a", ParentID: srcRoot, Path: "/a*1.txt", ParentPath: "/",
					Name: "a*1.txt", Type: db.NodeTypeFile, Depth: 1},
				{ID: bID, ServiceID: "b", ParentID: srcRoot, Path: "/b*2.txt", ParentPath: "/",
					Name: "b*2.txt", Type: db.NodeTypeFile, Depth: 1},
			}); err != nil {
				return err
			}
			return w.BatchInsertPathEvents([]db.PathEvent{
				{ID: aID, EventTime: time.Now().UnixNano(), Category: db.PathEventCategoryGPLClean,
					ProposedPath: "a_1.txt", Status: db.PathEventStatusPending},
				{ID: bID, EventTime: time.Now().UnixNano() + 1, Category: db.PathEventCategoryGPLClean,
					ProposedPath: "b_2.txt", Status: db.PathEventStatusPending},
			})
		})
	}); err != nil {
		t.Fatal(err)
	}

	m := &Migration{DB: database}
	n, err := m.AcceptAllPathProposals()
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("accepted=%d want 2", n)
	}
	issues, err := m.ListPathIssues(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 0 {
		t.Fatalf("expected empty queue after accept-all, got %+v", issues)
	}

	// Seed a fresh pending issue and ignore-all.
	cID := db.MintNodeID("SRC", srcRoot, db.NodeTypeFile, "c*3.txt")
	if err := database.RunWrite(t.Context(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert("src_nodes", []*db.NodeState{
				{ID: cID, ServiceID: "c", ParentID: srcRoot, Path: "/c*3.txt", ParentPath: "/",
					Name: "c*3.txt", Type: db.NodeTypeFile, Depth: 1},
			}); err != nil {
				return err
			}
			return w.BatchInsertPathEvents([]db.PathEvent{{
				ID: cID, EventTime: time.Now().UnixNano(), Category: db.PathEventCategoryGPLClean,
				ProposedPath: "c_3.txt", Status: db.PathEventStatusPending,
			}})
		})
	}); err != nil {
		t.Fatal(err)
	}
	ignored, err := m.IgnoreAllPathIssues()
	if err != nil {
		t.Fatal(err)
	}
	if ignored != 1 {
		t.Fatalf("ignored=%d want 1", ignored)
	}
	issues, err = m.ListPathIssues(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 0 {
		t.Fatalf("expected empty queue after ignore-all, got %+v", issues)
	}
}
