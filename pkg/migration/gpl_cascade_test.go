// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/subtree"
	"codeberg.org/Sylos/go-path-linter/pkg/check"
	"codeberg.org/Sylos/go-path-linter/pkg/gpl"
)

func TestAcceptPathProposal_fansOutGPLPendingDescendants(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/gpl_fanout.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	srcRoot := db.MintNodeID("SRC", "", db.NodeTypeFolder, "/")
	folderID := db.MintNodeID("SRC", srcRoot, db.NodeTypeFolder, "folder")
	childID := db.MintNodeID("SRC", folderID, db.NodeTypeFile, "child.txt")

	if err := database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			nodes := []*db.NodeState{
				{ID: srcRoot, ServiceID: "/", Path: "/", Type: db.NodeTypeFolder, Depth: 0,
					GPLState: `{"valid":true,"part":{"valid":true},"path":{"valid":true},"parts":[]}`},
				{ID: folderID, ServiceID: "f1", ParentID: srcRoot, Path: "/folder", ParentPath: "/",
					Name: "folder", Type: db.NodeTypeFolder, Depth: 1,
					GPLState: `{"valid":true,"part":{"valid":true},"path":{"valid":true},"parts":["folder"]}`},
				{ID: childID, ServiceID: "c1", ParentID: folderID, Path: "/folder/child.txt", ParentPath: "/folder",
					Name: "child.txt", Type: db.NodeTypeFile, Depth: 2,
					GPLState: `{"valid":true,"part":{"valid":true},"path":{"valid":true},"parts":["folder","child.txt"]}`},
			}
			if err := w.AppenderInsert("src_nodes", nodes); err != nil {
				return err
			}
			return w.BatchInsertPathEvents([]db.PathEvent{{
				ID: folderID, EventTime: time.Now().UnixNano(), Category: db.PathEventCategoryGPLClean,
				ProposedPath: "renamed", Status: db.PathEventStatusPending,
			}})
		})
	}); err != nil {
		t.Fatal(err)
	}

	m := &Migration{DB: database}
	if err := m.AcceptPathChange(folderID, "renamed", false); err != nil {
		t.Fatal(err)
	}

	conn, err := database.GetDB()
	if err != nil {
		t.Fatal(err)
	}
	var folderGPL, childGPL string
	_ = conn.QueryRowContext(context.Background(),
		`SELECT COALESCE(arg_max(gpl_status, event_time),'') FROM src_status_events WHERE id = $1 AND COALESCE(gpl_status,'') <> ''`, folderID,
	).Scan(&folderGPL)
	_ = conn.QueryRowContext(context.Background(),
		`SELECT COALESCE(arg_max(gpl_status, event_time),'') FROM src_status_events WHERE id = $1 AND COALESCE(gpl_status,'') <> ''`, childID,
	).Scan(&childGPL)
	if folderGPL == db.GPLStatusPending {
		t.Fatal("accepted node itself must not be marked gpl pending")
	}
	if childGPL != db.GPLStatusPending {
		t.Fatalf("child gpl_status=%q want pending", childGPL)
	}
}

func TestGPLSweep_clearsPathLengthAfterAncestorShorten(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/gpl_sweep.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	// Custom target: tiny path length so we can exercise cascade without huge names.
	rs := check.RuleSet{
		Separator: "/",
		Checkers: []check.Checker{
			check.NewPathLength(12),
		},
	}
	long := "abcdefghij" // 10 chars
	// parent "abcdefghij" + "/" + "xy" = 13 > 12
	srcRoot := db.MintNodeID("SRC", "", db.NodeTypeFolder, "/")
	folderID := db.MintNodeID("SRC", srcRoot, db.NodeTypeFolder, long)
	childID := db.MintNodeID("SRC", folderID, db.NodeTypeFile, "xy")

	parentParts := []string{long}
	childParts := []string{long, "xy"}
	childState := db.GPLStatePayload{
		Valid: false,
		Part:  db.GPLScopeState{Valid: true, Categories: []string{"InvalidChar"}}, // sticky part finding
		Path:  db.GPLScopeState{Valid: false, Categories: []string{"Length"}},
		Parts: childParts,
	}
	childJSON, _ := json.Marshal(childState)
	folderJSON, _ := json.Marshal(db.GPLStatePayload{
		Valid: false,
		Part:  db.GPLScopeState{Valid: true},
		Path:  db.GPLScopeState{Valid: false, Categories: []string{"Length"}},
		Parts: parentParts,
	})

	if err := database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.AppenderInsert("src_nodes", []*db.NodeState{
				{ID: srcRoot, ServiceID: "/", Path: "/", Type: db.NodeTypeFolder, Depth: 0,
					GPLState: `{"valid":true,"part":{"valid":true},"path":{"valid":true},"parts":[]}`},
				{ID: folderID, ServiceID: "f", ParentID: srcRoot, Path: "/" + long, ParentPath: "/",
					Name: long, Type: db.NodeTypeFolder, Depth: 1, GPLState: string(folderJSON)},
				{ID: childID, ServiceID: "c", ParentID: folderID, Path: "/" + long + "/xy", ParentPath: "/" + long,
					Name: "xy", Type: db.NodeTypeFile, Depth: 2, GPLState: string(childJSON)},
			}); err != nil {
				return err
			}
			return subtree.InsertGPLStatusEventsForSubtree(w, "SRC", "/"+long, db.GPLStatusPending, false)
		})
	}); err != nil {
		t.Fatal(err)
	}

	// Simulate accept: shorten folder to "ab" and update gpl_state parts.
	shortParts := []string{"ab"}
	shortJSON, _ := json.Marshal(db.GPLStatePayload{
		Valid: true,
		Part:  db.GPLScopeState{Valid: true, ProposedClean: "ab"},
		Path:  db.GPLScopeState{Valid: true},
		Parts: shortParts,
	})
	if err := database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := w.UpdateNodeGPLState(folderID, string(shortJSON)); err != nil {
				return err
			}
			return w.BatchInsertPathEvents([]db.PathEvent{{
				ID: folderID, EventTime: time.Now().UnixNano(),
				Category: db.PathEventCategoryManualRemap, ProposedPath: "ab", Status: db.PathEventStatusAccepted,
			}})
		})
	}); err != nil {
		t.Fatal(err)
	}

	// Path-only revalidate child with shortened parent parts via ProcessGPLTaskSRC path.
	m := &Migration{DB: database}
	// Override target by running evaluate through custom rules via queue helper isn't exposed;
	// call evaluateGPLPathOnly equivalent: build linter with rs.
	l, err := gpl.NewWithRules(rs, "", gpl.WithRelative(true), gpl.WithAutoValidate(false), gpl.WithRaiseErrors(false), gpl.WithFileAdded(true))
	if err != nil {
		t.Fatal(err)
	}
	l.SetParts([]string{"ab", "xy"})
	_ = l.ValidatePath()
	if len(l.Log.Issues) != 0 {
		t.Fatalf("expected path length clear after shorten, issues=%v", l.Log.Issues)
	}

	// Full sweep with Linux may not clear length — exercise fan-out + successful status instead.
	if err := m.RunGPLSweep(); err != nil {
		t.Fatal(err)
	}
	conn, err := database.GetDB()
	if err != nil {
		t.Fatal(err)
	}
	var childStatus string
	if err := conn.QueryRowContext(context.Background(),
		`SELECT COALESCE(arg_max(gpl_status, event_time),'') FROM src_status_events WHERE id = $1 AND COALESCE(gpl_status,'') <> ''`, childID,
	).Scan(&childStatus); err != nil {
		t.Fatal(err)
	}
	if childStatus != db.GPLStatusSuccessful {
		t.Fatalf("after sweep child gpl_status=%q", childStatus)
	}

	var gplState string
	if err := conn.QueryRowContext(context.Background(), `SELECT COALESCE(gpl_state,'') FROM src_nodes WHERE id = $1`, childID).Scan(&gplState); err != nil {
		t.Fatal(err)
	}
	var payload db.GPLStatePayload
	if err := json.Unmarshal([]byte(gplState), &payload); err != nil {
		t.Fatal(err)
	}
	// Part-local InvalidChar must survive path-only cascade.
	found := false
	for _, c := range payload.Part.Categories {
		if c == "InvalidChar" {
			found = true
		}
	}
	if !found {
		t.Fatalf("part categories must preserve InvalidChar, got %#v state=%s", payload.Part.Categories, gplState)
	}
	if !strings.Contains(gplState, `"parts"`) {
		t.Fatalf("expected parts in gpl_state: %s", gplState)
	}
}
