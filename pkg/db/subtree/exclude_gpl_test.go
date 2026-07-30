package subtree_test

import (
	"context"
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/subtree"
	_ "codeberg.org/Sylos/Migration-Engine/pkg/db/seal"
)

func TestExcludeUnexcludeCouplesGPLIgnore(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/exclude-gpl.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	folderID := db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/folder")
	fileID := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/folder/bad:name.txt")
	t0 := time.Now().UnixNano()

	if err := database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			nodes := []*db.NodeState{
				{ID: folderID, Path: "/folder", ParentPath: "/", Name: "folder", Type: db.NodeTypeFolder, Depth: 1},
				{ID: fileID, Path: "/folder/bad:name.txt", ParentPath: "/folder", ParentID: folderID, Name: "bad:name.txt", Type: db.NodeTypeFile, Depth: 2},
			}
			if err := w.AppenderInsert(db.TableSrcNodes, nodes); err != nil {
				return err
			}
			return w.BatchInsertSrcStatusEvents([]db.StatusEvent{
				{ID: folderID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, GPLStatus: db.GPLStatusSuccessful, EventTime: t0, Depth: 1},
				{ID: fileID, TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending, GPLStatus: db.GPLStatusFailed, EventTime: t0, Depth: 2},
			})
		})
	}); err != nil {
		t.Fatal(err)
	}

	currentGPL := func(id string) string {
		t.Helper()
		conn, err := database.GetDB()
		if err != nil {
			t.Fatal(err)
		}
		var s string
		if err := conn.QueryRowContext(context.Background(), `
SELECT COALESCE(arg_max(gpl_status, event_time), '')
FROM src_status_events
WHERE id = $1 AND COALESCE(gpl_status, '') <> ''`, id).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}

	if err := database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if _, err := subtree.InsertExclusionEventsForSubtree(w, "SRC", "/folder"); err != nil {
				return err
			}
			return subtree.InsertGPLStatusEventsForSubtree(w, "SRC", "/folder", db.GPLStatusIgnored, true)
		})
	}); err != nil {
		t.Fatal(err)
	}
	if got := currentGPL(fileID); got != db.GPLStatusIgnored {
		t.Fatalf("after exclude gpl=%q want ignored", got)
	}

	if err := database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if _, err := subtree.InsertUnexcludeEventsForSubtree(w, "SRC", "/folder"); err != nil {
				return err
			}
			return subtree.InsertGPLRestoredEventsForSubtree(w, "SRC", "/folder")
		})
	}); err != nil {
		t.Fatal(err)
	}
	if got := currentGPL(fileID); got != db.GPLStatusFailed {
		t.Fatalf("after unexclude gpl=%q want failed", got)
	}
	if got := currentGPL(folderID); got != db.GPLStatusSuccessful {
		t.Fatalf("after unexclude folder gpl=%q want successful", got)
	}
}
