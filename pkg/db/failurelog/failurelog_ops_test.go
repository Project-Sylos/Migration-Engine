// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package failurelog_test

import (
	"context"
	"path/filepath"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/failurelog"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func TestFailureLogOpsStore(t *testing.T) {
	dir := t.TempDir()
	database, err := db.Open(db.Options{
		Path:   filepath.Join(dir, "fail.db"),
		OpsDir: filepath.Join(dir, "fail.ops"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if database.Ops() == nil {
		t.Fatal("expected ops store")
	}

	ev := db.StatusEvent{ID: "node-a", TraversalStatus: db.StatusFailed, Depth: 1}
	failurelog.AttachTaskFailureLog(&ev, "traversal", "src", "node-a", "/a", 2, "permission denied")
	if err := db.FlushFailureLogEvent(database, ev, "src"); err != nil {
		t.Fatal(err)
	}
	st, ok, err := database.Ops().GetStatus(opsdb.SideSRC, "node-a")
	if err != nil {
		t.Fatal(err)
	}
	_ = ok
	st.ErrorLogID = ev.ErrorLogID
	if err := database.Ops().PutStatus(opsdb.SideSRC, "node-a", st); err != nil {
		t.Fatal(err)
	}

	byNode, err := failurelog.LatestFailureLogIDsByNodeIDs(context.Background(), database, db.TableSrcStatusEvents, []string{"node-a"})
	if err != nil {
		t.Fatal(err)
	}
	if byNode["node-a"] != ev.ErrorLogID {
		t.Fatalf("logID=%q want %s", byNode["node-a"], ev.ErrorLogID)
	}

	logs, err := failurelog.GetFailureLogsByIDs(context.Background(), database, []string{ev.ErrorLogID})
	if err != nil {
		t.Fatal(err)
	}
	log, ok := logs[ev.ErrorLogID]
	if !ok {
		t.Fatal("missing failure log")
	}
	if failurelog.FailureLogDisplayText(log) != "permission denied" {
		t.Fatalf("display=%q", failurelog.FailureLogDisplayText(log))
	}
}
