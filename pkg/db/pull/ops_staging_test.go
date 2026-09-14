package pull_test

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func TestGetNodeByIDReflectsCopyExclusion(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/copy-exclude.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if database.Ops() == nil {
		t.Skip("ops store required")
	}

	id := "node-1"
	writes := []opsdb.SealNodeWrite{{
		Side: opsdb.SideSRC,
		Node: opsdb.NodeRecord{
			ID: id, Path: "/folder/item.txt", Name: "item.txt", Type: opsdb.NodeTypeFile, Size: 42, Depth: 2,
		},
		Status: opsdb.StatusRecord{TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending},
		Depth:  2,
		Deltas: []opsdb.PendingDelta{{Phase: opsdb.PhaseCopy, NodeType: opsdb.NodeTypeFile, Add: true}},
	}}
	if _, err := database.Ops().WriteSealBatch(writes, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	before, err := pull.GetNodeByID(database, "SRC", id)
	if err != nil {
		t.Fatal(err)
	}
	if before.Excluded || before.CopyStatus != db.CopyStatusPending {
		t.Fatalf("before exclude: copy=%q excluded=%v", before.CopyStatus, before.Excluded)
	}

	if _, err := database.ApplyNodeCopyExclusion(id, true); err != nil {
		t.Fatal(err)
	}

	after, err := pull.GetNodeByID(database, "SRC", id)
	if err != nil {
		t.Fatal(err)
	}
	if !after.Excluded || after.CopyStatus != db.CopyStatusExcludedExplicit {
		t.Fatalf("after exclude: copy=%q excluded=%v", after.CopyStatus, after.Excluded)
	}
	got, err := pull.ListNodesCopyKeyset(database, 2, db.NodeTypeFile, "", 10, db.CopyStatusPending)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("copy pull after exclude: %d rows", len(got))
	}

	if _, err := database.ApplyNodeCopyExclusion(id, false); err != nil {
		t.Fatal(err)
	}

	restored, err := pull.GetNodeByID(database, "SRC", id)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Excluded || restored.CopyStatus != db.CopyStatusPending {
		t.Fatalf("after unexclude: copy=%q excluded=%v", restored.CopyStatus, restored.Excluded)
	}
	got, err = pull.ListNodesCopyKeyset(database, 2, db.NodeTypeFile, "", 10, db.CopyStatusPending)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].State == nil || got[0].State.ID != id {
		t.Fatalf("copy pull after unexclude: %+v", got)
	}
}
