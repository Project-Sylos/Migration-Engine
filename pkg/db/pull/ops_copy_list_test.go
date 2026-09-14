package pull_test

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func TestListSrcNodesByCopyStatusOpsSealedOnly(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/copy-list.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if database.Ops() == nil {
		t.Skip("ops store required")
	}

	writes := []opsdb.SealNodeWrite{
		{Side: opsdb.SideSRC, Node: opsdb.NodeRecord{ID: "done", Path: "/done.txt", Name: "done.txt", Type: opsdb.NodeTypeFile}, Status: opsdb.StatusRecord{TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful}, Depth: 1, InsertOnly: true},
		{Side: opsdb.SideSRC, Node: opsdb.NodeRecord{ID: "pend", Path: "/pend.txt", Name: "pend.txt", Type: opsdb.NodeTypeFile}, Status: opsdb.StatusRecord{TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending}, Depth: 1, InsertOnly: true},
	}
	if _, err := database.Ops().WriteSealBatch(writes, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	nodes, err := pull.ListSrcNodesByCopyStatus(database, db.CopyStatusSuccessful, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Fatalf("len=%d want 1 sealed copy-complete node", len(nodes))
	}
	if nodes[0].ID != "done" {
		t.Fatalf("id=%q want done", nodes[0].ID)
	}
}
