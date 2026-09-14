package filterapply_test

import (
	"fmt"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/filterapply"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/review"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func TestApplySearchUnexclusionRestoresInheritedSubtree(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/search-unexclude.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if database.Ops() == nil {
		t.Skip("ops store required")
	}

	writes := []opsdb.SealNodeWrite{
		{
			Side: opsdb.SideSRC, Node: opsdb.NodeRecord{ID: "p", Path: "/p", Name: "p", Type: opsdb.NodeTypeFolder, Depth: 1},
			Status: opsdb.StatusRecord{TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending},
			Depth:  1, InsertOnly: true,
			Deltas: []opsdb.PendingDelta{{Phase: opsdb.PhaseCopy, NodeType: opsdb.NodeTypeFolder, Add: true}},
		},
		{
			Side: opsdb.SideSRC, Node: opsdb.NodeRecord{ID: "c", Path: "/p/c", Name: "c", Type: opsdb.NodeTypeFile, ParentID: "p", Depth: 2},
			Status: opsdb.StatusRecord{TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending},
			Depth:  2, InsertOnly: true,
			Deltas: []opsdb.PendingDelta{{Phase: opsdb.PhaseCopy, NodeType: opsdb.NodeTypeFile, Add: true}},
		},
	}
	if _, err := database.Ops().WriteSealBatch(writes, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ApplySubtreeCopyExclusion("/p", true, nil); err != nil {
		t.Fatal(err)
	}
	if got, err := pull.ListNodesCopyKeyset(database, 1, db.NodeTypeFolder, "", 10, db.CopyStatusPending); err != nil || len(got) != 0 {
		t.Fatalf("folder copy pull after exclude %+v err=%v", got, err)
	}
	if got, err := pull.ListNodesCopyKeyset(database, 2, db.NodeTypeFile, "", 10, db.CopyStatusPending); err != nil || len(got) != 0 {
		t.Fatalf("file copy pull after exclude %+v err=%v", got, err)
	}

	f := review.ReviewFilter{CopyStatus: db.CopyStatusExcluded, StatusSearchType: "copy"}
	mut, err := filterapply.ApplySearchUnexclusionOps(database, f, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if mut.Affected != 2 {
		t.Fatalf("affected=%d want 2", mut.Affected)
	}
	for _, id := range []string{"p", "c"} {
		n, err := dbOpenNode(database, id)
		if err != nil {
			t.Fatal(err)
		}
		if n.Excluded || n.CopyStatus != db.CopyStatusPending {
			t.Fatalf("node %s copy=%q excluded=%v", id, n.CopyStatus, n.Excluded)
		}
	}
	folders, err := pull.ListNodesCopyKeyset(database, 1, db.NodeTypeFolder, "", 10, db.CopyStatusPending)
	if err != nil {
		t.Fatal(err)
	}
	if len(folders) != 1 || folders[0].State == nil || folders[0].State.ID != "p" {
		t.Fatalf("folder copy pull %+v", folders)
	}
	files, err := pull.ListNodesCopyKeyset(database, 2, db.NodeTypeFile, "", 10, db.CopyStatusPending)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].State == nil || files[0].State.ID != "c" {
		t.Fatalf("file copy pull %+v", files)
	}
}

func dbOpenNode(database *db.DB, id string) (*db.NodeState, error) {
	st, ok, err := database.Ops().GetStatus(opsdb.SideSRC, id)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("status not found for %s", id)
	}
	nrec, ok, err := database.Ops().GetNode(opsdb.SideSRC, id)
	if err != nil || !ok {
		return nil, err
	}
	n := &db.NodeState{
		ID: nrec.ID, Path: nrec.Path, Type: nrec.Type, Size: nrec.Size, Depth: nrec.Depth,
	}
	db.HydrateNodeFromOps(n, st)
	return n, nil
}
