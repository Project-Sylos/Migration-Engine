// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package filterapply_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/filterapply"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/review"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func TestApplySearchExclusionOpsPagesPastPageSize(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/search-exclude-pages.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if database.Ops() == nil {
		t.Skip("ops store required")
	}
	database.SetDisableQueryTimeout(true)

	const n = 1200
	writes := make([]opsdb.SealNodeWrite, 0, n)
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("se%05d", i)
		ids = append(ids, id)
		name := fmt.Sprintf("e%05d", i)
		writes = append(writes, opsdb.SealNodeWrite{
			Side: opsdb.SideSRC,
			Node: opsdb.NodeRecord{
				ID: id, Path: "/" + name, Name: name, Type: opsdb.NodeTypeFile, Depth: 1, Size: 1,
			},
			Status: opsdb.StatusRecord{
				TraversalStatus: db.StatusSuccessful,
				CopyStatus:      db.CopyStatusPending,
			},
			Depth: 1, InsertOnly: true,
			Deltas: []opsdb.PendingDelta{{Phase: opsdb.PhaseCopy, NodeType: opsdb.NodeTypeFile, Add: true}},
		})
	}
	if _, err := database.Ops().WriteSealBatch(writes, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	f := review.ReviewFilter{TypeFilter: "file"}
	mut, err := filterapply.ApplySearchExclusionOps(database, f, `{"type":"file"}`, nil, "app-pages", time.Now().UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	if mut.Affected != n {
		t.Fatalf("affected=%d want %d", mut.Affected, n)
	}
	for _, id := range ids {
		st, ok, err := database.Ops().GetStatus(opsdb.SideSRC, id)
		if err != nil || !ok {
			t.Fatalf("status %s ok=%v err=%v", id, ok, err)
		}
		if st.CopyStatus != db.CopyStatusExcludedExplicit {
			t.Fatalf("node %s copy=%q want excluded_explicit", id, st.CopyStatus)
		}
	}
}

func TestApplySearchExclusionOpsTimeoutMidApply(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/search-exclude-timeout.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if database.Ops() == nil {
		t.Skip("ops store required")
	}

	const n = 800
	writes := make([]opsdb.SealNodeWrite, 0, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("sx%05d", i)
		name := fmt.Sprintf("x%05d", i)
		writes = append(writes, opsdb.SealNodeWrite{
			Side: opsdb.SideSRC,
			Node: opsdb.NodeRecord{
				ID: id, Path: "/" + name, Name: name, Type: opsdb.NodeTypeFile, Depth: 1,
			},
			Status: opsdb.StatusRecord{
				TraversalStatus: db.StatusSuccessful,
				CopyStatus:      db.CopyStatusPending,
			},
			Depth: 1, InsertOnly: true,
			Deltas: []opsdb.PendingDelta{{Phase: opsdb.PhaseCopy, NodeType: opsdb.NodeTypeFile, Add: true}},
		})
	}
	if _, err := database.Ops().WriteSealBatch(writes, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(2 * time.Millisecond)

	_, err = filterapply.ApplySearchExclusionOpsCtx(
		ctx, database, review.ReviewFilter{TypeFilter: "file"}, `{"type":"file"}`, nil, "app-to", time.Now().UnixNano(),
	)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !errors.Is(err, filterapply.ErrSearchApplyTimedOut) {
		t.Fatalf("got %v want ErrSearchApplyTimedOut", err)
	}
}
