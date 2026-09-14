// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"context"
	"fmt"
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func TestGetMergedReviewStatsPagesPastFormer100kCap(t *testing.T) {
	t.Parallel()
	database, err := db.Open(db.Options{Path: t.TempDir() + "/stats-page.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if database.Ops() == nil {
		t.Skip("ops required")
	}

	const n = 1200
	writes := make([]opsdb.SealNodeWrite, 0, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("sf%05d", i)
		name := fmt.Sprintf("f%05d", i)
		path := "/" + name
		writes = append(writes, opsdb.SealNodeWrite{
			Side: opsdb.SideSRC,
			Node: opsdb.NodeRecord{
				ID: id, Path: path, Name: name, Type: opsdb.NodeTypeFile, Depth: 1, Size: 1,
			},
			Status: opsdb.StatusRecord{
				TraversalStatus: db.StatusSuccessful,
				CopyStatus:      db.CopyStatusPending,
			},
			Depth: 1, InsertOnly: true,
		})
	}
	if _, err := database.Ops().WriteSealBatch(writes, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	stats, err := GetMergedReviewStats(database, ReviewFilter{
		TypeFilter: "file",
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Truncated {
		t.Fatal("unexpected truncated")
	}
	if stats.Total != n || stats.Files != n {
		t.Fatalf("total=%d files=%d want %d", stats.Total, stats.Files, n)
	}
}

func TestGetMergedReviewStatsTruncatedOnDeadline(t *testing.T) {
	t.Parallel()
	database, err := db.Open(db.Options{Path: t.TempDir() + "/stats-trunc.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if database.Ops() == nil {
		t.Skip("ops required")
	}

	const n = 400
	writes := make([]opsdb.SealNodeWrite, 0, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("st%05d", i)
		name := fmt.Sprintf("t%05d", i)
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
		})
	}
	if _, err := database.Ops().WriteSealBatch(writes, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(2 * time.Millisecond)

	stats, err := getMergedReviewStatsCtx(ctx, database, ReviewFilter{TypeFilter: "file"})
	if err != nil {
		t.Fatal(err)
	}
	if !stats.Truncated {
		t.Fatalf("expected truncated stats, got %+v", stats)
	}
}

func TestListMergedReviewDiffsPageCtxHonorsCancel(t *testing.T) {
	t.Parallel()
	database, err := db.Open(db.Options{Path: t.TempDir() + "/list-cancel.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if database.Ops() == nil {
		t.Skip("ops required")
	}

	writes := make([]opsdb.SealNodeWrite, 0, 100)
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("sc%05d", i)
		name := fmt.Sprintf("c%05d", i)
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
		})
	}
	if _, err := database.Ops().WriteSealBatch(writes, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = ListMergedReviewDiffsPageCtx(ctx, database, ReviewFilter{TypeFilter: "file"}, "path ASC", 50, 0)
	if err == nil {
		t.Fatal("expected cancel error")
	}
}
