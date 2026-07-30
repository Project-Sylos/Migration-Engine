// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package checkpoint

import (
	"context"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	_ "codeberg.org/Sylos/Migration-Engine/pkg/db/seal"
)

func TestSrcNodesAppenderAcceptsXferColumns(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/xfer-append.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	node := &db.NodeState{
		ID: "n1", ServiceID: "s1", Path: "/a", ParentPath: "/", Type: db.NodeTypeFile, Size: 10, MTime: "t", Depth: 1,
	}
	if got := len(db.NodeStateAppendRowArgsForTable(db.TableSrcNodes, node)); got != 16 {
		t.Fatalf("src_nodes appender args want 16, got %d", got)
	}
	if got := len(db.NodeStateAppendRowArgsForTable(db.TableDstNodes, node)); got != 11 {
		t.Fatalf("dst_nodes appender args want 11, got %d", got)
	}

	if err := database.SealLevel("SRC", 1, []*db.NodeState{node}, 1, 0, 0, 0, -1, -1, -1); err != nil {
		t.Fatalf("SealLevel enqueue: %v", err)
	}
	if err := database.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	ckpt, err := GetTransferCheckpoint(database, context.Background(), "n1")
	if err != nil {
		t.Fatal(err)
	}
	if ckpt != nil {
		t.Fatalf("fresh insert should have nil checkpoint, got %+v", ckpt)
	}
}
