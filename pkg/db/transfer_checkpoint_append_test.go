// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"testing"
)

func TestSrcNodesAppenderAcceptsXferColumns(t *testing.T) {
	database, err := Open(Options{Path: t.TempDir() + "/xfer-append.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	node := &NodeState{
		ID: "n1", ServiceID: "s1", Path: "/a", ParentPath: "/", Type: NodeTypeFile, Size: 10, MTime: "t", Depth: 1,
	}
	if got := len(NodeStateAppendRowArgsForTable(tableSrcNodes, node)); got != 15 {
		t.Fatalf("src_nodes appender args want 15, got %d", got)
	}
	if got := len(NodeStateAppendRowArgsForTable(tableDstNodes, node)); got != 10 {
		t.Fatalf("dst_nodes appender args want 10, got %d", got)
	}

	if err := database.SealLevel("SRC", 1, []*NodeState{node}, 1, 0, 0, 0, -1, -1, -1); err != nil {
		t.Fatalf("SealLevel enqueue: %v", err)
	}
	if err := database.FlushSealBuffer(); err != nil {
		t.Fatalf("FlushSealBuffer: %v", err)
	}

	ckpt, err := database.GetTransferCheckpoint(context.Background(), "n1")
	if err != nil {
		t.Fatal(err)
	}
	if ckpt != nil {
		t.Fatalf("fresh insert should have nil checkpoint, got %+v", ckpt)
	}
}
