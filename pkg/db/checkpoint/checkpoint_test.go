// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package checkpoint

import (
	"context"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

func TestGetTransferCheckpointFreshInsertNil(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/xfer-checkpoint.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	id := db.DeterministicNodeID("SRC", db.NodeTypeFile, "/a.txt")
	node := &db.NodeState{
		ID: id, ServiceID: "s1", Path: "/a.txt", ParentPath: "/", Name: "a.txt",
		Type: db.NodeTypeFile, Size: 10, MTime: "t", Depth: 1,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending,
	}
	if err := database.AppendDiscoveredNodes([]db.InsertOperation{{
		QueueType: "SRC", Level: 1, Status: db.StatusSuccessful, State: node,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := database.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	ckpt, err := GetTransferCheckpoint(database, context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if ckpt != nil {
		t.Fatalf("fresh insert should have nil checkpoint, got %+v", ckpt)
	}
}
