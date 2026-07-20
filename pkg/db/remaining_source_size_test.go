// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"testing"
	"time"
)

func TestGetRemainingSourceSizeAfterDelete(t *testing.T) {
	database, err := Open(Options{Path: t.TempDir() + "/remaining-src-size.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	type testNode struct {
		path string
		size int64
	}
	nodeDefs := []testNode{
		{path: "/deleted.bin", size: 100},
		{path: "/pending.bin", size: 200},
		{path: "/failed.bin", size: 300},
		{path: "/skipped.bin", size: 400},
		{path: "/unset.bin", size: 500},
		{path: "/retried.bin", size: 600},
	}
	nodes := make([]*NodeState, 0, len(nodeDefs))
	for _, def := range nodeDefs {
		nodes = append(nodes, &NodeState{
			ID:         DeterministicNodeID("SRC", NodeTypeFile, def.path),
			Path:       def.path,
			ParentPath: "/",
			Name:       def.path[1:],
			Type:       NodeTypeFile,
			Depth:      1,
			Size:       def.size,
		})
	}

	now := time.Now().UnixNano()
	events := []StatusEvent{
		{ID: nodes[0].ID, DeleteStatus: DeleteStatusDeleted, EventTime: now, Depth: 1},
		{ID: nodes[1].ID, DeleteStatus: DeleteStatusPending, EventTime: now, Depth: 1},
		{ID: nodes[2].ID, DeleteStatus: DeleteStatusFailed, EventTime: now, Depth: 1},
		{ID: nodes[3].ID, DeleteStatus: DeleteStatusSkipped, EventTime: now, Depth: 1},
		// nodes[4] intentionally has no delete event.
		{ID: nodes[5].ID, DeleteStatus: DeleteStatusDeleted, EventTime: now, Depth: 1},
		{ID: nodes[5].ID, DeleteStatus: DeleteStatusFailed, EventTime: now + 1, Depth: 1},
	}

	err = database.RunWrite(context.Background(), func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			if err := w.AppenderInsert(tableSrcNodes, nodes); err != nil {
				return err
			}
			return w.BatchInsertSrcStatusEvents(events)
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := database.GetRemainingSourceSizeAfterDelete()
	if err != nil {
		t.Fatal(err)
	}
	const want = int64(200 + 300 + 400 + 500 + 600)
	if got != want {
		t.Fatalf("remaining source size = %d, want %d", got, want)
	}
}
