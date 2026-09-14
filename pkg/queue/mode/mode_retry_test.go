// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package mode

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
)

func TestPullRetryTasks_pastMaxKnownDepthRecordsPull(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/retry-past-depth.db"})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	q := queue.NewQueue("src", 3, 1, nil, nil)
	q.SetDatabase(database)
	q.SetMode(queue.QueueModeRetry)
	q.SetState(queue.QueueStateRunning)
	q.SetMaxKnownDepth(2)
	q.SetRound(3)
	q.SetTraversalCacheLoaded(true)

	res := PullRetryTasks(q, true)
	if !res.OK() {
		t.Fatalf("past maxKnownDepth pull: status=%v want PullOK (nested pull lock would Skip)", res.Status)
	}
	if !q.RoundHasCountedPull(3) {
		t.Fatal("expected counted pull on round past maxKnownDepth")
	}
	if !res.Partial || res.Yield != 0 {
		t.Fatalf("empty frontier: partial=%v yield=%d", res.Partial, res.Yield)
	}
}
