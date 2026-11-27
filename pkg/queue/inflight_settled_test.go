// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"testing"
	"time"

	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func TestSnapshotSettledWindowKeepsActiveAndSettlesLeavers(t *testing.T) {
	q := NewQueue("copy", 3, 1, nil, nil)
	t1 := &TaskBase{
		ID:          "a",
		Type:        TaskTypeCopyFile,
		DisplayPath: "/a.txt",
		LeaseTime:   time.Now().Add(-2 * time.Second),
		File:        types.File{Size: 100, DisplayName: "a.txt", Type: types.NodeTypeFile},
	}
	t2 := &TaskBase{
		ID:          "b",
		Type:        TaskTypeCopyFile,
		DisplayPath: "/b.txt",
		LeaseTime:   time.Now(),
		File:        types.File{Size: 50, DisplayName: "b.txt", Type: types.NodeTypeFile},
	}

	q.AddInProgress("a", t1)
	q.AddInProgress("b", t2)

	active1, settled1 := q.SnapshotSettledWindow(24)
	if len(active1) != 2 || len(settled1) != 0 {
		t.Fatalf("first snapshot: active=%d settled=%d", len(active1), len(settled1))
	}

	q.RememberSettledOutcome(t1, true)
	q.RemoveInProgress("a")

	active2, settled2 := q.SnapshotSettledWindow(24)
	if len(active2) != 1 {
		t.Fatalf("second snapshot active=%d want 1", len(active2))
	}
	if active2[0].NodeID != "b" {
		t.Fatalf("still-active id=%s want b", active2[0].NodeID)
	}
	if len(settled2) != 1 || settled2[0].NodeID != "a" || !settled2[0].Success {
		t.Fatalf("settled=%v", settled2)
	}

	active3, settled3 := q.SnapshotSettledWindow(24)
	if len(active3) != 1 || len(settled3) != 0 {
		t.Fatalf("third snapshot must forget settled: active=%d settled=%d", len(active3), len(settled3))
	}
}

func TestRequestUserCancelArmsLease(t *testing.T) {
	prev := userCancelGraceOverride
	userCancelGraceOverride = 20 * time.Millisecond
	defer func() { userCancelGraceOverride = prev }()

	q := NewQueue("src", 3, 1, nil, nil)
	q.SetMode(QueueModeTraversal)
	task := &TaskBase{
		ID:          "c1",
		Type:        TaskTypeSrcTraversal,
		DisplayPath: "/hung",
		Round:       1,
		Folder:      types.Folder{ServiceID: "s1", DisplayName: "hung", Type: types.NodeTypeFolder},
	}
	q.AddInProgress("c1", task)
	q.BindLeaseOwner(task, "src-worker-0")

	cancelled := false
	q.BindLeaseCancel("c1", func() { cancelled = true })
	if !q.RequestUserCancel("c1") {
		t.Fatal("expected cancel armed")
	}
	if cancelled {
		t.Fatal("soft cancel must not hard-cancel the lease immediately")
	}
	if !task.UserCancelRequested {
		t.Fatal("expected UserCancelRequested")
	}
	if task.UserCancelHard {
		t.Fatal("hard cancel must wait for grace")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cancelled && task.UserCancelHard && !q.HasInProgress("c1") {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !cancelled {
		t.Fatal("expected lease cancel after grace")
	}
	if !task.UserCancelHard {
		t.Fatal("expected UserCancelHard after grace")
	}
	if q.HasInProgress("c1") {
		t.Fatal("expected control-plane force-fail to clear in-progress after grace")
	}
	if q.RequestUserCancel("missing") {
		t.Fatal("missing lease should return false")
	}
}

func TestForceAbortInFlightLeaseClearsBeforeWorkerReplace(t *testing.T) {
	q := NewQueue("src", 3, 1, nil, nil)
	q.SetMode(QueueModeTraversal)
	task := &TaskBase{
		ID:          "a1",
		Type:        TaskTypeSrcTraversal,
		DisplayPath: "/a",
		Round:       1,
		Folder:      types.Folder{ServiceID: "a", DisplayName: "a", Type: types.NodeTypeFolder},
	}
	q.AddInProgress("a1", task)
	q.BindLeaseOwner(task, "missing-worker")
	q.ForceAbortInFlightLease("a1", "traversal stalled after test")
	if q.HasInProgress("a1") {
		t.Fatal("Working-on must clear even when worker replace is a no-op")
	}
	if q.GetPendingCount() != 0 {
		t.Fatal("force-abort must not leave the folder pending for retry")
	}
}
