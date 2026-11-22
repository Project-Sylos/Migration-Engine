// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/filter"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

func TestNormalizeDeadAbortedSetsUncleanSuspended(t *testing.T) {
	mgr := NewMigrationManager()
	defer mgr.Close()
	mig, err := mgr.CreateMigration(CreateMigrationConfig{
		MigrationID:  "heal-aborted",
		Name:         "heal-aborted",
		MigrationDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := mig.transitionTo(PhaseFiltersSet); err != nil {
		t.Fatal(err)
	}
	if err := mig.transitionTo(PhaseTraversing); err != nil {
		t.Fatal(err)
	}
	if err := mig.transitionTo(PhaseAborted); err != nil {
		t.Fatal(err)
	}
	_ = mig.SetUncleanInterrupt("copy")
	changed, err := mig.NormalizeDeadInProgressToSuspended()
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("expected phase change")
	}
	if mig.Phase() != PhaseCopySuspended {
		t.Fatalf("phase=%s want copy-suspended", mig.Phase())
	}
	if !mig.HasUncleanInterrupt() || mig.UncleanInterruptKind() != "copy" {
		t.Fatalf("unclean=%v kind=%q", mig.HasUncleanInterrupt(), mig.UncleanInterruptKind())
	}
}

func TestNormalizeDeadInProgressSetsUnclean(t *testing.T) {
	mgr := NewMigrationManager()
	defer mgr.Close()
	mig, err := mgr.CreateMigration(CreateMigrationConfig{
		MigrationID:  "heal-copying",
		Name:         "heal-copying",
		MigrationDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := mig.transitionTo(PhaseFiltersSet); err != nil {
		t.Fatal(err)
	}
	if err := mig.transitionTo(PhaseTraversing); err != nil {
		t.Fatal(err)
	}
	if err := mig.transitionTo(PhaseTraversalFinalizing); err != nil {
		t.Fatal(err)
	}
	if err := mig.transitionTo(PhaseTraversalReview); err != nil {
		t.Fatal(err)
	}
	if err := mig.transitionTo(PhaseCopying); err != nil {
		t.Fatal(err)
	}
	changed, err := mig.NormalizeDeadInProgressToSuspended()
	if err != nil {
		t.Fatal(err)
	}
	if !changed || mig.Phase() != PhaseCopySuspended {
		t.Fatalf("changed=%v phase=%s", changed, mig.Phase())
	}
	if !mig.HasUncleanInterrupt() {
		t.Fatal("expected unclean_interrupt")
	}
}

func TestCopyResumeRecoveryHashVerifyClearsWithWindow(t *testing.T) {
	q := queue.NewQueue("copy", 3, 1, nil, nil)
	q.SetMode(queue.QueueModeCopy)
	q.SetCopyResumeRecoveryHashVerify(true)
	q.SetCopyResumeDstExistenceWindow(2, 3)
	q.SetCopyPass(2)
	q.SetRound(3)
	if !q.ShouldCopyResumeRecoveryHashVerify() {
		t.Fatal("expected recovery hash verify in window")
	}
	cleared := false
	q.SetOnLeaveCopyResumeWindow(func() { cleared = true })
	q.NoteCopyResumeDstExistenceLeavingAnchorRound()
	if q.ShouldCopyResumeRecoveryHashVerify() {
		t.Fatal("hash verify should clear when window leaves")
	}
	if !cleared {
		t.Fatal("expected OnLeaveCopyResumeWindow callback")
	}
}

func TestApplyTraversalResumeUncleanIgnoresSavedCursor(t *testing.T) {
	src := queue.NewQueue("src", 3, 1, nil, nil)
	dst := queue.NewQueue("dst", 3, 1, nil, nil)
	applyTraversalResume(nil, &RuntimeSuspendV1{
		Version:         1,
		Kind:            "traversal",
		LastRoundSrc:    5,
		LastRoundDst:    5,
		SrcKeysetCursor: "should-not-use",
		DstKeysetCursor: "should-not-use",
	}, src, dst, nil, true)
	if src.GetKeysetCursor() != "" || dst.GetKeysetCursor() != "" {
		t.Fatalf("unclean with nil db should not keep soft cursors; src=%q dst=%q", src.GetKeysetCursor(), dst.GetKeysetCursor())
	}
	if src.GetRound() != 5 {
		t.Fatalf("round=%d want saved 5 when no pending scan", src.GetRound())
	}
}

func TestApplyCopyResumeUncleanEnablesWindowWhenSuccessfulZero(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/unclean-copy-window.db"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	srcRoot := types.Folder{ServiceID: "src-root", DisplayName: "Src", LocationPath: "/", Type: types.NodeTypeFolder}
	dstRoot := types.Folder{ServiceID: "dst-root", DisplayName: "Dst", LocationPath: "/", Type: types.NodeTypeFolder}
	if _, err := SeedRootTasksWithPreparation(srcRoot, dstRoot, database, RootPreparation{}, "local", "local", "windows", false, nil); err != nil {
		t.Fatal(err)
	}
	srcRootID, _, ok := pull.GetRootNode(database, "SRC")
	if !ok {
		t.Fatal("missing SRC root")
	}
	childID := db.MintNodeID("SRC", srcRootID, types.NodeTypeFile, "a.txt")
	childPath := db.JoinIDPath("/", childID)
	if err := database.SeedDiscoveredNodes([]db.InsertOperation{{
		QueueType: "SRC",
		Level:     1,
		Status:    db.StatusSuccessful,
		State: &db.NodeState{
			ID:              childID,
			ServiceID:       "src-a",
			ParentID:        srcRootID,
			ParentPath:      "/",
			Path:            childPath,
			Name:            "a.txt",
			DisplayPath:     filter.DisplayPathForChild("/", 0, "a.txt"),
			Type:            types.NodeTypeFile,
			Depth:           1,
			TraversalStatus: db.StatusSuccessful,
			CopyStatus:      db.CopyStatusPending,
		},
	}}); err != nil {
		t.Fatal(err)
	}

	q := queue.NewQueue("copy", 3, 1, nil, nil)
	q.SetMode(queue.QueueModeCopy)
	q.SetCopyPass(2)
	q.SetRound(1)

	applyCopyResumeDstExistenceWindow(q, database, 1, -1, 1, false, nil)
	if q.ShouldCopyResumeRecoveryHashVerify() || q.ShouldApplyCopyDstResumeExistenceCheck() {
		t.Fatal("soft-stop path must not enable window when Successful==0")
	}

	applyCopyResumeDstExistenceWindow(q, database, 1, -1, 1, true, nil)
	if !q.ShouldApplyCopyDstResumeExistenceCheck() {
		t.Fatal("unclean resume must enable existence window when Pending>0 and Successful==0")
	}
	if !q.ShouldCopyResumeRecoveryHashVerify() {
		t.Fatal("unclean resume must enable recovery HashVerify")
	}
}

func TestSoftStopClearsUncleanInterrupt(t *testing.T) {
	mgr := NewMigrationManager()
	defer mgr.Close()
	mig, err := mgr.CreateMigration(CreateMigrationConfig{
		MigrationID:  "soft-clear-unclean",
		Name:         "soft-clear-unclean",
		MigrationDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := mig.SetUncleanInterrupt("copy"); err != nil {
		t.Fatal(err)
	}
	if !mig.HasUncleanInterrupt() {
		t.Fatal("expected unclean before clear")
	}
	if err := mig.ClearUncleanInterrupt(); err != nil {
		t.Fatal(err)
	}
	if mig.HasUncleanInterrupt() {
		t.Fatal("soft-stop / clear must drop unclean_interrupt")
	}
}
