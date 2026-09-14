// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"fmt"
	"path/filepath"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/filter"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func TestTrigramPlanRulesetContains(t *testing.T) {
	dir := t.TempDir()
	database, err := db.Open(db.Options{Path: filepath.Join(dir, "m.db"), OpsDir: filepath.Join(dir, "m.ops")})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ops := database.Ops()

	writes := make([]opsdb.SealNodeWrite, 0, 20)
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("n%d", i)
		path := fmt.Sprintf("/other_%d/x.txt", i)
		writes = append(writes, opsdb.SealNodeWrite{
			Side: opsdb.SideSRC,
			Node: opsdb.NodeRecord{
				ID: id, Path: path, DisplayPath: path, Name: "x.txt", Type: opsdb.NodeTypeFile, Size: 1, Depth: 2,
			},
			Status:     opsdb.StatusRecord{TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending},
			InsertOnly: true,
		})
	}
	hitPath := "/folder_4/file_12.txt"
	writes = append(writes, opsdb.SealNodeWrite{
		Side: opsdb.SideSRC,
		Node: opsdb.NodeRecord{
			ID: "hit", Path: hitPath, DisplayPath: hitPath, Name: "file_12.txt",
			Type: opsdb.NodeTypeFile, Size: 1, Depth: 2,
		},
		Status:     opsdb.StatusRecord{TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusPending},
		InsertOnly: true,
	})
	if _, err := ops.WriteSealBatch(writes, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	rs := filter.Ruleset{
		SchemaVersion: filter.SchemaVersion,
		RootGroup: filter.Group{Op: filter.OpAND, Children: []filter.Child{
			{Condition: &filter.Condition{
				ID: "n", Field: filter.FieldName, Operator: filter.OpContains, Value: []any{"file"},
				AppliesTo: filter.AppliesBoth,
			}},
			{Condition: &filter.Condition{
				ID: "p", Field: filter.FieldPath, Operator: filter.OpContains, Value: []any{"folder_4"},
				AppliesTo: filter.AppliesFolder,
			}},
			{Condition: &filter.Condition{
				ID: "s", Field: filter.FieldReviewStatus, Operator: filter.OpEQ, Value: "pending",
				AppliesTo: filter.AppliesBoth,
			}},
		}},
	}
	compiled, err := filter.Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	f := ReviewFilter{CompiledFilter: compiled, ExcludeRoot: true}
	if reviewSearchShouldStreamPathIndex(f, "path") {
		t.Fatal("ruleset contains should not full-stream path index")
	}
	plan, err := planReviewCandidateIDs(ops, opsdb.SideSRC, f, 50)
	if err != nil {
		t.Fatal(err)
	}
	if plan.kind != "tri" {
		t.Fatalf("want trigram plan, got %+v", plan)
	}
	if len(plan.ids) == 0 || len(plan.ids) > 5 {
		t.Fatalf("trigram candidates should be tiny, got %d (%+v)", len(plan.ids), plan.ids)
	}

	rows, _, err := ListMergedReviewDiffsPage(database, f, "path ASC", 20, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].SrcNodeID != "hit" {
		t.Fatalf("rows=%+v", rows)
	}
}
