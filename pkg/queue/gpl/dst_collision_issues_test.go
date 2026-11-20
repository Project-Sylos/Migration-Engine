package gpl

import (
	"encoding/json"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
	"codeberg.org/Sylos/go-path-linter/pkg/issue"
)

func TestEnsureSiblingCollisionIssuesJSON_InjectsDetail(t *testing.T) {
	raw := `[{"category":"InvalidChar","detail":"*","message":"Removed invalid characters."}]`
	out := ensureSiblingCollisionIssuesJSON(raw, "cascade")
	var issues []issue.Issue
	if err := json.Unmarshal([]byte(out), &issues); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, iss := range issues {
		if iss.Category == issue.CategorySiblingCollision && iss.Detail == "cascade" {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing SiblingCollision detail, got %s", out)
	}
}

func TestAppendDSTSiblingCollisionIssues_UsesSRCProposedClean(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/gpl-cascade.db"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	srcID := "src-cascade-star"
	payload := db.GPLStatePayload{
		Valid: false,
		Part: db.GPLScopeState{
			Valid:         false,
			ProposedClean: "cascade",
			Categories:    []string{string(issue.CategoryInvalidChar)},
		},
		Path: db.GPLScopeState{Valid: true},
	}
	stateJSON, _ := json.Marshal(payload)

	q := &queue.Queue{}
	q.ConfigureScalingContext("local", "onedrive", "", "", "onedrive", true)

	task := &queue.TaskBase{
		ID: "dst-root",
		ExpectedFolders: []types.Folder{{
			DisplayName: "cascade*", Type: types.NodeTypeFolder, LocationPath: "/cascade*",
		}},
		ExpectedSrcIDMap: map[string]string{
			queue.DstChildMatchKey(types.NodeTypeFolder, "cascade*", "/cascade*"): srcID,
		},
		ExpectedSrcNodeMeta: map[string]queue.SrcNodeMeta{
			srcID: {GPLState: string(stateJSON)},
		},
		DiscoveredChildren: []queue.ChildResult{{
			IsFile: false,
			Status: db.StatusPending,
			Folder: types.Folder{DisplayName: "cascade", Type: types.NodeTypeFolder, LocationPath: "/cascade"},
		}},
	}

	AppendDSTSiblingCollisionIssues(database, q, task)
	database.WaitUntilFlushedThrough(0)

	rec, ok, err := database.Ops().GetGPL(srcID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected GPL issue after DST sibling collision append")
	}
	if rec.Status != db.GPLIssueStatusManualReview {
		t.Fatalf("status=%q want manual_review", rec.Status)
	}
	if siblingCollisionDetail(rec.IssuesJSON) != "cascade" {
		t.Fatalf("issues=%s want SiblingCollision detail=cascade", rec.IssuesJSON)
	}
}
