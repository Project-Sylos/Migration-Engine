package filter

import (
	"testing"
	"time"
)

func TestCompileReviewFields(t *testing.T) {
	rs := Ruleset{
		SchemaVersion: SchemaVersion,
		RootGroup: Group{Op: OpAND, Children: []Child{
			{Condition: &Condition{ID: "r1", Field: FieldReviewStatus, Operator: OpEQ, Value: "failed", AppliesTo: AppliesBoth}},
			{Condition: &Condition{ID: "r2", Field: FieldPathIssueStatus, Operator: OpEQ, Value: "issues", AppliesTo: AppliesBoth}},
			{Condition: &Condition{ID: "r3", Field: FieldPathIssueCategory, Operator: OpEQ, Value: "illegal_chars", AppliesTo: AppliesBoth}},
		}},
	}
	c, err := Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	miss := c.EvaluateDetermining(NodeInput{
		Name: "a.txt", DisplayPath: "/a.txt", Depth: 1, NodeType: NodeFile, Size: 1,
	}, time.Now())
	if miss.EngineError {
		t.Fatalf("empty review context should fail match, not engine error: %+v", miss)
	}
	if miss.Outcome != OutcomePassed {
		t.Fatalf("want passed (not excluded) without review context, got %+v", miss)
	}
	hit := c.EvaluateDetermining(NodeInput{
		Name: "a.txt", DisplayPath: "/a.txt", Depth: 1, NodeType: NodeFile, Size: 1,
		ReviewStatus: "failed", PathIssueStatus: "issues", PathIssueCategory: "illegal_chars",
	}, time.Now())
	if hit.EngineError || hit.Outcome != OutcomeExcluded {
		t.Fatalf("want excluded with review context, got %+v", hit)
	}
}

func TestPathAppliesToFolderStillMatchesFiles(t *testing.T) {
	rs := Ruleset{
		SchemaVersion: SchemaVersion,
		RootGroup: Group{Op: OpAND, Children: []Child{
			{Condition: &Condition{
				ID: "p", Field: FieldPath, Operator: OpContains, Value: "folder_4",
				AppliesTo: AppliesFolder,
			}},
		}},
	}
	c, err := Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	out := c.EvaluateDetermining(NodeInput{
		Name: "file_12.txt", DisplayPath: "/folder_4/file_12.txt", Depth: 2, NodeType: NodeFile, Size: 1,
	}, time.Now())
	if out.Outcome != OutcomeExcluded {
		t.Fatalf("path contains on file must match despite applies_to=folder, got %+v", out)
	}
}
