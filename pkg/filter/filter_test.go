// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package filter

import (
	"testing"
	"time"
)

func TestDisplayPathForChild(t *testing.T) {
	if got := RootDisplayPath(); got != "/" {
		t.Fatalf("root=%q", got)
	}
	if got := DisplayPathForChild("/", 0, "Reports"); got != "/Reports" {
		t.Fatalf("depth1=%q", got)
	}
	if got := DisplayPathForChild("/Reports", 1, "Q1"); got != "/Reports/Q1" {
		t.Fatalf("depth2=%q", got)
	}
	if got := DisplayPathForChild("/Reports/Q1", 2, "a.pdf"); got != "/Reports/Q1/a.pdf" {
		t.Fatalf("depth3=%q", got)
	}
}

func TestCompileSchemaMismatch(t *testing.T) {
	_, err := Compile(Ruleset{SchemaVersion: 99, RootGroup: Group{Op: OpAND, Children: []Child{
		{Condition: &Condition{ID: "a", Field: FieldDepth, Operator: OpEQ, Value: 1, AppliesTo: AppliesBoth}},
	}}})
	if err == nil {
		t.Fatal("expected schema error")
	}
}

func TestStaleLargeORSpreadsheet(t *testing.T) {
	rs := Ruleset{
		SchemaVersion: 1,
		RootGroup: Group{
			Op: OpOR,
			Children: []Child{
				{Group: &Group{Op: OpAND, Children: []Child{
					{Condition: &Condition{ID: "size", Field: FieldSize, Operator: OpGT, Value: 5368709120, AppliesTo: AppliesFile}},
					{Condition: &Condition{ID: "old5", Field: FieldMTime, Operator: OpOlderThan, Value: "5y", AppliesTo: AppliesFile}},
				}}},
				{Group: &Group{Op: OpAND, Children: []Child{
					{Condition: &Condition{ID: "sheet", Field: FieldMimetypeCategory, Operator: OpEQ, Value: "spreadsheet", AppliesTo: AppliesFile}},
					{Condition: &Condition{ID: "old3", Field: FieldMTime, Operator: OpOlderThan, Value: "3y", AppliesTo: AppliesFile}},
				}}},
			},
		},
	}
	c, err := Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC)
	old := now.Add(-4 * 365 * 24 * time.Hour).Format(time.RFC3339)

	out := c.EvaluateDetermining(NodeInput{
		Name: "budget.xlsx", DisplayPath: "/Finance/budget.xlsx",
		Size: 100, MTime: old, Depth: 2, NodeType: NodeFile,
	}, now)
	if out.Outcome != OutcomeExcluded {
		t.Fatalf("spreadsheet old3y: want excluded, got %+v", out)
	}

	out = c.EvaluateDetermining(NodeInput{
		Name: "huge.bin", DisplayPath: "/huge.bin",
		Size: 6_000_000_000, MTime: now.Add(-6 * 365 * 24 * time.Hour).Format(time.RFC3339),
		Depth: 1, NodeType: NodeFile,
	}, now)
	if out.Outcome != OutcomeExcluded {
		t.Fatalf("large old: want excluded, got %+v", out)
	}

	out = c.EvaluateDetermining(NodeInput{
		Name: "fresh.xlsx", DisplayPath: "/fresh.xlsx",
		Size: 100, MTime: now.Add(-24 * time.Hour).Format(time.RFC3339),
		Depth: 1, NodeType: NodeFile,
	}, now)
	if out.Outcome != OutcomePassed {
		t.Fatalf("fresh sheet: want passed, got %+v", out)
	}
}

func TestSkipPropagationInOR(t *testing.T) {
	// Folder-only child in OR with a file-matching sibling: folder child SKIPs,
	// must not force false.
	rs := Ruleset{
		SchemaVersion: 1,
		RootGroup: Group{
			Op: OpOR,
			Children: []Child{
				{Condition: &Condition{ID: "empty", Field: FieldIsEmpty, Operator: OpEQ, Value: true, AppliesTo: AppliesFolder}},
				{Condition: &Condition{ID: "pdf", Field: FieldExtension, Operator: OpEQ, Value: "pdf", AppliesTo: AppliesFile}},
			},
		},
	}
	c, err := Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	out := c.EvaluateDetermining(NodeInput{
		Name: "a.pdf", DisplayPath: "/a.pdf", Size: 1, Depth: 1, NodeType: NodeFile,
	}, time.Now())
	if out.Outcome != OutcomeExcluded {
		t.Fatalf("pdf file: want excluded, got %+v", out)
	}
}

func TestPathRegexAndNameGlob(t *testing.T) {
	rs := Ruleset{
		SchemaVersion: 1,
		RootGroup: Group{
			Op: OpAND,
			Children: []Child{
				{Condition: &Condition{ID: "path", Field: FieldPath, Operator: OpRegex, Value: `(?i)(^|/)reports(/|$)`, AppliesTo: AppliesFile}},
				{Condition: &Condition{ID: "ext", Field: FieldExtension, Operator: OpEQ, Value: "pdf", AppliesTo: AppliesFile}},
			},
			Negate: true, // include-only: exclude when NOT (path reports AND pdf)
		},
	}
	c, err := Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	keep := c.EvaluateDetermining(NodeInput{
		Name: "a.pdf", DisplayPath: "/Client/Reports/a.pdf", Depth: 3, NodeType: NodeFile, Size: 1,
	}, time.Now())
	if keep.Outcome != OutcomePassed {
		t.Fatalf("reports pdf should keep, got %+v", keep)
	}
	drop := c.EvaluateDetermining(NodeInput{
		Name: "a.pdf", DisplayPath: "/Other/a.pdf", Depth: 2, NodeType: NodeFile, Size: 1,
	}, time.Now())
	if drop.Outcome != OutcomeExcluded {
		t.Fatalf("other pdf should exclude, got %+v", drop)
	}
}

func TestNameListMatchesAnyPattern(t *testing.T) {
	rs := Ruleset{
		SchemaVersion: 1,
		RootGroup: Group{Op: OpAND, Children: []Child{
			{Condition: &Condition{ID: "junk", Field: FieldName, Operator: OpGlob,
				Value: []any{"Thumbs.db", ".DS_Store", "*.tmp", "~$*"}, AppliesTo: AppliesBoth}},
		}},
	}
	c, err := Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"Thumbs.db", ".DS_Store", "build.tmp", "~$report.docx"} {
		out := c.EvaluateDetermining(NodeInput{
			Name: name, DisplayPath: "/" + name, Depth: 1, NodeType: NodeFile, Size: 1,
		}, time.Now())
		if out.Outcome != OutcomeExcluded {
			t.Fatalf("%s: want excluded, got %+v", name, out)
		}
	}
	keep := c.EvaluateDetermining(NodeInput{
		Name: "report.docx", DisplayPath: "/report.docx", Depth: 1, NodeType: NodeFile, Size: 1,
	}, time.Now())
	if keep.Outcome != OutcomePassed {
		t.Fatalf("report.docx: want passed, got %+v", keep)
	}
}

func TestNameListNotEqualMeansNoneMatch(t *testing.T) {
	rs := Ruleset{
		SchemaVersion: 1,
		RootGroup: Group{Op: OpAND, Children: []Child{
			{Condition: &Condition{ID: "keep", Field: FieldName, Operator: OpNEQ,
				Value: []any{"keep.txt", "also-keep.txt"}, AppliesTo: AppliesFile}},
		}},
	}
	c, err := Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"keep.txt", "also-keep.txt"} {
		out := c.EvaluateDetermining(NodeInput{
			Name: name, DisplayPath: "/" + name, Depth: 1, NodeType: NodeFile, Size: 1,
		}, time.Now())
		if out.Outcome != OutcomePassed {
			t.Fatalf("%s: listed name must not match, got %+v", name, out)
		}
	}
	out := c.EvaluateDetermining(NodeInput{
		Name: "other.txt", DisplayPath: "/other.txt", Depth: 1, NodeType: NodeFile, Size: 1,
	}, time.Now())
	if out.Outcome != OutcomeExcluded {
		t.Fatalf("unlisted name: want excluded, got %+v", out)
	}
}

func TestGlobIgnoresCase(t *testing.T) {
	rs := Ruleset{
		SchemaVersion: 1,
		RootGroup: Group{Op: OpAND, Children: []Child{
			{Condition: &Condition{ID: "junk", Field: FieldName, Operator: OpGlob,
				Value: []any{"Thumbs.db", "*.tmp"}, AppliesTo: AppliesFile}},
		}},
	}
	c, err := Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"thumbs.db", "THUMBS.DB", "Build.TMP"} {
		out := c.EvaluateDetermining(NodeInput{
			Name: name, DisplayPath: "/" + name, Depth: 1, NodeType: NodeFile, Size: 1,
		}, time.Now())
		if out.Outcome != OutcomeExcluded {
			t.Fatalf("%s: want excluded, got %+v", name, out)
		}
	}
}

func TestEmptyNamePatternListIsSkipped(t *testing.T) {
	rs := Ruleset{
		SchemaVersion: 1,
		RootGroup: Group{Op: OpAND, Children: []Child{
			{Condition: &Condition{ID: "unfinished", Field: FieldName, Operator: OpGlob,
				Value: []any{}, Negate: true, AppliesTo: AppliesFile}},
		}},
	}
	c, err := Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	out := c.EvaluateDetermining(NodeInput{
		Name: "anything.txt", DisplayPath: "/anything.txt", Depth: 1, NodeType: NodeFile, Size: 1,
	}, time.Now())
	if out.Outcome != OutcomePassed {
		t.Fatalf("a rule with no patterns must not exclude anything, got %+v", out)
	}
}

func TestNameWithCommaStaysOnePattern(t *testing.T) {
	rs := Ruleset{
		SchemaVersion: 1,
		RootGroup: Group{Op: OpAND, Children: []Child{
			{Condition: &Condition{ID: "comma", Field: FieldName, Operator: OpEQ,
				Value: "Smith, John.pdf", AppliesTo: AppliesFile}},
		}},
	}
	c, err := Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	out := c.EvaluateDetermining(NodeInput{
		Name: "Smith, John.pdf", DisplayPath: "/Smith, John.pdf", Depth: 1, NodeType: NodeFile, Size: 1,
	}, time.Now())
	if out.Outcome != OutcomeExcluded {
		t.Fatalf("comma in a single pattern must not split: %+v", out)
	}
	keep := c.EvaluateDetermining(NodeInput{
		Name: "Smith", DisplayPath: "/Smith", Depth: 1, NodeType: NodeFile, Size: 1,
	}, time.Now())
	if keep.Outcome != OutcomePassed {
		t.Fatalf("half of the pattern must not match: %+v", keep)
	}
}

func TestNameListRegexCompilesEveryPattern(t *testing.T) {
	rs := Ruleset{
		SchemaVersion: 1,
		RootGroup: Group{Op: OpAND, Children: []Child{
			{Condition: &Condition{ID: "res", Field: FieldName, Operator: OpRegex,
				Value: []any{"^draft-", "^wip-"}, AppliesTo: AppliesFile}},
		}},
	}
	c, err := Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	out := c.EvaluateDetermining(NodeInput{
		Name: "wip-notes.txt", DisplayPath: "/wip-notes.txt", Depth: 1, NodeType: NodeFile, Size: 1,
	}, time.Now())
	if out.Outcome != OutcomeExcluded {
		t.Fatalf("second regex should match: %+v", out)
	}

	_, err = Compile(Ruleset{
		SchemaVersion: 1,
		RootGroup: Group{Op: OpAND, Children: []Child{
			{Condition: &Condition{ID: "bad", Field: FieldName, Operator: OpRegex,
				Value: []any{"^ok-", "("}, AppliesTo: AppliesFile}},
		}},
	})
	if err == nil {
		t.Fatal("expected compile error for the invalid pattern in the list")
	}
}

func TestInvalidRegexCompile(t *testing.T) {
	_, err := Compile(Ruleset{
		SchemaVersion: 1,
		RootGroup: Group{Op: OpAND, Children: []Child{
			{Condition: &Condition{ID: "bad", Field: FieldName, Operator: OpRegex, Value: "(", AppliesTo: AppliesBoth}},
		}},
	})
	if err == nil {
		t.Fatal("expected compile error")
	}
}

func TestPostNodeIsEmpty(t *testing.T) {
	rs := Ruleset{
		SchemaVersion: 1,
		RootGroup: Group{Op: OpAND, Children: []Child{
			{Condition: &Condition{ID: "e", Field: FieldIsEmpty, Operator: OpEQ, Value: true, AppliesTo: AppliesFolder}},
		}},
	}
	c, err := Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	// Pre-phase without child data → skip → passed (no applicable).
	pre := c.EvaluateDetermining(NodeInput{Name: "f", DisplayPath: "/f", Depth: 1, NodeType: NodeFolder}, time.Now())
	if pre.Outcome != OutcomePassed {
		t.Fatalf("pre: %+v", pre)
	}
	zero := 0
	empty := true
	post := c.EvaluateDetermining(NodeInput{
		Name: "f", DisplayPath: "/f", Depth: 1, NodeType: NodeFolder, ChildCount: &zero, IsEmpty: &empty,
	}, time.Now())
	if post.Outcome != OutcomeExcluded {
		t.Fatalf("empty folder: %+v", post)
	}
}

func TestEvaluateAllApplicable(t *testing.T) {
	rs := Ruleset{
		SchemaVersion: 1,
		RootGroup: Group{Op: OpOR, Children: []Child{
			{Condition: &Condition{ID: "a", Field: FieldExtension, Operator: OpEQ, Value: "pdf", AppliesTo: AppliesFile}},
			{Condition: &Condition{ID: "b", Field: FieldExtension, Operator: OpEQ, Value: "doc", AppliesTo: AppliesFile}},
		}},
	}
	c, err := Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	out := c.EvaluateAllApplicable(NodeInput{
		Name: "x.pdf", DisplayPath: "/x.pdf", Depth: 1, NodeType: NodeFile, Size: 1,
	}, time.Now())
	if len(out.AllMatches) != 2 {
		t.Fatalf("want 2 matches, got %d %+v", len(out.AllMatches), out.AllMatches)
	}
}

func TestMTimeBeforeAfterCalendarDate(t *testing.T) {
	rs := Ruleset{
		SchemaVersion: 1,
		RootGroup: Group{Op: OpAND, Children: []Child{
			{Condition: &Condition{ID: "cutoff", Field: FieldMTime, Operator: OpBefore, Value: "2020-01-01", AppliesTo: AppliesFile}},
		}},
	}
	c, err := Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC)

	out := c.EvaluateDetermining(NodeInput{
		Name: "old.txt", DisplayPath: "/old.txt", Size: 1, Depth: 1, NodeType: NodeFile,
		MTime: "2019-06-30T12:00:00Z",
	}, now)
	if out.Outcome != OutcomeExcluded {
		t.Fatalf("pre-cutoff file: want excluded, got %+v", out)
	}

	out = c.EvaluateDetermining(NodeInput{
		Name: "new.txt", DisplayPath: "/new.txt", Size: 1, Depth: 1, NodeType: NodeFile,
		MTime: "2021-03-01T00:00:00Z",
	}, now)
	if out.Outcome != OutcomePassed {
		t.Fatalf("post-cutoff file: want passed, got %+v", out)
	}

	after := Ruleset{
		SchemaVersion: 1,
		RootGroup: Group{Op: OpAND, Children: []Child{
			{Condition: &Condition{ID: "cutoff", Field: FieldMTime, Operator: OpAfter, Value: "2020-01-01T00:00:00Z", AppliesTo: AppliesFile}},
		}},
	}
	ca, err := Compile(after)
	if err != nil {
		t.Fatal(err)
	}
	out = ca.EvaluateDetermining(NodeInput{
		Name: "new.txt", DisplayPath: "/new.txt", Size: 1, Depth: 1, NodeType: NodeFile,
		MTime: "2021-03-01T00:00:00Z",
	}, now)
	if out.Outcome != OutcomeExcluded {
		t.Fatalf("after cutoff: want excluded, got %+v", out)
	}
}

// Keep-only polarity lives on the root group; conditions stay positive. "All of
// these" then means every criterion must hold, so only /mnt/media survives.
func TestKeepOnlyAndOfTwoPathFragments(t *testing.T) {
	c, err := Compile(Ruleset{
		SchemaVersion: 1,
		RootGroup: Group{
			Op:     OpAND,
			Negate: true,
			Children: []Child{
				{Condition: &Condition{ID: "mnt", Field: FieldPath, Operator: OpContains, Value: []any{"mnt"}, AppliesTo: AppliesBoth}},
				{Condition: &Condition{ID: "media", Field: FieldPath, Operator: OpContains, Value: []any{"media"}, AppliesTo: AppliesBoth}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]string{
		"/mnt/media":       OutcomePassed,
		"/mnt/media/shows": OutcomePassed,
		"/mnt":             OutcomeExcluded,
		"/mnt/other":       OutcomeExcluded,
		"/media":           OutcomeExcluded,
		"/home":            OutcomeExcluded,
	} {
		out := c.EvaluateDetermining(NodeInput{
			Name: path, DisplayPath: path, Depth: 1, NodeType: NodeFolder,
		}, time.Now())
		if out.Outcome != want {
			t.Fatalf("%s: want %s, got %s (%+v)", path, want, out.Outcome, out)
		}
	}
}

// Condition Negate inverts one predicate, it does not mark the rule as an include.
// Exclude-polarity AND of two negated checks is De Morgan "keep when either matches";
// the editor must not present this shape as "require both fragments".
func TestNegatedConditionsUnderAndKeepWhenEitherMatches(t *testing.T) {
	c, err := Compile(Ruleset{
		SchemaVersion: 1,
		RootGroup: Group{
			Op: OpAND,
			Children: []Child{
				{Condition: &Condition{ID: "mnt", Field: FieldPath, Operator: OpContains, Value: []any{"mnt"}, Negate: true, AppliesTo: AppliesBoth}},
				{Condition: &Condition{ID: "media", Field: FieldPath, Operator: OpContains, Value: []any{"media"}, Negate: true, AppliesTo: AppliesBoth}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]string{
		"/mnt":       OutcomePassed,
		"/media":     OutcomePassed,
		"/mnt/media": OutcomePassed,
		"/home":      OutcomeExcluded,
	} {
		out := c.EvaluateDetermining(NodeInput{
			Name: path, DisplayPath: path, Depth: 1, NodeType: NodeFolder,
		}, time.Now())
		if out.Outcome != want {
			t.Fatalf("%s: want %s, got %s (%+v)", path, want, out.Outcome, out)
		}
	}
}

func TestNameContainsCaseSensitivity(t *testing.T) {
	fold := Ruleset{
		SchemaVersion: 1,
		RootGroup: Group{Op: OpAND, Children: []Child{
			{Condition: &Condition{ID: "n", Field: FieldName, Operator: OpContains, Value: []any{"Report"}, AppliesTo: AppliesFile}},
		}},
	}
	cFold, err := Compile(fold)
	if err != nil {
		t.Fatal(err)
	}
	out := cFold.EvaluateDetermining(NodeInput{
		Name: "report.pdf", DisplayPath: "/report.pdf", Depth: 1, NodeType: NodeFile, Size: 1,
	}, time.Now())
	if out.Outcome != OutcomeExcluded {
		t.Fatalf("default fold: want excluded, got %+v", out)
	}

	sensitive := Ruleset{
		SchemaVersion: 1,
		RootGroup: Group{Op: OpAND, Children: []Child{
			{Condition: &Condition{
				ID: "n", Field: FieldName, Operator: OpContains, Value: []any{"Report"},
				AppliesTo: AppliesFile, CaseSensitive: true,
			}},
		}},
	}
	cSens, err := Compile(sensitive)
	if err != nil {
		t.Fatal(err)
	}
	miss := cSens.EvaluateDetermining(NodeInput{
		Name: "report.pdf", DisplayPath: "/report.pdf", Depth: 1, NodeType: NodeFile, Size: 1,
	}, time.Now())
	if miss.Outcome != OutcomePassed {
		t.Fatalf("case-sensitive miss: want passed, got %+v", miss)
	}
	hit := cSens.EvaluateDetermining(NodeInput{
		Name: "Report.pdf", DisplayPath: "/Report.pdf", Depth: 1, NodeType: NodeFile, Size: 1,
	}, time.Now())
	if hit.Outcome != OutcomeExcluded {
		t.Fatalf("case-sensitive hit: want excluded, got %+v", hit)
	}
}

func TestMTimeInvalidCalendarDateCompile(t *testing.T) {
	_, err := Compile(Ruleset{
		SchemaVersion: 1,
		RootGroup: Group{Op: OpAND, Children: []Child{
			{Condition: &Condition{ID: "bad", Field: FieldMTime, Operator: OpBefore, Value: "last tuesday", AppliesTo: AppliesFile}},
		}},
	})
	if err == nil {
		t.Fatal("expected compile error")
	}
}
