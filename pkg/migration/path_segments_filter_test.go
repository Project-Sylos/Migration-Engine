// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db/review"
	"codeberg.org/Sylos/Migration-Engine/pkg/filter"
)

func TestSearchRequestToReviewFilterPathSegmentsOrder(t *testing.T) {
	f, err := searchRequestToReviewFilter(SearchRequest{
		Conditions: []PathReviewSearchCondition{
			{Field: "path", Value: "Reports"},
			{Field: "path", Value: "Q1"},
			{Field: "path", Value: "summ"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(f.PathSegments) != 3 {
		t.Fatalf("want 3 path segments, got %#v", f.PathSegments)
	}
	if f.PathSegments[0] != "Reports" || f.PathSegments[1] != "Q1" || f.PathSegments[2] != "summ" {
		t.Fatalf("order wrong: %#v", f.PathSegments)
	}
	if f.Query != "" || f.QueryField != "" {
		t.Fatalf("path-only request should not set Query: query=%q field=%q", f.Query, f.QueryField)
	}
}

func TestSearchRequestToReviewFilterRulesetPathSegments(t *testing.T) {
	rs := filter.Ruleset{
		SchemaVersion: filter.SchemaVersion,
		RootGroup: filter.Group{Op: filter.OpAND, Children: []filter.Child{
			{Condition: &filter.Condition{
				ID: "p", Field: filter.FieldPath, Operator: "segments",
				Value: []any{"Reports", "Q1"}, AppliesTo: filter.AppliesBoth,
			}},
			{Condition: &filter.Condition{
				ID: "n", Field: filter.FieldName, Operator: filter.OpContains,
				Value: []any{"file"}, AppliesTo: filter.AppliesBoth,
			}},
		}},
	}
	f, err := searchRequestToReviewFilter(SearchRequest{Ruleset: &rs, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(f.PathSegments) != 2 || f.PathSegments[0] != "Reports" || f.PathSegments[1] != "Q1" {
		t.Fatalf("segments %#v", f.PathSegments)
	}
	if f.CompiledFilter == nil {
		t.Fatal("name rule should remain compiled")
	}
	if walkHasPath(f.CompiledFilter.Ruleset.RootGroup) {
		t.Fatal("path segments must not stay on the compiled ruleset")
	}
}

func walkHasPath(g filter.Group) bool {
	for _, ch := range g.Children {
		if ch.Condition != nil && ch.Condition.Field == filter.FieldPath {
			return true
		}
		if ch.Group != nil && walkHasPath(*ch.Group) {
			return true
		}
	}
	return false
}

func TestSearchRequestToReviewFilterRulesetAloneIsPredicate(t *testing.T) {
	rs := filter.Ruleset{
		SchemaVersion: filter.SchemaVersion,
		RootGroup: filter.Group{
			Op: filter.OpAND,
			Children: []filter.Child{{
				Condition: &filter.Condition{
					ID:        "r1",
					Field:     "name",
					Operator:  "contains",
					Value:     "tmp",
					AppliesTo: "both",
				},
			}},
		},
	}
	f, err := searchRequestToReviewFilter(SearchRequest{Ruleset: &rs, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if f.CompiledFilter == nil {
		t.Fatal("expected CompiledFilter from ruleset")
	}
	if !review.ReviewFilterHasSearchPredicate(f) {
		t.Fatal("ruleset-only search must count as a predicate")
	}
}
