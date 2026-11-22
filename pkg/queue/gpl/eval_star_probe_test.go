package gpl

import (
	"encoding/json"
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	pathgpl "codeberg.org/Sylos/go-path-linter/pkg/gpl"
	"codeberg.org/Sylos/go-path-linter/pkg/issue"
)

func TestEvaluateGPLAddPart_StarCleansIntoSibling(t *testing.T) {
	payload, pe := evaluateGPLAddPart(pathgpl.OneDrive, 0, "cascade*", []string{"cascade"}, false, true)
	if !payload.Part.Collision {
		t.Fatalf("expected collision, payload=%#v pe=%#v", payload, pe)
	}
	if pe == nil || pe.Status != db.GPLIssueStatusManualReview {
		t.Fatalf("expected manual_review, got %#v", pe)
	}
	if siblingCollisionDetail(pe.IssuesJSON) != "cascade" {
		t.Fatalf("issues=%s", pe.IssuesJSON)
	}
	var issues []issue.Issue
	_ = json.Unmarshal([]byte(pe.IssuesJSON), &issues)
	hasInvalid := false
	for _, iss := range issues {
		if iss.Category == issue.CategoryInvalidChar {
			hasInvalid = true
		}
	}
	if !hasInvalid {
		t.Fatal("expected InvalidChar finding alongside SiblingCollision")
	}
}

func TestEvaluateGPLAddPart_InvalidStarChars(t *testing.T) {
	payload, pe := evaluateGPLAddPart(pathgpl.OneDrive, 0, "Invalid *Chars", []string{"Invalid Chars"}, false, true)
	if !payload.Part.Collision {
		t.Fatalf("expected collision, payload=%#v pe=%#v", payload, pe)
	}
	if pe == nil || pe.Status != db.GPLIssueStatusManualReview {
		t.Fatalf("expected manual_review, got %#v", pe)
	}
	if siblingCollisionDetail(pe.IssuesJSON) != "Invalid Chars" {
		t.Fatalf("issues=%s", pe.IssuesJSON)
	}
}
