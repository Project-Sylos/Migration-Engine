// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package gpl

import (
	"testing"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	pathgpl "codeberg.org/Sylos/go-path-linter/pkg/gpl"
)

func TestEvaluateGPLAddPart_DropboxBaselineSkipsTrailingDots(t *testing.T) {
	payload, issue := evaluateGPLAddPart(pathgpl.Dropbox, 0, "folder...", nil, false, false)
	if !payload.Valid {
		t.Fatalf("baseline Dropbox should accept trailing dots: %#v", payload)
	}
	if issue != nil {
		t.Fatalf("baseline Dropbox should not enqueue issue: %#v", issue)
	}
}

func TestEvaluateGPLAddPart_DropboxWindowsCompatFlagsTrailingDots(t *testing.T) {
	payload, issue := evaluateGPLAddPart(pathgpl.Dropbox, 0, "folder...", nil, false, true)
	if payload.Valid {
		t.Fatalf("WindowsCompat Dropbox should reject trailing dots: %#v", payload)
	}
	if issue == nil || issue.Status != db.GPLIssueStatusPending {
		t.Fatalf("expected pending proposal, got %#v", issue)
	}
	if issue.ProposedName != "folder" {
		t.Fatalf("proposed=%q want folder", issue.ProposedName)
	}
}

func TestEvaluateGPLAddPart_DSTSiblingCollisionManual(t *testing.T) {
	// Cleaned name collides with existing DST sibling "folder".
	payload, issue := evaluateGPLAddPart(
		pathgpl.Dropbox,
		0,
		"folder...",
		[]string{"folder"},
		false,
		true,
	)
	if !payload.Part.Collision {
		t.Fatalf("expected sibling collision on cleaned name, payload=%#v issue=%#v", payload, issue)
	}
	if issue == nil || issue.Status != db.GPLIssueStatusManualReview {
		t.Fatalf("collision must require manual review, got %#v", issue)
	}
}
