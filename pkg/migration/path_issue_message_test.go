// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"strings"
	"testing"
)

func TestPathIssueMessagesFromGPLJSON_PreservesInvalidCharDetail(t *testing.T) {
	raw := `[{"category":"InvalidChar","detail":"*<>","userMessage":"This part of the path contains invalid characters: * < >","docsURL":"https://example.com"}]`
	msgs := PathIssueMessagesFromGPLJSON(raw)
	if len(msgs) != 1 {
		t.Fatalf("msgs=%+v", msgs)
	}
	if msgs[0].Detail != "*<>" {
		t.Fatalf("Detail=%q want *<>", msgs[0].Detail)
	}
	if msgs[0].DocsURL == "" {
		t.Fatal("expected DocsURL")
	}

	// Fallback path: Detail only, no UserMessage — FriendlyPathIssueMessage must include chars.
	raw2 := `[{"category":"InvalidChar","detail":"*"}]`
	msgs2 := PathIssueMessagesFromGPLJSON(raw2)
	if len(msgs2) != 1 || msgs2[0].Detail != "*" {
		t.Fatalf("msgs2=%+v", msgs2)
	}
	if !strings.Contains(msgs2[0].Message, "*") {
		t.Fatalf("Message should include *: %q", msgs2[0].Message)
	}
}
