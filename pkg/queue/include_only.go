// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"encoding/json"
	"strings"
)

// ParseIncludeOnlyJSON decodes a JSON array of child service IDs.
// Empty / invalid input means unrestricted (nil set).
func ParseIncludeOnlyJSON(raw string) map[string]struct{} {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "null" {
		return nil
	}
	var ids []string
	if err := json.Unmarshal([]byte(raw), &ids); err != nil || len(ids) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		out[id] = struct{}{}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// EncodeIncludeOnlyJSON marshals child service IDs for src_nodes.include_only.
func EncodeIncludeOnlyJSON(ids []string) string {
	cleaned := make([]string, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		cleaned = append(cleaned, id)
	}
	if len(cleaned) == 0 {
		return ""
	}
	b, err := json.Marshal(cleaned)
	if err != nil {
		return ""
	}
	return string(b)
}

// ChildAllowedByIncludeOnly reports whether serviceID may be kept after ListChildren.
// A nil/empty allowlist allows every child.
func ChildAllowedByIncludeOnly(allow map[string]struct{}, serviceID string) bool {
	if allow == nil {
		return true
	}
	_, ok := allow[strings.TrimSpace(serviceID)]
	return ok
}
