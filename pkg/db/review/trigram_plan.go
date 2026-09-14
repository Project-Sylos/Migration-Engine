// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/filter"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

// containsNeedlesFromFilter returns name contains needles for trigram planning.
// Path search uses ordered PathSegments (idx:seg), not open path contains.
func containsNeedlesFromFilter(f ReviewFilter) (needles []string) {
	add := func(s string) {
		s = strings.TrimSpace(s)
		if s == "" || strings.ContainsAny(s, "*%") {
			return
		}
		needles = append(needles, s)
	}
	if q := strings.TrimSpace(f.Query); q != "" {
		field := strings.ToLower(strings.TrimSpace(f.QueryField))
		if field == "" || field == "name" || field == "path" {
			add(q)
		}
	}
	if f.CompiledFilter != nil {
		walkRulesetContains(f.CompiledFilter.Ruleset.RootGroup, &needles)
	}
	return needles
}

func walkRulesetContains(g filter.Group, needles *[]string) {
	for _, ch := range g.Children {
		if ch.Group != nil {
			walkRulesetContains(*ch.Group, needles)
			continue
		}
		if ch.Condition == nil {
			continue
		}
		c := ch.Condition
		if c.Field != filter.FieldName {
			continue
		}
		if c.Operator != filter.OpContains && c.Operator != filter.OpEQ {
			continue
		}
		for _, p := range conditionStringPatterns(c.Value) {
			p = strings.TrimSpace(p)
			if p == "" || strings.ContainsAny(p, "*%") {
				continue
			}
			*needles = append(*needles, p)
		}
	}
}

func conditionStringPatterns(v any) []string {
	switch t := v.(type) {
	case string:
		if strings.TrimSpace(t) == "" {
			return nil
		}
		return []string{t}
	case []string:
		return t
	case []any:
		out := make([]string, 0, len(t))
		for _, x := range t {
			if s, ok := x.(string); ok && strings.TrimSpace(s) != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func bestContainsNeedle(needles []string) string {
	best := ""
	for _, n := range needles {
		if len([]rune(n)) > len([]rune(best)) {
			best = n
		}
	}
	return best
}

func planTrigramContains(ops *opsdb.Store, side string, f ReviewFilter, scanLimit int) (plannerPlan, bool, error) {
	needle := bestContainsNeedle(containsNeedlesFromFilter(f))
	if needle == "" {
		return plannerPlan{}, false, nil
	}
	if len(opsdb.ExtractTrigrams(needle)) == 0 {
		return plannerPlan{}, false, nil
	}
	ids, err := ops.ScanTrigramIntersect(side, needle, scanLimit)
	if err != nil {
		return plannerPlan{}, false, err
	}
	if len(ids) == 0 {
		return plannerPlan{kind: "tri", ids: nil, est: 0, reason: "trigram_empty"}, true, nil
	}
	return plannerPlan{kind: "tri", ids: ids, est: int64(len(ids)), reason: "trigram_contains"}, true, nil
}

// nodePassesTrigramVerify rejects false positives from the planning needle's intersect.
func nodePassesTrigramVerify(n opsdb.NodeRecord, f ReviewFilter) bool {
	needle := bestContainsNeedle(containsNeedlesFromFilter(f))
	if needle == "" {
		return true
	}
	needle = strings.ToLower(strings.TrimSpace(needle))
	name := strings.ToLower(n.Name)
	path := strings.ToLower(n.DisplayPath)
	if path == "" {
		path = strings.ToLower(n.Path)
	}
	return strings.Contains(name, needle) || strings.Contains(path, needle)
}
