// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package review

import (
	"strings"
	"unicode/utf8"

	"codeberg.org/Sylos/Migration-Engine/pkg/filter"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

// containsNeedlesFromFilter returns text needles for name/path Query and ruleset
// name contains/eq leaves. Path-segment bar parts are not included: those are
// ordered ancestry predicates (pathSegmentsMatchAncestry), not leaf substring
// needles. planTrigramContains adds segments only when falling back to idx:tri.
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
	needles := containsNeedlesFromFilter(f)
	for _, seg := range pathSegmentsForFilter(f) {
		s := strings.TrimSpace(seg)
		if s == "" || strings.ContainsAny(s, "*%") {
			continue
		}
		needles = append(needles, s)
	}
	if len(needles) == 0 {
		return plannerPlan{}, false, nil
	}
	// Union ranked posting lists across every needle. Score is gram-overlap count
	// (partial match); callers soft-verify then apply compiled filters.
	seen := map[string]struct{}{}
	var ids []string
	anyNeedle := false
	for _, needle := range needles {
		if len(opsdb.ExtractTrigrams(opsdb.TrigramSourceText(needle))) == 0 {
			continue
		}
		anyNeedle = true
		hit, err := ops.ScanTrigramRanked(side, needle, scanLimit)
		if err != nil {
			return plannerPlan{}, false, err
		}
		for _, id := range hit {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			ids = append(ids, id)
			if len(ids) >= scanLimit {
				return plannerPlan{kind: "tri", ids: ids, est: int64(len(ids)), reason: "trigram_ranked"}, true, nil
			}
		}
	}
	if !anyNeedle {
		return plannerPlan{}, false, nil
	}
	if len(ids) == 0 {
		return plannerPlan{kind: "tri", ids: nil, est: 0, reason: "trigram_empty"}, true, nil
	}
	return plannerPlan{kind: "tri", ids: ids, est: int64(len(ids)), reason: "trigram_ranked"}, true, nil
}

// nodePassesTrigramVerify soft-checks name/path Query and ruleset name needles
// (substring, small edit distance, or shared trigrams). Path segments are not
// checked here; pathSegmentsMatchAncestry already required them on the chain.
// Compiled ruleset contains still enforces exact contains when present.
func nodePassesTrigramVerify(n opsdb.NodeRecord, f ReviewFilter) bool {
	needles := containsNeedlesFromFilter(f)
	if len(needles) == 0 {
		return true
	}
	name := n.Name
	path := n.DisplayPath
	if path == "" {
		path = n.Path
	}
	for _, needle := range needles {
		if fuzzyNeedleMatch(name, needle) || fuzzyNeedleMatch(path, needle) {
			return true
		}
	}
	return false
}

// fuzzyNeedleMatch reports whether haystack is a useful hit for needle: exact
// substring, edit distance 1 (or 2 for longer needles) on the whole string or a
// path part, or enough shared trigrams for partial weight.
func fuzzyNeedleMatch(haystack, needle string) bool {
	needle = strings.ToLower(strings.TrimSpace(needle))
	haystack = strings.ToLower(strings.TrimSpace(haystack))
	if needle == "" {
		return true
	}
	if haystack == "" {
		return false
	}
	if strings.Contains(haystack, needle) {
		return true
	}
	maxDist := 1
	if utf8.RuneCountInString(needle) >= 6 {
		maxDist = 2
	}
	if editDistanceAtMost(haystack, needle, maxDist) {
		return true
	}
	for _, part := range strings.Split(haystack, "/") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		base := opsdb.TrigramSourceText(part)
		if strings.Contains(part, needle) || strings.Contains(base, needle) {
			return true
		}
		if editDistanceAtMost(part, needle, maxDist) || editDistanceAtMost(base, needle, maxDist) {
			return true
		}
		if trigramOverlapOK(part, needle) || trigramOverlapOK(base, needle) {
			return true
		}
	}
	return trigramOverlapOK(haystack, needle)
}

func trigramOverlapOK(haystack, needle string) bool {
	ng := opsdb.ExtractTrigrams(opsdb.TrigramSourceText(needle))
	if len(ng) == 0 {
		return false
	}
	hg := opsdb.ExtractTrigrams(opsdb.TrigramSourceText(haystack))
	if len(hg) == 0 {
		hg = opsdb.ExtractTrigrams(haystack)
	}
	if len(hg) == 0 {
		return false
	}
	hset := make(map[string]struct{}, len(hg))
	for _, g := range hg {
		hset[g] = struct{}{}
	}
	for _, g := range ng {
		if _, ok := hset[g]; ok {
			return true
		}
	}
	return false
}

// needleRelevanceScore ranks a haystack against needles (higher is better).
func needleRelevanceScore(haystack string, needles []string) int {
	best := 0
	for _, needle := range needles {
		if s := singleNeedleScore(haystack, needle); s > best {
			best = s
		}
	}
	return best
}

func singleNeedleScore(haystack, needle string) int {
	needle = strings.ToLower(strings.TrimSpace(needle))
	haystack = strings.ToLower(strings.TrimSpace(haystack))
	if needle == "" || haystack == "" {
		return 0
	}
	if strings.Contains(haystack, needle) {
		return 1000 + utf8.RuneCountInString(needle)
	}
	best := 0
	scorePart := func(part string) {
		if part == "" {
			return
		}
		if strings.Contains(part, needle) {
			if s := 1000 + utf8.RuneCountInString(needle); s > best {
				best = s
			}
			return
		}
		if editDistanceAtMost(part, needle, 1) {
			if best < 500 {
				best = 500
			}
			return
		}
		if editDistanceAtMost(part, needle, 2) && utf8.RuneCountInString(needle) >= 6 {
			if best < 400 {
				best = 400
			}
			return
		}
		ng := opsdb.ExtractTrigrams(opsdb.TrigramSourceText(needle))
		hg := opsdb.ExtractTrigrams(part)
		if len(ng) == 0 || len(hg) == 0 {
			return
		}
		hset := make(map[string]struct{}, len(hg))
		for _, g := range hg {
			hset[g] = struct{}{}
		}
		shared := 0
		for _, g := range ng {
			if _, ok := hset[g]; ok {
				shared++
			}
		}
		if s := shared * 10; s > best {
			best = s
		}
	}
	scorePart(haystack)
	for _, part := range strings.Split(haystack, "/") {
		scorePart(strings.TrimSpace(part))
	}
	return best
}

func editDistanceAtMost(a, b string, maxDist int) bool {
	if maxDist < 0 {
		return false
	}
	ar, br := []rune(a), []rune(b)
	la, lb := len(ar), len(br)
	if absInt(la-lb) > maxDist {
		return false
	}
	if la == 0 {
		return lb <= maxDist
	}
	if lb == 0 {
		return la <= maxDist
	}
	// Two-row DP; abort when every cell exceeds maxDist.
	prev := make([]int, lb+1)
	cur := make([]int, lb+1)
	for j := 0; j <= lb; j++ {
		prev[j] = j
	}
	for i := 1; i <= la; i++ {
		cur[0] = i
		rowMin := cur[0]
		for j := 1; j <= lb; j++ {
			cost := 1
			if ar[i-1] == br[j-1] {
				cost = 0
			}
			ins := cur[j-1] + 1
			del := prev[j] + 1
			sub := prev[j-1] + cost
			cur[j] = ins
			if del < cur[j] {
				cur[j] = del
			}
			if sub < cur[j] {
				cur[j] = sub
			}
			if cur[j] < rowMin {
				rowMin = cur[j]
			}
		}
		if rowMin > maxDist {
			return false
		}
		prev, cur = cur, prev
	}
	return prev[lb] <= maxDist
}

func absInt(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
