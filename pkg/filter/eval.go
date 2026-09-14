// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package filter

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

// EmptyRuleset reports whether rs has no meaningful root children.
func EmptyRuleset(rs *Ruleset) bool {
	return rs == nil || len(rs.RootGroup.Children) == 0
}

// EvaluateDetermining evaluates the ruleset in Go for discovery and path-review apply.
func (c *CompiledRuleset) EvaluateDetermining(node NodeInput, now time.Time) EvalOutcome {
	if c == nil {
		return EvalOutcome{Result: ResultPass, Outcome: OutcomePassed, Phase: phaseForNode(node)}
	}
	res, attr := c.evalGroup(c.Ruleset.RootGroup, node, now, true)
	return outcomeFrom(res, attr, phaseForNode(node), nil)
}

// EvaluateAllApplicable walks all applicable conditions (no short-circuit at leaves
// within a group for attribution collection). Group short-circuit still applies for
// the boolean result, but every non-SKIP condition is recorded for preview.
func (c *CompiledRuleset) EvaluateAllApplicable(node NodeInput, now time.Time) EvalOutcome {
	if c == nil {
		return EvalOutcome{Result: ResultPass, Outcome: OutcomePassed, Phase: phaseForNode(node)}
	}
	var matches []MatchDetail
	res, attr := c.evalGroupNoShort(c.Ruleset.RootGroup, node, now, &matches)
	return outcomeFrom(res, attr, phaseForNode(node), matches)
}

type attr struct {
	ruleID      string
	label       string
	message     string
	engineError bool
	phase       string
}

func outcomeFrom(res EvalResult, a attr, defaultPhase string, matches []MatchDetail) EvalOutcome {
	phase := a.phase
	if phase == "" {
		phase = defaultPhase
	}
	out := EvalOutcome{
		Result:      res,
		RuleID:      a.ruleID,
		Label:       a.label,
		Message:     a.message,
		Phase:       phase,
		EngineError: a.engineError,
		AllMatches:  matches,
	}
	switch {
	case a.engineError:
		out.Outcome = OutcomeEngineError
		out.Result = ResultFail
	case res == ResultPass:
		// Root Pass = exclusion rule matched → exclude the node.
		out.Outcome = OutcomeExcluded
	case res == ResultSkip:
		out.Result = ResultPass
		out.Outcome = OutcomePassed
	default:
		// Root Fail = exclusion rule did not match → keep the node.
		out.Outcome = OutcomePassed
		out.Result = ResultPass
	}
	return out
}

func phaseForNode(node NodeInput) string {
	if node.ChildCount != nil || node.IsEmpty != nil {
		return PhasePost
	}
	return PhasePre
}

func (c *CompiledRuleset) evalGroup(g Group, node NodeInput, now time.Time, shortCircuit bool) (EvalResult, attr) {
	var results []EvalResult
	var attrs []attr
	for _, ch := range g.Children {
		var r EvalResult
		var a attr
		switch {
		case ch.Condition != nil:
			r, a = c.evalCondition(*ch.Condition, node, now)
		case ch.Group != nil:
			r, a = c.evalGroup(*ch.Group, node, now, shortCircuit)
		default:
			continue
		}
		if r == ResultSkip {
			continue
		}
		results = append(results, r)
		attrs = append(attrs, a)
		if shortCircuit {
			if g.Op == OpAND && r == ResultFail {
				break
			}
			if g.Op == OpOR && r == ResultPass {
				break
			}
		}
	}
	if len(results) == 0 {
		return ResultSkip, attr{}
	}
	combined := results[0]
	det := attrs[0]
	if g.Op == OpOR {
		combined = ResultFail
		det = attr{}
		for i, r := range results {
			if r == ResultPass {
				combined = ResultPass
				det = attrs[i]
				break
			}
			if r == ResultFail && det.ruleID == "" {
				det = attrs[i] // last-fail fallback; overwritten when a pass appears
			}
		}
		// Prefer the first failing attr when OR fully fails.
		if combined == ResultFail {
			for i, r := range results {
				if r == ResultFail {
					det = attrs[i]
					break
				}
			}
		}
	} else {
		combined = ResultPass
		for i, r := range results {
			if r == ResultFail {
				combined = ResultFail
				det = attrs[i]
				break
			}
			if r == ResultPass {
				det = attrs[i]
			}
		}
	}
	if g.Negate {
		switch combined {
		case ResultPass:
			combined = ResultFail
		case ResultFail:
			combined = ResultPass
		}
	}
	return combined, det
}

func (c *CompiledRuleset) evalGroupNoShort(g Group, node NodeInput, now time.Time, matches *[]MatchDetail) (EvalResult, attr) {
	var results []EvalResult
	var attrs []attr
	for _, ch := range g.Children {
		var r EvalResult
		var a attr
		switch {
		case ch.Condition != nil:
			r, a = c.evalCondition(*ch.Condition, node, now)
			*matches = append(*matches, MatchDetail{
				RuleID:  ch.Condition.ID,
				Field:   ch.Condition.Field,
				Label:   conditionLabel(*ch.Condition),
				Result:  matchResultString(r, a.engineError),
				Phase:   conditionPhase(*ch.Condition),
				Message: a.message,
			})
		case ch.Group != nil:
			r, a = c.evalGroupNoShort(*ch.Group, node, now, matches)
		default:
			continue
		}
		if r == ResultSkip {
			continue
		}
		results = append(results, r)
		attrs = append(attrs, a)
	}
	if len(results) == 0 {
		return ResultSkip, attr{}
	}
	combined, det := combine(g.Op, results, attrs)
	if g.Negate {
		switch combined {
		case ResultPass:
			combined = ResultFail
		case ResultFail:
			combined = ResultPass
		}
	}
	return combined, det
}

func combine(op string, results []EvalResult, attrs []attr) (EvalResult, attr) {
	if op == OpOR {
		for i, r := range results {
			if r == ResultPass {
				return ResultPass, attrs[i]
			}
		}
		for i, r := range results {
			if r == ResultFail {
				return ResultFail, attrs[i]
			}
		}
		return ResultFail, attrs[0]
	}
	for i, r := range results {
		if r == ResultFail {
			return ResultFail, attrs[i]
		}
	}
	return ResultPass, attrs[len(attrs)-1]
}

func matchResultString(r EvalResult, engineErr bool) string {
	if engineErr {
		return OutcomeEngineError
	}
	switch r {
	case ResultPass:
		return "pass"
	case ResultFail:
		return "fail"
	default:
		return "skip"
	}
}

func conditionPhase(cond Condition) string {
	switch cond.Field {
	case FieldIsEmpty, FieldChildCount:
		return PhasePost
	default:
		return PhasePre
	}
}

func (c *CompiledRuleset) evalCondition(cond Condition, node NodeInput, now time.Time) (EvalResult, attr) {
	a := attr{
		ruleID: cond.ID,
		label:  conditionLabel(cond),
		phase:  conditionPhase(cond),
	}
	// Path is a string on every node (file or folder). applies_to:folder must not SKIP files
	// for path contains/eq, or path-part search silently drops file hits.
	appliesTo := cond.AppliesTo
	if cond.Field == FieldPath && appliesTo == AppliesFolder {
		appliesTo = AppliesBoth
	}
	if !applies(appliesTo, node.NodeType) {
		return ResultSkip, attr{}
	}
	// Post-only fields without post data → SKIP (not fail).
	if (cond.Field == FieldIsEmpty || cond.Field == FieldChildCount) &&
		node.ChildCount == nil && node.IsEmpty == nil {
		return ResultSkip, attr{}
	}
	// A name/path rule with its pattern list emptied is unfinished, not a rule that
	// matches everything. SKIP keeps Negate from turning it into a catch-all.
	if cond.Field == FieldName || cond.Field == FieldPath {
		if pats, err := patternList(cond.Value); err == nil && len(pats) == 0 {
			return ResultSkip, attr{}
		}
	}
	pass, err := c.matchCondition(cond, node, now)
	if err != nil {
		a.engineError = true
		a.message = err.Error()
		r := ResultFail
		if cond.Negate {
			r = ResultPass
		}
		return r, a
	}
	r := ResultFail
	if pass {
		r = ResultPass
	}
	if cond.Negate {
		if r == ResultPass {
			r = ResultFail
		} else {
			r = ResultPass
		}
	}
	a.message = a.label
	return r, a
}

func applies(appliesTo, nodeType string) bool {
	switch appliesTo {
	case AppliesBoth:
		return true
	case AppliesFile:
		return nodeType == NodeFile
	case AppliesFolder:
		return nodeType == NodeFolder
	default:
		return false
	}
}

func (c *CompiledRuleset) matchCondition(cond Condition, node NodeInput, now time.Time) (bool, error) {
	switch cond.Field {
	case FieldSize:
		if node.NodeType != NodeFile {
			return false, nil
		}
		n, err := valueInt64(cond.Value)
		if err != nil {
			return false, err
		}
		return compareInt(node.Size, cond.Operator, n)
	case FieldDepth:
		n, err := valueInt64(cond.Value)
		if err != nil {
			return false, err
		}
		return compareInt(int64(node.Depth), cond.Operator, n)
	case FieldChildCount:
		if node.ChildCount == nil {
			return false, nil
		}
		n, err := valueInt64(cond.Value)
		if err != nil {
			return false, err
		}
		return compareInt(int64(*node.ChildCount), cond.Operator, n)
	case FieldIsEmpty:
		empty := false
		if node.IsEmpty != nil {
			empty = *node.IsEmpty
		} else if node.ChildCount != nil {
			empty = *node.ChildCount == 0
		}
		want, err := valueBool(cond.Value)
		if err != nil {
			return false, err
		}
		switch cond.Operator {
		case OpEQ, "":
			return empty == want, nil
		case OpNEQ:
			return empty != want, nil
		default:
			return false, fmt.Errorf("is_empty: unsupported operator %q", cond.Operator)
		}
	case FieldExtension:
		ext := ExtensionOf(node.Name)
		return matchStringList(ext, cond.Operator, cond.Value, true)
	case FieldMimetypeCategory:
		cat := CategoryForExtension(ExtensionOf(node.Name))
		return matchStringList(cat, cond.Operator, cond.Value, true)
	case FieldName:
		return c.matchPattern(cond, node.Name)
	case FieldPath:
		return c.matchPattern(cond, node.DisplayPath)
	case FieldMTime:
		return matchMTime(node.MTime, cond.Operator, cond.Value, now)
	case FieldReviewStatus:
		return matchStringList(node.ReviewStatus, cond.Operator, cond.Value, true)
	case FieldPathIssueStatus:
		return matchStringList(node.PathIssueStatus, cond.Operator, cond.Value, true)
	case FieldPathIssueCategory:
		pats, err := patternList(cond.Value)
		if err != nil {
			return false, err
		}
		hay := strings.ToLower(node.PathIssueCategory)
		for _, p := range pats {
			if p != "" && strings.Contains(hay, strings.ToLower(p)) {
				return true, nil
			}
		}
		return false, nil
	default:
		return false, fmt.Errorf("unknown field %q", cond.Field)
	}
}

// matchPattern matches a name or path against one pattern or a list of them.
// A list matches when any entry matches, so "not equal" means none of them do.
func (c *CompiledRuleset) matchPattern(cond Condition, s string) (bool, error) {
	pats, err := patternList(cond.Value)
	if err != nil {
		return false, err
	}
	op := cond.Operator
	negate := op == OpNEQ
	if negate {
		op = OpEQ
	}
	for i, pat := range pats {
		ok, err := c.matchOnePattern(cond, op, pat, i, s)
		if err != nil {
			return false, err
		}
		if ok {
			return !negate, nil
		}
	}
	return negate, nil
}

func (c *CompiledRuleset) matchOnePattern(cond Condition, op, pat string, index int, s string) (bool, error) {
	switch op {
	case OpGlob:
		if cond.CaseSensitive {
			return filepath.Match(pat, s)
		}
		// Fold case so wildcards behave like eq and contains, which already ignore it.
		return filepath.Match(strings.ToLower(pat), strings.ToLower(s))
	case OpRegex:
		compiled := c.regexes[cond.ID]
		if index < len(compiled) && compiled[index] != nil {
			return compiled[index].MatchString(s), nil
		}
		re, err := compileNamePathRegex(pat, cond.CaseSensitive)
		if err != nil {
			return false, err
		}
		return re.MatchString(s), nil
	case OpContains:
		if cond.CaseSensitive {
			return strings.Contains(s, pat), nil
		}
		return strings.Contains(strings.ToLower(s), strings.ToLower(pat)), nil
	case OpEQ:
		if cond.CaseSensitive {
			return s == pat, nil
		}
		return strings.EqualFold(s, pat), nil
	default:
		return false, fmt.Errorf("unsupported string operator %q", op)
	}
}

func matchMTime(mtime, operator string, value any, now time.Time) (bool, error) {
	t, err := ParseNodeMTime(mtime)
	if err != nil {
		return false, err
	}
	switch operator {
	case OpOlderThan, OpNewerThan:
		s, err := valueString(value)
		if err != nil {
			return false, err
		}
		d, err := ParseRelativeDuration(s)
		if err != nil {
			return false, err
		}
		threshold := now.Add(-d)
		if operator == OpOlderThan {
			return t.Before(threshold), nil
		}
		return t.After(threshold), nil
	case OpBefore, OpAfter:
		s, err := valueString(value)
		if err != nil {
			return false, err
		}
		want, err := ParseNodeMTime(s)
		if err != nil {
			return false, err
		}
		if operator == OpBefore {
			return t.Before(want), nil
		}
		return t.After(want), nil
	default:
		return false, fmt.Errorf("mtime: unsupported operator %q", operator)
	}
}

func compareInt(have int64, op string, want int64) (bool, error) {
	switch op {
	case OpGT:
		return have > want, nil
	case OpLT:
		return have < want, nil
	case OpGTE:
		return have >= want, nil
	case OpLTE:
		return have <= want, nil
	case OpEQ:
		return have == want, nil
	case OpNEQ:
		return have != want, nil
	default:
		return false, fmt.Errorf("unsupported numeric operator %q", op)
	}
}

func matchStringList(have, op string, value any, fold bool) (bool, error) {
	list, err := valueStringSlice(value)
	if err != nil {
		return false, err
	}
	norm := func(s string) string {
		s = strings.TrimPrefix(strings.TrimSpace(s), ".")
		if fold {
			return strings.ToLower(s)
		}
		return s
	}
	haveN := norm(have)
	contains := false
	for _, item := range list {
		if norm(item) == haveN {
			contains = true
			break
		}
	}
	switch op {
	case OpEQ, OpIn, "":
		return contains, nil
	case OpNEQ:
		return !contains, nil
	default:
		return false, fmt.Errorf("unsupported list operator %q", op)
	}
}

func valueBool(v any) (bool, error) {
	switch t := v.(type) {
	case bool:
		return t, nil
	case string:
		return strconvParseBool(t)
	case float64:
		return t != 0, nil
	default:
		return false, fmt.Errorf("bool value required, got %T", v)
	}
}

func strconvParseBool(s string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "true", "yes", "y":
		return true, nil
	case "0", "false", "no", "n":
		return false, nil
	default:
		return false, fmt.Errorf("invalid bool %q", s)
	}
}
