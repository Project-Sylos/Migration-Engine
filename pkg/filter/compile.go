// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package filter

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// CompiledRuleset is a validated ruleset with precompiled regexes.
type CompiledRuleset struct {
	Ruleset Ruleset
	regexes map[string][]*regexp.Regexp // condition id → compiled patterns, in value order
}

// Compile validates schema_version, structure, and compiles regex patterns.
func Compile(rs Ruleset) (*CompiledRuleset, error) {
	if rs.SchemaVersion != SchemaVersion {
		return nil, fmt.Errorf("unsupported schema_version %d (want %d)", rs.SchemaVersion, SchemaVersion)
	}
	if rs.RootGroup.Op != OpAND && rs.RootGroup.Op != OpOR {
		return nil, fmt.Errorf("root_group.op must be AND or OR, got %q", rs.RootGroup.Op)
	}
	out := &CompiledRuleset{Ruleset: rs, regexes: make(map[string][]*regexp.Regexp)}
	if err := compileGroup(&rs.RootGroup, out); err != nil {
		return nil, err
	}
	return out, nil
}

func compileGroup(g *Group, out *CompiledRuleset) error {
	if g.Op != OpAND && g.Op != OpOR {
		return fmt.Errorf("group op must be AND or OR, got %q", g.Op)
	}
	if len(g.Children) == 0 {
		return fmt.Errorf("group has no children")
	}
	for i := range g.Children {
		ch := &g.Children[i]
		switch {
		case ch.Condition != nil && ch.Group != nil:
			return fmt.Errorf("child cannot set both condition and group")
		case ch.Condition != nil:
			if err := compileCondition(ch.Condition, out); err != nil {
				return err
			}
		case ch.Group != nil:
			if err := compileGroup(ch.Group, out); err != nil {
				return err
			}
		default:
			return fmt.Errorf("child must set condition or group")
		}
	}
	return nil
}

func compileCondition(c *Condition, out *CompiledRuleset) error {
	if c.ID == "" {
		return fmt.Errorf("condition id is required")
	}
	switch c.AppliesTo {
	case AppliesFile, AppliesFolder, AppliesBoth:
	default:
		return fmt.Errorf("condition %s: invalid applies_to %q", c.ID, c.AppliesTo)
	}
	switch c.Field {
	case FieldSize, FieldMTime, FieldName, FieldPath, FieldExtension,
		FieldMimetypeCategory, FieldDepth, FieldIsEmpty, FieldChildCount,
		FieldReviewStatus, FieldPathIssueStatus, FieldPathIssueCategory:
	default:
		return fmt.Errorf("condition %s: unknown field %q", c.ID, c.Field)
	}
	if c.Operator == OpRegex && (c.Field == FieldName || c.Field == FieldPath) {
		pats, err := patternList(c.Value)
		if err != nil {
			return fmt.Errorf("condition %s: %w", c.ID, err)
		}
		compiled := make([]*regexp.Regexp, 0, len(pats))
		for _, pat := range pats {
			re, err := compileNamePathRegex(pat, c.CaseSensitive)
			if err != nil {
				return fmt.Errorf("condition %s: invalid regex: %w", c.ID, err)
			}
			compiled = append(compiled, re)
		}
		out.regexes[c.ID] = compiled
	}
	if c.Field == FieldMTime && (c.Operator == OpOlderThan || c.Operator == OpNewerThan) {
		s, err := valueString(c.Value)
		if err != nil {
			return fmt.Errorf("condition %s: %w", c.ID, err)
		}
		if _, err := ParseRelativeDuration(s); err != nil {
			return fmt.Errorf("condition %s: %w", c.ID, err)
		}
	}
	if c.Field == FieldMTime && (c.Operator == OpBefore || c.Operator == OpAfter) {
		s, err := valueString(c.Value)
		if err != nil {
			return fmt.Errorf("condition %s: %w", c.ID, err)
		}
		if _, err := ParseNodeMTime(s); err != nil {
			return fmt.Errorf("condition %s: %w", c.ID, err)
		}
	}
	return nil
}

func valueString(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), nil
	case int:
		return strconv.Itoa(t), nil
	case int64:
		return strconv.FormatInt(t, 10), nil
	case bool:
		return strconv.FormatBool(t), nil
	default:
		return "", fmt.Errorf("value must be string-compatible, got %T", v)
	}
}

func valueInt64(v any) (int64, error) {
	switch t := v.(type) {
	case float64:
		return int64(t), nil
	case int:
		return int64(t), nil
	case int64:
		return t, nil
	case string:
		return strconv.ParseInt(strings.TrimSpace(t), 10, 64)
	default:
		return 0, fmt.Errorf("numeric value required, got %T", v)
	}
}

// patternList reads name/path values, which may hold one pattern or several.
// Unlike valueStringSlice a lone string stays intact, since commas are legal in names.
func patternList(v any) ([]string, error) {
	switch t := v.(type) {
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			s, err := valueString(item)
			if err != nil {
				return nil, err
			}
			out = append(out, s)
		}
		return out, nil
	case []string:
		return t, nil
	default:
		s, err := valueString(v)
		if err != nil {
			return nil, err
		}
		return []string{s}, nil
	}
}

func valueStringSlice(v any) ([]string, error) {
	s, ok := v.(string)
	if !ok {
		return patternList(v)
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out, nil
}

func compileNamePathRegex(pat string, caseSensitive bool) (*regexp.Regexp, error) {
	if caseSensitive {
		return regexp.Compile(pat)
	}
	return regexp.Compile("(?i)" + pat)
}
