// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package filter

import "fmt"
import "strings"

func conditionLabel(cond Condition) string {
	prefix := "Filter out when "
	if cond.Negate {
		prefix = "Filter in when "
	}
	label := fmt.Sprintf("%s%s %s %s", prefix, cond.Field, cond.Operator, formatConditionValue(cond.Value))
	if cond.CaseSensitive && (cond.Field == FieldName || cond.Field == FieldPath) {
		return label + " (case sensitive)"
	}
	return label
}

func formatConditionValue(v any) string {
	items, ok := v.([]any)
	if !ok {
		if strs, isStrs := v.([]string); isStrs {
			return strings.Join(strs, ", ")
		}
		return fmt.Sprintf("%v", v)
	}
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, fmt.Sprintf("%v", item))
	}
	return strings.Join(parts, ", ")
}
