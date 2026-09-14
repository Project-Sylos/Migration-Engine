// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package filter

// SchemaVersion is the only supported ruleset schema for v1.
const SchemaVersion = 1

// Group operators.
const (
	OpAND = "AND"
	OpOR  = "OR"
)

// AppliesTo values on conditions.
const (
	AppliesFile   = "file"
	AppliesFolder = "folder"
	AppliesBoth   = "both"
)

// Condition fields.
const (
	FieldSize             = "size"
	FieldMTime            = "mtime"
	FieldName             = "name"
	FieldPath             = "path"
	FieldExtension        = "extension"
	FieldMimetypeCategory = "mimetype_category"
	FieldDepth            = "depth"
	FieldIsEmpty          = "is_empty"
	FieldChildCount       = "child_count"
	// Review-search-only fields (need events-derived current / gpl_issues joins).
	FieldReviewStatus       = "review_status"
	FieldPathIssueStatus    = "path_issue_status"
	FieldPathIssueCategory  = "path_issue_category"
)

// Operators.
const (
	OpGT         = "gt"
	OpLT         = "lt"
	OpGTE        = "gte"
	OpLTE        = "lte"
	OpEQ         = "eq"
	OpNEQ        = "neq"
	OpIn         = "in"
	OpOlderThan  = "older_than"
	OpNewerThan  = "newer_than"
	OpBefore     = "before"
	OpAfter      = "after"
	OpGlob       = "glob"
	OpRegex      = "regex"
	OpContains   = "contains"
)

// EvalResult is the three-state condition/group outcome.
type EvalResult int

const (
	ResultPass EvalResult = iota
	ResultFail
	ResultSkip
)

// Result strings for exclusion outcomes.
const (
	OutcomePassed      = "passed"
	OutcomeExcluded    = "excluded"
	OutcomeEngineError = "engine_error"
)

// Eval phases.
const (
	PhasePre  = "pre"
	PhasePost = "post"
)

// NodeType values for eval input.
const (
	NodeFile   = "file"
	NodeFolder = "folder"
)

// Ruleset is a named, versioned boolean expression tree.
type Ruleset struct {
	RulesetID     string `json:"ruleset_id"`
	Name          string `json:"name"`
	Description   string `json:"description,omitempty"`
	CreatedBy     string `json:"created_by,omitempty"` // user | prepackaged
	SchemaVersion int    `json:"schema_version"`
	RootGroup     Group  `json:"root_group"`
}

// Group is a recursive AND/OR node. Negate inverts the non-SKIP result.
type Group struct {
	ID       string  `json:"id,omitempty"`
	Op       string  `json:"op"` // AND | OR
	Negate   bool    `json:"negate"`
	Children []Child `json:"children"`
}

// Child is either a Condition or a nested Group (exactly one set).
type Child struct {
	Condition *Condition `json:"condition,omitempty"`
	Group     *Group     `json:"group,omitempty"`
}

// Condition is a leaf predicate.
type Condition struct {
	ID        string `json:"id"` // stable rule_id
	Field     string `json:"field"`
	Operator  string `json:"operator"`
	Value     any    `json:"value"`
	Negate    bool   `json:"negate"`
	AppliesTo string `json:"applies_to"` // file | folder | both
	// CaseSensitive applies to name/path string operators (contains, glob, regex, eq).
	// Default false: fold case. Omitted in JSON keeps existing rules case-insensitive.
	CaseSensitive bool `json:"case_sensitive,omitempty"`
}

// NodeInput is the intrinsic + optional post-node data for evaluation.
type NodeInput struct {
	Name        string
	DisplayPath string
	Size        int64
	MTime       string // RFC3339 or provider timestamp string
	Depth       int
	NodeType    string // file | folder
	// Post-node (optional; only when children have been enumerated).
	ChildCount *int
	IsEmpty    *bool
	// Path-review search context (optional; empty → review fields fail match, not engine error).
	ReviewStatus      string
	PathIssueStatus   string
	PathIssueCategory string
}

// MatchDetail is one applicable condition outcome (preview mode).
type MatchDetail struct {
	RuleID  string `json:"rule_id"`
	Field   string `json:"field"`
	Label   string `json:"label"`
	Result  string `json:"result"` // pass | fail | skip | engine_error
	Phase   string `json:"phase"`  // pre | post
	Message string `json:"message,omitempty"`
}

// EvalOutcome is the top-level result plus attribution for the determining condition.
type EvalOutcome struct {
	Result       EvalResult
	Outcome      string // passed | excluded | engine_error
	RuleID       string
	Label        string
	Message      string
	Phase        string
	EngineError  bool
	AllMatches   []MatchDetail // filled only in EvaluateAllApplicable
}
