// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package gpl

import (
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	pathgpl "codeberg.org/Sylos/go-path-linter/pkg/gpl"
	"codeberg.org/Sylos/go-path-linter/pkg/issue"
)

// GPLTargetFromProvider maps a Sylos provider id to a GPL built-in target.
func GPLTargetFromProvider(provider string) pathgpl.Target {
	p := NormalizeProviderID(provider)
	switch {
	case strings.Contains(p, "dropbox"):
		return pathgpl.Dropbox
	case strings.Contains(p, "box"):
		return pathgpl.Box
	case strings.Contains(p, "egnyte"):
		return pathgpl.Egnyte
	case strings.Contains(p, "onedrive") || strings.Contains(p, "one_drive"):
		return pathgpl.OneDrive
	case strings.Contains(p, "sharepoint") || strings.Contains(p, "share_point"):
		return pathgpl.SharePoint
	case strings.Contains(p, "sharefile") || strings.Contains(p, "share_file"):
		return pathgpl.ShareFile
	case strings.Contains(p, "windows"):
		return pathgpl.Windows
	case strings.Contains(p, "darwin") || strings.Contains(p, "macos") || strings.Contains(p, "mac"):
		return pathgpl.MacOS
	default:
		return pathgpl.Linux
	}
}

// NormalizeProviderID lowercases and normalizes separators for provider comparisons.
func NormalizeProviderID(provider string) string {
	p := strings.ToLower(strings.TrimSpace(provider))
	p = strings.ReplaceAll(p, "-", "_")
	p = strings.ReplaceAll(p, " ", "_")
	return p
}

// ResolvePathCheckTarget returns the provider id whose path rules should be applied,
// or "" when destination-name checks should be skipped.
//
// profile:
//   - "" / "auto" — check only when src and dst service types differ; target = dst
//   - "none" / "off" / "skip" — never check
//   - otherwise — always check against that profile (e.g. "windows" for local→local cross-OS)
func ResolvePathCheckTarget(srcProvider, dstProvider, profile string) string {
	p := NormalizeProviderID(profile)
	switch p {
	case "none", "off", "disabled", "skip":
		return ""
	case "", "auto":
		src := NormalizeProviderID(srcProvider)
		dst := NormalizeProviderID(dstProvider)
		if src == "" || dst == "" {
			if dst != "" {
				return dst
			}
			return "linux"
		}
		if src == dst {
			return ""
		}
		return dst
	default:
		return p
	}
}

// PathChecksRequired reports whether destination-name checks should run.
func PathChecksRequired(srcProvider, dstProvider, profile string) bool {
	return ResolvePathCheckTarget(srcProvider, dstProvider, profile) != ""
}

// appendDSTSiblingCollisionIssues upserts manual_review GPL issues when a SRC child's
// intended/cleaned destination basename collides with a DST-listed sibling.
// Append-only (AppendGPLIssue); call after DST compare seal. No node UPDATEs.
func AppendDSTSiblingCollisionIssues(database *db.DB, q *queue.Queue, task *queue.TaskBase) {
	if database == nil || q == nil || task == nil {
		return
	}
	checkTarget := ResolvePathCheckTarget(q.ScalingSrcProvider(), q.ScalingDstProvider(), q.PathCheckProfile())
	if checkTarget == "" {
		return
	}
	target := GPLTargetFromProvider(checkTarget)
	windowsCompat := q.WindowsCompat()

	dstBases := make([]string, 0, len(task.DiscoveredChildren))
	dstSet := make(map[string]struct{}, len(task.DiscoveredChildren))
	for _, c := range task.DiscoveredChildren {
		base := discoveredChildBasename(c)
		if base == "" {
			continue
		}
		if _, ok := dstSet[base]; ok {
			continue
		}
		dstSet[base] = struct{}{}
		dstBases = append(dstBases, base)
	}
	if len(dstBases) == 0 {
		return
	}

	parentLen := LoadParentPathLen(database, task.ID, task.GPLState)
	seen := make(map[string]struct{})
	// SRC nodes already paired via exact match or cleaned-name rematch.
	mappedSRC := make(map[string]struct{})
	for _, c := range task.DiscoveredChildren {
		if c.SrcID != "" {
			mappedSRC[c.SrcID] = struct{}{}
		}
	}

	consider := func(srcID, basename string, isFile bool) {
		if srcID == "" || basename == "" {
			return
		}
		if _, ok := seen[srcID]; ok {
			return
		}
		seen[srcID] = struct{}{}
		if _, ok := mappedSRC[srcID]; ok {
			return
		}
		// Exclude this node's own basename from sibling set so AE self-match is not a collision.
		siblings := make([]string, 0, len(dstBases))
		for _, s := range dstBases {
			if s == basename {
				continue
			}
			siblings = append(siblings, s)
		}
		if len(siblings) == 0 {
			return
		}
		payload, pe := evaluateGPLAddPart(target, parentLen, basename, siblings, isFile, windowsCompat)
		collides := payload.Part.Collision
		if !collides && pe != nil && pe.ProposedName != "" {
			if _, ok := dstSet[pe.ProposedName]; ok && pe.ProposedName != basename {
				collides = true
			}
		}
		if !collides {
			return
		}
		issuesJSON := ""
		if pe != nil {
			issuesJSON = pe.IssuesJSON
		}
		database.AppendGPLIssue(db.GPLIssue{
			SrcID:        srcID,
			Status:       db.GPLIssueStatusManualReview,
			ProposedName: "",
			IssuesJSON:   issuesJSON,
		})
	}

	for _, f := range task.ExpectedFolders {
		key := queue.DstChildMatchKey(f.Type, f.DisplayName, f.LocationPath)
		srcID := ""
		if task.ExpectedSrcIDMap != nil {
			srcID = task.ExpectedSrcIDMap[key]
		}
		consider(srcID, queue.DstChildMatchName(f.DisplayName, f.LocationPath), false)
	}
	for _, f := range task.ExpectedFiles {
		key := queue.DstChildMatchKey(f.Type, f.DisplayName, f.LocationPath)
		srcID := ""
		if task.ExpectedSrcIDMap != nil {
			srcID = task.ExpectedSrcIDMap[key]
		}
		consider(srcID, queue.DstChildMatchName(f.DisplayName, f.LocationPath), true)
	}
}

func discoveredChildBasename(c queue.ChildResult) string {
	if c.IsFile {
		return queue.DstChildMatchName(c.File.DisplayName, c.File.LocationPath)
	}
	return queue.DstChildMatchName(c.Folder.DisplayName, c.Folder.LocationPath)
}

// ApplyGPLToSRCChildren runs destination-rule GPL on each SRC child via parent path_len + AddPart.
// Sets NodeState.GPLState JSON and enqueues sparse GPL issues for pending/manual review.
// When skipChecks is true (same-provider migration), only identity path_len is stored.
func ApplyGPLToSRCChildren(database *db.DB, target pathgpl.Target, parentPathLen int, children []*db.NodeState, skipChecks, windowsCompat bool) {
	if db.GPLDisabled {
		return
	}
	if len(children) == 0 {
		return
	}
	if skipChecks {
		applyPassthroughPathLenToSRCChildren(children, parentPathLen)
		return
	}
	siblings := make([]string, 0, len(children))
	for _, c := range children {
		if c == nil {
			continue
		}
		siblings = append(siblings, db.NormalizeNodeBasename(c.Name))
	}
	for _, c := range children {
		if c == nil {
			continue
		}
		base := db.NormalizeNodeBasename(c.Name)
		isFile := c.Type == db.NodeTypeFile
		payload, pe := evaluateGPLAddPart(target, parentPathLen, base, siblings, isFile, windowsCompat)
		if b, err := json.Marshal(payload); err == nil {
			c.GPLState = string(b)
		}
		if pe != nil {
			pe.SrcID = c.ID
			pe.UpdatedAt = 0 // seal buffer fills
			if database != nil {
				database.AppendGPLIssue(*pe)
			}
		}
	}
}

// applyPassthroughPathLenToSRCChildren writes valid gpl_state with path_len and no lint findings.
func applyPassthroughPathLenToSRCChildren(children []*db.NodeState, parentPathLen int) {
	for _, c := range children {
		if c == nil {
			continue
		}
		base := db.NormalizeNodeBasename(c.Name)
		payload := db.GPLStatePayload{
			Valid:   true,
			Part:    db.GPLScopeState{Valid: true},
			Path:    db.GPLScopeState{Valid: true},
			PathLen: pathLenAfterLeaf(parentPathLen, base, "/"),
		}
		if b, err := json.Marshal(payload); err == nil {
			c.GPLState = string(b)
		}
	}
}

// LoadParentPathLen returns the parent's stored path_len, reading src_nodes when not supplied.
func LoadParentPathLen(database *db.DB, parentID string, parentGPLState string) int {
	if n := ParseGPLPathLen(parentGPLState); n > 0 || parentGPLState != "" {
		return n
	}
	if database == nil || parentID == "" || database.Ops() == nil {
		return 0
	}
	n, ok, err := database.Ops().GetNode("src", parentID)
	if err != nil || !ok {
		return 0
	}
	return ParseGPLPathLen(n.GPLState)
}

func evaluateGPLAddPart(target pathgpl.Target, parentPathLen int, basename string, siblings []string, isFile, windowsCompat bool) (db.GPLStatePayload, *db.GPLIssue) {
	out := db.GPLStatePayload{
		Valid: true,
		Part:  db.GPLScopeState{Valid: true},
		Path:  db.GPLScopeState{Valid: true},
	}
	opts := []pathgpl.Option{
		pathgpl.WithRelative(true),
		pathgpl.WithAutoClean(true),
		pathgpl.WithRaiseErrors(false),
		pathgpl.WithSiblings(siblings),
		pathgpl.WithAutoValidate(false),
		pathgpl.WithParentPathLen(parentPathLen),
		// Never collapse hierarchy by deleting a path part (empty-after-strip, etc.).
		pathgpl.WithDisallowPartRemoval(true),
	}
	if isFile {
		opts = append(opts, pathgpl.WithFileAdded(true))
	}
	l, err := pathgpl.NewWithWindowsCompat(target, "", windowsCompat, opts...)
	if err != nil {
		out.Valid = false
		out.Part.Valid = false
		out.Part.Categories = []string{"gpl_init_error"}
		return out, nil
	}
	partOpts := []pathgpl.PartOption(nil)
	if isFile {
		partOpts = append(partOpts, pathgpl.AsFile())
	}
	_ = l.AddPart(basename, partOpts...)
	leafIdx := 0
	cleaned, _ := l.Clean()
	cleanedParts := l.Parts()
	cleanedBase := basename
	if len(cleanedParts) > 0 {
		cleanedBase = cleanedParts[len(cleanedParts)-1]
		leafIdx = len(cleanedParts) - 1
	} else {
		cleanedBase = path.Base(strings.ReplaceAll(cleaned, "\\", "/"))
		if cleanedBase == "." || cleanedBase == "/" {
			cleanedBase = cleaned
		}
	}

	partCats := make([]string, 0)
	pathCats := make([]string, 0)
	collision := false
	manualReview := false
	// Leaf-only eval: PartIndex is relative to the leaf-only parts slice.
	for _, act := range l.Log.Actions {
		if act.Category == "" || act.PartIndex != leafIdx {
			continue
		}
		partCats = append(partCats, string(act.Category))
		if act.Category == issue.CategoryEmptyPart || act.Kind == issue.KindRemove {
			manualReview = true
		}
	}
	for _, iss := range l.Log.Issues {
		if iss.Scope == issue.ScopePath {
			pathCats = append(pathCats, string(iss.Category))
			continue
		}
		if iss.PartIndex != leafIdx {
			continue
		}
		partCats = append(partCats, string(iss.Category))
		if iss.Category == issue.CategorySiblingCollision {
			collision = true
		}
		if iss.Category == issue.CategoryEmptyPart {
			manualReview = true
		}
	}
	if len(cleanedParts) == 0 && strings.TrimSpace(basename) != "" {
		manualReview = true
	}
	if strings.TrimSpace(cleanedBase) == "" && strings.TrimSpace(basename) != "" {
		manualReview = true
		cleanedBase = ""
	}

	out.Part.Categories = partCats
	out.Part.Collision = collision
	out.Path.Categories = pathCats
	if !manualReview && cleanedBase != "" && cleanedBase != basename {
		out.Part.ProposedClean = cleanedBase
	}
	out.Part.Valid = !collision && !manualReview && len(partCats) == 0
	out.Path.Valid = len(pathCats) == 0
	out.PathLen = l.PathLength()
	if out.PathLen == 0 && cleanedBase != "" {
		out.PathLen = pathLenAfterLeaf(parentPathLen, cleanedBase, "/")
	}
	out.Valid = out.Part.Valid && out.Path.Valid

	leafIssues := filterIssuesForPartIndex(mergeIssuesFromActions(l.Log.Issues, l.Log.Actions), leafIdx)
	issuesJSON, _ := json.Marshal(leafIssues)
	if collision {
		return out, &db.GPLIssue{
			ProposedName: firstNonEmpty(out.Part.ProposedClean, cleanedBase, basename),
			Status:       db.GPLIssueStatusManualReview,
			IssuesJSON:   string(issuesJSON),
		}
	}
	if manualReview {
		return out, &db.GPLIssue{
			ProposedName: "",
			Status:       db.GPLIssueStatusManualReview,
			IssuesJSON:   string(issuesJSON),
		}
	}
	if out.Part.ProposedClean != "" {
		return out, &db.GPLIssue{
			ProposedName: out.Part.ProposedClean,
			Status:       db.GPLIssueStatusPending,
			IssuesJSON:   string(issuesJSON),
		}
	}
	return out, nil
}

func pathLenAfterLeaf(parentPathLen int, leaf, sep string) int {
	leaf = strings.TrimSpace(leaf)
	if parentPathLen <= 0 {
		return len(leaf)
	}
	if leaf == "" {
		return parentPathLen
	}
	return parentPathLen + len(sep) + len(leaf)
}

// filterIssuesForPartIndex keeps path-scoped issues and part-local issues at partIndex.
func filterIssuesForPartIndex(issues []issue.Issue, partIndex int) []issue.Issue {
	if len(issues) == 0 {
		return nil
	}
	out := make([]issue.Issue, 0, len(issues))
	for _, iss := range issues {
		if iss.Scope == issue.ScopePath || iss.PartIndex == partIndex {
			out = append(out, iss)
		}
	}
	return out
}

// mergeIssuesFromActions ensures Clean Actions (e.g. TrailingSpace) are persisted as Issues
// for the review queue when Validate-after-clean left Log.Issues empty.
func mergeIssuesFromActions(issues []issue.Issue, actions []issue.Action) []issue.Issue {
	out := append([]issue.Issue(nil), issues...)
	seen := make(map[string]struct{}, len(issues)+len(actions))
	for _, iss := range issues {
		key := string(iss.Category) + "\x00" + iss.Part + "\x00" + iss.Detail
		seen[key] = struct{}{}
	}
	for _, act := range actions {
		if act.Category == "" {
			continue
		}
		key := string(act.Category) + "\x00" + act.Original + "\x00"
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, issue.Issue{
			Category:    act.Category,
			PartIndex:   act.PartIndex,
			Part:        act.Original,
			Message:     act.Reason,
			Detail:      act.Detail,
			UserMessage: act.UserMessage,
			DocsURL:     act.DocsURL,
		})
	}
	return out
}

// mergePathScopeIntoGPLState refreshes only the path scope of an existing gpl_state JSON.
func mergePathScopeIntoGPLState(existing string, pathState db.GPLScopeState, pathLen int) string {
	var p db.GPLStatePayload
	if existing != "" {
		_ = json.Unmarshal([]byte(existing), &p)
	}
	p.Path = pathState
	if pathLen > 0 {
		p.PathLen = pathLen
	}
	p.Valid = p.Part.Valid && p.Path.Valid && !p.Part.Collision
	b, err := json.Marshal(p)
	if err != nil {
		return existing
	}
	return string(b)
}

// EvaluateGPLPathOnly revalidates path-scoped rules using parent path_len + leaf basename.
func EvaluateGPLPathOnly(target pathgpl.Target, parentPathLen int, leaf string, existingGPLState string, isFile, windowsCompat bool) (string, []issue.Issue, error) {
	opts := []pathgpl.Option{
		pathgpl.WithRelative(true),
		pathgpl.WithAutoClean(false),
		pathgpl.WithRaiseErrors(false),
		pathgpl.WithAutoValidate(false),
		pathgpl.WithParentPathLen(parentPathLen),
	}
	if isFile {
		opts = append(opts, pathgpl.WithFileAdded(true))
	}
	l, err := pathgpl.NewWithWindowsCompat(target, "", windowsCompat, opts...)
	if err != nil {
		return existingGPLState, nil, err
	}
	// Restore part-local issues onto Log so ValidatePath preserves them.
	if existingGPLState != "" {
		var p db.GPLStatePayload
		if json.Unmarshal([]byte(existingGPLState), &p) == nil {
			for _, cat := range p.Part.Categories {
				l.Log.AddIssue(issue.Issue{Category: issue.Category(cat), Scope: issue.ScopePart})
			}
		}
	}
	if leaf != "" {
		_ = l.AddPart(leaf)
	}
	_ = l.ValidatePath()

	pathCats := make([]string, 0)
	var pathIssues []issue.Issue
	for _, iss := range l.Log.Issues {
		if iss.Scope == issue.ScopePath {
			pathCats = append(pathCats, string(iss.Category))
			pathIssues = append(pathIssues, iss)
		}
	}
	pathState := db.GPLScopeState{Valid: len(pathCats) == 0, Categories: pathCats}
	merged := mergePathScopeIntoGPLState(existingGPLState, pathState, l.PathLength())
	return merged, pathIssues, nil
}

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}

// ParseGPLPathLen returns the migration-relative path length stored on a node's gpl_state.
func ParseGPLPathLen(gplState string) int {
	if gplState == "" {
		return 0
	}
	var p db.GPLStatePayload
	if err := json.Unmarshal([]byte(gplState), &p); err != nil {
		return 0
	}
	return p.PathLen
}

// ProcessGPLTaskSRC revalidates path scope from parent path_len + leaf name and persists gpl_state.
func ProcessGPLTaskSRC(database *db.DB, target pathgpl.Target, task *queue.TaskBase, windowsCompat bool) error {
	if database == nil || task == nil {
		return fmt.Errorf("nil database or task")
	}
	parentLen := ParseGPLPathLen(task.ParentGPLState)
	base := task.ResolvedDstName
	if base == "" {
		if task.IsFile() {
			base = db.NormalizeNodeBasename(task.File.DisplayName)
		} else {
			base = db.NormalizeNodeBasename(task.Folder.DisplayName)
		}
	}
	if base == "" {
		base = db.NormalizeNodeBasename(task.LocationPath())
	}
	merged, pathIssues, err := EvaluateGPLPathOnly(target, parentLen, base, task.GPLState, task.IsFile(), windowsCompat)
	if err != nil {
		return err
	}
	if err := database.UpdateNodeGPLState(task.ID, merged); err != nil {
		return err
	}
	if len(pathIssues) == 0 {
		return nil
	}
	issuesJSON, _ := json.Marshal(pathIssues)
	database.AppendGPLIssue(db.GPLIssue{
		SrcID:      task.ID,
		UpdatedAt:  time.Now().UnixNano(),
		Status:     db.GPLIssueStatusManualReview,
		IssuesJSON: string(issuesJSON),
	})
	return nil
}
