// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"encoding/json"
	"path"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/go-path-linter/pkg/gpl"
	"codeberg.org/Sylos/go-path-linter/pkg/issue"
)

// gplTargetFromProvider maps a Sylos provider id to a GPL built-in target.
func gplTargetFromProvider(provider string) gpl.Target {
	return GPLTargetFromProvider(provider)
}

// GPLTargetFromProvider maps a Sylos provider id to a GPL built-in target (exported for migration store).
func GPLTargetFromProvider(provider string) gpl.Target {
	p := NormalizeProviderID(provider)
	switch {
	case strings.Contains(p, "dropbox"):
		return gpl.Dropbox
	case strings.Contains(p, "box"):
		return gpl.Box
	case strings.Contains(p, "egnyte"):
		return gpl.Egnyte
	case strings.Contains(p, "onedrive") || strings.Contains(p, "one_drive"):
		return gpl.OneDrive
	case strings.Contains(p, "sharepoint") || strings.Contains(p, "share_point"):
		return gpl.SharePoint
	case strings.Contains(p, "sharefile") || strings.Contains(p, "share_file"):
		return gpl.ShareFile
	case strings.Contains(p, "windows"):
		return gpl.Windows
	case strings.Contains(p, "darwin") || strings.Contains(p, "macos") || strings.Contains(p, "mac"):
		return gpl.MacOS
	default:
		return gpl.Linux
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

// applyGPLToSRCChildren runs destination-rule GPL on each SRC child via parent parts + AddPart.
// Sets NodeState.GPLState JSON and enqueues path_events for collisions / pending cleans.
// When skipChecks is true (same-provider migration), only identity parts are stored — no path_events.
func applyGPLToSRCChildren(database *db.DB, target gpl.Target, parentParts []string, children []*db.NodeState, skipChecks bool) {
	if len(children) == 0 {
		return
	}
	if skipChecks {
		applyPassthroughPartsToSRCChildren(children, parentParts)
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
		payload, pe := evaluateGPLAddPart(target, parentParts, base, siblings, isFile)
		if b, err := json.Marshal(payload); err == nil {
			c.GPLState = string(b)
		}
		if pe != nil {
			pe.ID = c.ID
			pe.EventTime = 0 // seal buffer fills
			if database != nil {
				database.AppendPathEvent(*pe)
			}
		}
	}
}

// applyPassthroughPartsToSRCChildren writes valid gpl_state.parts without linting or path_events.
func applyPassthroughPartsToSRCChildren(children []*db.NodeState, parentParts []string) {
	for _, c := range children {
		if c == nil {
			continue
		}
		base := db.NormalizeNodeBasename(c.Name)
		parts := append(append([]string(nil), parentParts...), base)
		payload := db.GPLStatePayload{
			Valid: true,
			Part:  db.GPLScopeState{Valid: true},
			Path:  db.GPLScopeState{Valid: true},
			Parts: parts,
		}
		if b, err := json.Marshal(payload); err == nil {
			c.GPLState = string(b)
		}
	}
}

// loadParentGPLParts returns effective migration-relative parts for the parent node.
func loadParentGPLParts(database *db.DB, parentID string, parentGPLState string) []string {
	if parts := parseGPLParts(parentGPLState); len(parts) > 0 || parentGPLState != "" {
		return parts
	}
	if database == nil || parentID == "" {
		return nil
	}
	conn, err := database.GetDB()
	if err != nil {
		return nil
	}
	var gplState string
	_ = conn.QueryRowContext(context.Background(),
		`SELECT COALESCE(gpl_state,'') FROM src_nodes WHERE id = $1`, parentID,
	).Scan(&gplState)
	return parseGPLParts(gplState)
}

func evaluateGPLAddPart(target gpl.Target, parentParts []string, basename string, siblings []string, isFile bool) (db.GPLStatePayload, *db.PathEvent) {
	out := db.GPLStatePayload{
		Valid: true,
		Part:  db.GPLScopeState{Valid: true},
		Path:  db.GPLScopeState{Valid: true},
	}
	opts := []gpl.Option{
		gpl.WithRelative(true),
		gpl.WithAutoClean(true),
		gpl.WithRaiseErrors(false),
		gpl.WithSiblings(siblings),
		gpl.WithAutoValidate(false),
	}
	if isFile {
		opts = append(opts, gpl.WithFileAdded(true))
	}
	l, err := gpl.New(target, "", opts...)
	if err != nil {
		out.Valid = false
		out.Part.Valid = false
		out.Part.Categories = []string{"gpl_init_error"}
		return out, nil
	}
	if len(parentParts) > 0 {
		l.SetParts(parentParts)
	}
	partOpts := []gpl.PartOption(nil)
	if isFile {
		partOpts = append(partOpts, gpl.AsFile())
	}
	_ = l.AddPart(basename, partOpts...)
	cleaned, _ := l.Clean()
	cleanedParts := l.Parts()
	cleanedBase := basename
	if len(cleanedParts) > 0 {
		cleanedBase = cleanedParts[len(cleanedParts)-1]
	} else {
		cleanedBase = path.Base(strings.ReplaceAll(cleaned, "\\", "/"))
		if cleanedBase == "." || cleanedBase == "/" {
			cleanedBase = cleaned
		}
	}

	partCats := make([]string, 0)
	pathCats := make([]string, 0)
	collision := false
	// Clean() re-validates the *cleaned* parts, so TrailingSpace/etc. often appear only as
	// Actions (original → cleaned), not as Issues. Treat those Actions as part-local findings.
	for _, act := range l.Log.Actions {
		if act.Category == "" {
			continue
		}
		partCats = append(partCats, string(act.Category))
	}
	for _, iss := range l.Log.Issues {
		if iss.Scope == issue.ScopePath {
			pathCats = append(pathCats, string(iss.Category))
			continue
		}
		partCats = append(partCats, string(iss.Category))
		if iss.Category == issue.CategorySiblingCollision {
			collision = true
		}
	}
	out.Part.Categories = partCats
	out.Part.Collision = collision
	out.Path.Categories = pathCats
	if cleanedBase != "" && cleanedBase != basename {
		out.Part.ProposedClean = cleanedBase
	}
	out.Part.Valid = !collision && len(partCats) == 0
	out.Path.Valid = len(pathCats) == 0
	out.Parts = append([]string(nil), cleanedParts...)
	out.Valid = out.Part.Valid && out.Path.Valid
	// Mirror flat fields for older readers.
	out.Collision = collision
	out.Categories = append(append([]string(nil), partCats...), pathCats...)
	out.ProposedClean = out.Part.ProposedClean

	issuesJSON, _ := json.Marshal(mergeIssuesFromActions(l.Log.Issues, l.Log.Actions))
	if collision {
		return out, &db.PathEvent{
			Category:     db.PathEventCategoryGPLClean,
			ProposedPath: firstNonEmpty(out.Part.ProposedClean, cleanedBase, basename),
			Status:       db.PathEventStatusCollision,
			GPLIssues:    string(issuesJSON),
		}
	}
	if out.Part.ProposedClean != "" {
		return out, &db.PathEvent{
			Category:     db.PathEventCategoryGPLClean,
			ProposedPath: out.Part.ProposedClean,
			Status:       db.PathEventStatusPending,
			GPLIssues:    string(issuesJSON),
		}
	}
	return out, nil
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
			UserMessage: act.UserMessage,
			DocsURL:     act.DocsURL,
		})
	}
	return out
}

// mergePathScopeIntoGPLState refreshes only the path scope of an existing gpl_state JSON.
func mergePathScopeIntoGPLState(existing string, pathState db.GPLScopeState, parts []string) string {
	var p db.GPLStatePayload
	if existing != "" {
		_ = json.Unmarshal([]byte(existing), &p)
	}
	if p.Part.Categories == nil && len(p.Categories) > 0 {
		// Legacy flat row: treat flat categories as part-local except we cannot split; keep as part.
		p.Part.Categories = p.Categories
		p.Part.Valid = p.Valid
		p.Part.ProposedClean = p.ProposedClean
		p.Part.Collision = p.Collision
	}
	p.Path = pathState
	if parts != nil {
		p.Parts = append([]string(nil), parts...)
	}
	p.Valid = p.Part.Valid && p.Path.Valid && !p.Part.Collision
	p.Categories = append(append([]string(nil), p.Part.Categories...), p.Path.Categories...)
	p.ProposedClean = p.Part.ProposedClean
	p.Collision = p.Part.Collision
	b, err := json.Marshal(p)
	if err != nil {
		return existing
	}
	return string(b)
}

// evaluateGPLPathOnly revalidates path-scoped rules for composed parts without re-linting segments.
func evaluateGPLPathOnly(target gpl.Target, parts []string, existingGPLState string, isFile bool) (string, []issue.Issue, error) {
	opts := []gpl.Option{
		gpl.WithRelative(true),
		gpl.WithAutoClean(false),
		gpl.WithRaiseErrors(false),
		gpl.WithAutoValidate(false),
	}
	if isFile {
		opts = append(opts, gpl.WithFileAdded(true))
	}
	l, err := gpl.New(target, "", opts...)
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
			for _, cat := range p.Categories {
				// Legacy: skip if already in part
				found := false
				for _, pc := range p.Part.Categories {
					if pc == cat {
						found = true
						break
					}
				}
				if !found && len(p.Part.Categories) == 0 {
					l.Log.AddIssue(issue.Issue{Category: issue.Category(cat), Scope: issue.ScopePart})
				}
			}
		}
	}
	l.SetParts(parts)
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
	merged := mergePathScopeIntoGPLState(existingGPLState, pathState, parts)
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

// parseGPLProposedClean extracts proposed_clean from gpl_state JSON.
func parseGPLProposedClean(gplState string) string {
	if gplState == "" {
		return ""
	}
	var p db.GPLStatePayload
	if err := json.Unmarshal([]byte(gplState), &p); err != nil {
		return ""
	}
	return p.EffectiveProposedClean()
}

func parseGPLParts(gplState string) []string {
	if gplState == "" {
		return nil
	}
	var p db.GPLStatePayload
	if err := json.Unmarshal([]byte(gplState), &p); err != nil {
		return nil
	}
	if len(p.Parts) == 0 {
		return nil
	}
	return append([]string(nil), p.Parts...)
}
