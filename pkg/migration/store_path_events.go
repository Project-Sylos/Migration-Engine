// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"encoding/json"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	queuegpl "codeberg.org/Sylos/Migration-Engine/pkg/queue/gpl"
	"codeberg.org/Sylos/go-path-linter/pkg/check"
	"codeberg.org/Sylos/go-path-linter/pkg/gpl"
	"codeberg.org/Sylos/go-path-linter/pkg/issue"
)

// PathIssueMessage is one user-facing finding attached to a path issue row.
type PathIssueMessage struct {
	Category string
	Message  string
	// Detail is a GPL structured attribute (e.g. InvalidChar: the forbidden runes found).
	Detail  string
	DocsURL string
}

// PathIssueRow is one current sparse GPL finding for review.
type PathIssueRow struct {
	NodeID       string
	Path         string
	Name         string
	ProposedPath string
	Status       string
	Category     string
	GPLIssues    string
	EventTime    int64
	Messages     []PathIssueMessage
	// Ignored is true when the node's current gpl_status is ignored (warning dismissed).
	Ignored bool
	// DstAction is set on accepted rows (rename); empty for open issues.
	DstAction string
	// CollisionSibling is the colliding DST basename from IssuesJSON Detail when present.
	CollisionSibling string
	// SuggestedUniqueName is a free sibling basename (base (1), …) when a collision is open.
	SuggestedUniqueName string
}

// ValidatePathProposalResult is a dry-run GPL check for a proposed basename.
type ValidatePathProposalResult struct {
	Valid    bool
	Part     db.GPLScopeState
	Path     db.GPLScopeState
	Issues   []issue.Issue
	Messages []string // short end-user sentences (never mentions GPL)
}

// PathValidationError is returned when remap/accept fails GPL without force.
type PathValidationError struct {
	Message  string
	Issues   []issue.Issue
	Messages []string
}

func (e *PathValidationError) Error() string {
	if e == nil {
		return "path validation failed"
	}
	if e.Message != "" {
		return e.Message
	}
	if len(e.Issues) > 0 {
		return e.Issues[0].Message
	}
	return "path validation failed"
}

// ListPathIssues returns sparse GPL rows whose status is pending or manual_review.
// Rows with gpl_status=ignored are included with Ignored=true
// so the UI can offer unignore. Returns an empty list when path checks are disabled.
func (m *Migration) ListPathIssues(limit int) ([]PathIssueRow, error) {
	if m == nil || m.DB == nil {
		return nil, fmt.Errorf("migration db not open")
	}
	if !m.PathChecksEnabled() {
		return nil, nil
	}
	if limit <= 0 {
		limit = 1000
	}
	pending, err := m.DB.Ops().ListGPLByStatus(db.GPLIssueStatusPending, limit)
	if err != nil {
		return nil, err
	}
	manual, err := m.DB.Ops().ListGPLByStatus(db.GPLIssueStatusManualReview, limit)
	if err != nil {
		return nil, err
	}
	ids := append(pending, manual...)
	if len(ids) == 0 {
		return nil, nil
	}
	recs, err := m.DB.Ops().BatchGetGPL(ids)
	if err != nil {
		return nil, err
	}
	nodes, err := m.DB.Ops().BatchGetNode("src", ids)
	if err != nil {
		return nil, err
	}
	stMap, err := m.DB.Ops().BatchGetStatus("src", ids)
	if err != nil {
		return nil, err
	}
	out := make([]PathIssueRow, 0, len(recs))
	for _, id := range ids {
		rec, ok := recs[id]
		if !ok {
			continue
		}
		n := nodes[id]
		r := PathIssueRow{
			NodeID:           id,
			Path:             n.Path,
			Name:             n.Name,
			ProposedPath:     rec.ProposedName,
			Status:           rec.Status,
			Category:         "gpl_clean",
			GPLIssues:        rec.IssuesJSON,
			EventTime:        rec.UpdatedAt,
			Ignored:          stMap[id].GPLStatus == db.GPLStatusIgnored,
			DstAction:        rec.DstAction,
			CollisionSibling: PathCollisionSibling(rec.IssuesJSON),
		}
		if r.Name == "" {
			r.Name = db.NormalizeNodeBasename(r.Path)
		}
		if display, err := db.ComposeDisplayPath(m.DB, "SRC", r.NodeID); err == nil && display != "" {
			r.Path = display
		}
		r.Messages = PathIssueMessagesFromGPLJSON(r.GPLIssues)
		if r.CollisionSibling != "" || pathIssueHasSiblingCollision(r.Messages) {
			if name, err := m.PreviewUniquePathName(id); err == nil && name != "" {
				r.SuggestedUniqueName = name
			}
		}
		out = append(out, r)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func pathIssueHasSiblingCollision(msgs []PathIssueMessage) bool {
	for _, m := range msgs {
		if m.Category == string(issue.CategorySiblingCollision) {
			return true
		}
	}
	return false
}

// ActivePathIssues returns the subset of ListPathIssues that are not ignored.
func ActivePathIssues(rows []PathIssueRow) []PathIssueRow {
	out := make([]PathIssueRow, 0, len(rows))
	for _, r := range rows {
		if !r.Ignored {
			out = append(out, r)
		}
	}
	return out
}

// ValidatePathProposal dry-runs parent+AddPart(proposed) without writing.
func (m *Migration) ValidatePathProposal(nodeID, proposedPath string) (ValidatePathProposalResult, error) {
	var out ValidatePathProposalResult
	if m == nil || m.DB == nil {
		return out, fmt.Errorf("migration db not open")
	}
	if !m.PathChecksEnabled() {
		out.Valid = true
		out.Messages = []string{PathChecksNotApplicableMessage}
		return out, nil
	}
	proposedPath = db.NormalizeNodeBasename(proposedPath)
	parentPathLen, siblings, isFile, err := m.loadPathChangeContext(nodeID)
	if err != nil {
		return out, err
	}
	payload, issues, err := validateProposedPart(m.dstGPLTarget(), parentPathLen, proposedPath, siblings, isFile, m.WindowsCompat())
	if err != nil {
		return out, err
	}
	out.Valid = payload.Valid
	out.Part = payload.Part
	out.Path = payload.Path
	out.Issues = issues
	out.Messages = FriendlyPathIssueMessages(issues)
	return out, nil
}

func (m *Migration) AcceptPathChange(nodeID, proposedPath string, forceSkipValidation bool) error {
	if !m.PathChecksEnabled() {
		return fmt.Errorf("%s", PathChecksNotApplicableMessage)
	}
	return m.acceptPathChange(nodeID, proposedPath, forceSkipValidation)
}

// ResetPathRemap undoes an accepted destination rename / unique name.
// Reversible while copy is still pending or already_existed. If the destination
// was already FS-renamed, it is renamed back to the source basename first.
// Source is never mutated.
func (m *Migration) ResetPathRemap(nodeID string) error {
	if m == nil || m.DB == nil {
		return fmt.Errorf("migration db not open")
	}
	if !m.PathChecksEnabled() {
		return nil
	}
	node, err := pull.GetNodeByID(m.DB, "SRC", nodeID)
	if err != nil {
		return err
	}
	if node == nil {
		return fmt.Errorf("node %s not found", nodeID)
	}
	gplRec, ok, err := m.DB.Ops().GetGPL(nodeID)
	if err != nil {
		return err
	}
	if !ok || gplRec.Status != db.GPLIssueStatusAccepted {
		return fmt.Errorf("node does not have an accepted destination rename")
	}
	if gplRec.DstAction != "" && gplRec.DstAction != db.DstActionRename {
		return fmt.Errorf("cannot reset dst_action %q: folder merge is deferred and unavailable", gplRec.DstAction)
	}
	copySt := node.CopyStatus
	if copySt == "" {
		copySt = db.CopyStatusPending
	}
	switch copySt {
	case db.CopyStatusPending, db.CopyStatusAlreadyExisted:
		// Reversible while copy has not successfully written this node.
	default:
		return fmt.Errorf("cannot reset rename: copy status %q", copySt)
	}
	if err := m.revertAcceptedDSTRename(nodeID, node, gplRec.ProposedName); err != nil {
		return err
	}
	if err := m.DB.DeleteGPLIssue(nodeID); err != nil {
		return err
	}
	if err := m.DB.SetSubtreeGPLStatus("src", node.Path, db.GPLStatusPending, true); err != nil {
		return err
	}
	if err := m.DB.SetSubtreeGPLStatus("dst", node.Path, db.GPLStatusPending, true); err != nil {
		return err
	}
	return m.RunGPLSweep(node.Path)
}

// AcceptAllPathProposals accepts every active pending proposal's proposed_path.
// Caller should RunGPLSweep once afterward. Collision rows are skipped (need manual remap).
func (m *Migration) AcceptAllPathProposals() (int, error) {
	if !m.PathChecksEnabled() {
		return 0, nil
	}
	issues, err := m.ListPathIssues(0)
	if err != nil {
		return 0, err
	}
	accepted := 0
	for _, row := range ActivePathIssues(issues) {
		if row.Status != db.GPLIssueStatusPending || row.ProposedPath == "" {
			continue
		}
		if err := m.AcceptPathChange(row.NodeID, row.ProposedPath, false); err != nil {
			if _, needsManual := err.(*PathValidationError); needsManual {
				continue
			}
			return accepted, fmt.Errorf("accept %s: %w", row.NodeID, err)
		}
		accepted++
	}
	if err := m.RunDSTRenameSweep(); err != nil {
		return accepted, fmt.Errorf("destination renames: %w", err)
	}
	return accepted, nil
}

func (m *Migration) applyGPLSubtreeStatus(nodeID, gplStatus string) error {
	if m == nil || m.DB == nil {
		return fmt.Errorf("migration db not open")
	}
	if !m.PathChecksEnabled() {
		return nil
	}
	node, err := pull.GetNodeByID(m.DB, "SRC", nodeID)
	if err != nil {
		return err
	}
	if node == nil {
		return fmt.Errorf("load node: %s not found", nodeID)
	}
	if err := m.DB.SetSubtreeGPLStatus("src", node.Path, gplStatus, true); err != nil {
		return err
	}
	return m.DB.SetSubtreeGPLStatus("dst", node.Path, gplStatus, true)
}

// SetGPLSubtreeIgnored marks nodeID and descendants ignored or restores prior gpl_status when ignored is false.
// Ignoring a manual_review issue is refused (must rename / merge / unique).
func (m *Migration) SetGPLSubtreeIgnored(nodeID string, ignored bool) error {
	if ignored {
		if err := m.ensurePathIssueIgnorable(nodeID); err != nil {
			return err
		}
	}
	status := db.GPLStatusPending
	if ignored {
		status = db.GPLStatusIgnored
	}
	return m.applyGPLSubtreeStatus(nodeID, status)
}

func (m *Migration) ensurePathIssueIgnorable(nodeID string) error {
	if m == nil || m.DB == nil || nodeID == "" {
		return fmt.Errorf("migration db not open")
	}
	rec, ok, err := m.DB.Ops().GetGPL(nodeID)
	if err != nil {
		return err
	}
	if ok && rec.Status == db.GPLIssueStatusManualReview {
		return fmt.Errorf("this naming issue requires a fix and cannot be dismissed")
	}
	return nil
}

// IgnoreAllPathIssues marks every active soft (non-manual_review) path-issue node's subtree as ignored.
func (m *Migration) IgnoreAllPathIssues() (int, error) {
	if !m.PathChecksEnabled() {
		return 0, nil
	}
	issues, err := m.ListPathIssues(0)
	if err != nil {
		return 0, err
	}
	seen := make(map[string]struct{}, len(issues))
	n := 0
	for _, row := range ActivePathIssues(issues) {
		if row.Status == db.GPLIssueStatusManualReview {
			continue
		}
		if _, ok := seen[row.NodeID]; ok {
			continue
		}
		seen[row.NodeID] = struct{}{}
		if err := m.SetGPLSubtreeIgnored(row.NodeID, true); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

func (m *Migration) loadPathChangeContext(nodeID string) (parentPathLen int, siblings []string, isFile bool, err error) {
	node, err := pull.GetNodeByID(m.DB, "SRC", nodeID)
	if err != nil {
		return 0, nil, false, err
	}
	if node == nil {
		return 0, nil, false, fmt.Errorf("load node: %s not found", nodeID)
	}
	parentPathLen = queuegpl.LoadParentPathLen(m.DB, node.ParentID, "")
	siblings, err = m.listSRCSiblingBasenames(node.ParentID, nodeID)
	if err != nil {
		return 0, nil, false, err
	}
	dstSiblings, err := m.listDSTSiblingBasenames(node.ParentID)
	if err != nil {
		return 0, nil, false, err
	}
	siblings = append(siblings, dstSiblings...)
	return parentPathLen, siblings, node.Type == db.NodeTypeFile, nil
}

func (m *Migration) acceptPathChange(nodeID, proposedPath string, forceSkipValidation bool) error {
	if m == nil || m.DB == nil {
		return fmt.Errorf("migration db not open")
	}
	proposedPath = db.NormalizeNodeBasename(proposedPath)
	node, err := pull.GetNodeByID(m.DB, "SRC", nodeID)
	if err != nil {
		return err
	}
	if node == nil {
		return fmt.Errorf("load node: %s not found", nodeID)
	}
	copySt := node.CopyStatus
	if copySt == "" {
		copySt = db.CopyStatusPending
	}
	var alreadyMapped bool
	switch copySt {
	case db.CopyStatusPending:
		// Planned create: store destination basename in GPL only (source stays untouched).
	case db.CopyStatusAlreadyExisted:
		mapped, ok, mapErr := m.DB.Ops().GetMapBySrc(nodeID)
		if mapErr != nil {
			return mapErr
		}
		if !ok || mapped.Status != db.IDMapStatusActive {
			return fmt.Errorf("cannot rename: destination item is not mapped")
		}
		alreadyMapped = true
	default:
		return fmt.Errorf("cannot rename: copy status %q (only pending or already-existed)", copySt)
	}
	parentPathLen := queuegpl.LoadParentPathLen(m.DB, node.ParentID, "")
	siblings, err := m.listSRCSiblingBasenames(node.ParentID, nodeID)
	if err != nil {
		return err
	}
	dstSiblings, err := m.listDSTSiblingBasenames(node.ParentID)
	if err != nil {
		return err
	}
	siblings = append(siblings, dstSiblings...)

	target := m.dstGPLTarget()
	isFile := node.Type == db.NodeTypeFile
	payload, issues, err := validateProposedPart(target, parentPathLen, proposedPath, siblings, isFile, m.WindowsCompat())
	if err != nil {
		return err
	}
	issuesJSON, _ := json.Marshal(issues)
	if !forceSkipValidation && len(issues) > 0 {
		for _, iss := range issues {
			if iss.Category == issue.CategorySiblingCollision {
				_ = m.DB.PutGPLIssue(nodeID, db.GPLIssueStatusManualReview, "", string(issuesJSON), "", time.Now().UnixNano())
				break
			}
		}
		msgs := FriendlyPathIssueMessages(issues)
		msg := "This name isn’t allowed."
		if len(msgs) > 0 {
			msg = msgs[0]
		} else if issues[0].Message != "" {
			msg = issues[0].Message
		}
		return &PathValidationError{Message: msg, Issues: issues, Messages: msgs}
	}

	gplJSON, _ := json.Marshal(payload)
	dstAction := ""
	if alreadyMapped {
		dstAction = db.DstActionRename
	}
	now := time.Now().UnixNano()
	if err := m.DB.PutGPLIssue(nodeID, db.GPLIssueStatusAccepted, proposedPath, string(issuesJSON), dstAction, now); err != nil {
		return err
	}
	if err := m.DB.UpdateNodeGPLState(nodeID, string(gplJSON)); err != nil {
		return err
	}
	if err := m.DB.DeleteGPLIssuesUnderPath(node.Path, false); err != nil {
		return err
	}
	gplStatus := db.GPLStatusPending
	includeRoot := false
	if forceSkipValidation {
		gplStatus = db.GPLStatusIgnored
		includeRoot = true
	}
	if err := m.DB.SetSubtreeGPLStatus("src", node.Path, gplStatus, includeRoot); err != nil {
		return err
	}
	if err := m.DB.SetSubtreeGPLStatus("dst", node.Path, gplStatus, includeRoot); err != nil {
		return err
	}
	// Destination basename is bookkeeping only (GPL ProposedName / ResolvedDstName).
	// Never rename or rewrite the source FS or SRC node name/DisplayPath.
	if dstAction == db.DstActionRename {
		// Already-mapped destination: rename the existing DST item when the adapter is live.
		// Otherwise dst_action stays set for RunDSTRenameSweep (e.g. Accept-all / pre-copy).
		if m.destinationAdapter() != nil {
			if renameErr := m.ApplyDSTRename(nodeID); renameErr != nil {
				return fmt.Errorf("accepted proposal but destination rename failed: %w", renameErr)
			}
		}
	}
	return nil
}

// PathChecksEnabled reports whether destination-name checks apply for this migration.
func (m *Migration) PathChecksEnabled() bool {
	if m != nil && m.DB != nil && db.GPLDisabled {
		return false
	}
	if m == nil {
		return true
	}
	m.mu.RLock()
	src, dst, profile := m.pathCheckSrcProvider, m.pathCheckDstProvider, m.pathCheckProfile
	cfg := m.lastRunConfig
	m.mu.RUnlock()
	if src != "" || dst != "" || profile != "" {
		return queuegpl.PathChecksRequired(src, dst, profile)
	}
	if cfg != nil {
		return queuegpl.PathChecksRequired(cfg.Source.ProviderID, cfg.Destination.ProviderID, cfg.PathCheckTarget)
	}
	return true
}

// PathCheckProfile returns the configured path-check profile ("none", "auto", or provider id).
func (m *Migration) PathCheckProfile() string {
	if m == nil {
		return "auto"
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.pathCheckProfile != "" {
		return m.pathCheckProfile
	}
	if m.lastRunConfig != nil && m.lastRunConfig.PathCheckTarget != "" {
		return m.lastRunConfig.PathCheckTarget
	}
	return "auto"
}

// PathChecksNotApplicableMessage is the user-facing reason when checks are skipped.
const PathChecksNotApplicableMessage = "Destination name checks aren’t needed when source and destination are the same type of service."

func (m *Migration) dstGPLTarget() gpl.Target {
	provider := ""
	if m != nil {
		m.mu.RLock()
		src, dst, profile := m.pathCheckSrcProvider, m.pathCheckDstProvider, m.pathCheckProfile
		cfg := m.lastRunConfig
		m.mu.RUnlock()
		if src != "" || dst != "" || profile != "" {
			provider = queuegpl.ResolvePathCheckTarget(src, dst, profile)
		} else if cfg != nil {
			provider = queuegpl.ResolvePathCheckTarget(cfg.Source.ProviderID, cfg.Destination.ProviderID, cfg.PathCheckTarget)
		}
	}
	return queuegpl.GPLTargetFromProvider(provider)
}

func (m *Migration) listSRCSiblingBasenames(parentID, excludeID string) ([]string, error) {
	if parentID == "" {
		return nil, nil
	}
	ids, err := m.DB.Ops().ListChildren("src", parentID, "", 10000)
	if err != nil {
		return nil, err
	}
	nodes, err := m.DB.Ops().BatchGetNode("src", ids)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == excludeID {
			continue
		}
		n := nodes[id]
		base := n.Name
		if base == "" {
			base = n.Path
		}
		out = append(out, db.NormalizeNodeBasename(base))
	}
	return out, nil
}

func (m *Migration) listDSTSiblingBasenames(srcParentID string) ([]string, error) {
	if srcParentID == "" {
		return nil, nil
	}
	mapped, ok, err := m.DB.Ops().GetMapBySrc(srcParentID)
	if err != nil || !ok || mapped.DstID == "" {
		return nil, err
	}
	ids, err := m.DB.Ops().ListChildren("dst", mapped.DstID, "", 10000)
	if err != nil {
		return nil, err
	}
	nodes, err := m.DB.Ops().BatchGetNode("dst", ids)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, db.NormalizeNodeBasename(nodes[id].Name))
	}
	return out, nil
}

func validateProposedPart(target gpl.Target, parentPathLen int, proposed string, siblings []string, isFile, windowsCompat bool) (db.GPLStatePayload, []issue.Issue, error) {
	opts := []gpl.Option{
		gpl.WithRelative(true),
		gpl.WithAutoClean(false),
		gpl.WithRaiseErrors(false),
		gpl.WithSiblings(siblings),
		gpl.WithAutoValidate(false),
		gpl.WithParentPathLen(parentPathLen),
	}
	if isFile {
		opts = append(opts, gpl.WithFileAdded(true))
	}
	l, err := gpl.NewWithWindowsCompat(target, "", windowsCompat, opts...)
	if err != nil {
		return db.GPLStatePayload{}, nil, err
	}
	var partOpts []gpl.PartOption
	if isFile {
		partOpts = append(partOpts, gpl.AsFile())
	}
	if err := l.AddPart(proposed, partOpts...); err != nil {
		return db.GPLStatePayload{}, nil, err
	}
	_ = l.Validate()

	payload := db.GPLStatePayload{
		Valid:   true,
		Part:    db.GPLScopeState{Valid: true},
		Path:    db.GPLScopeState{Valid: true},
		PathLen: l.PathLength(),
	}
	partCats := make([]string, 0)
	pathCats := make([]string, 0)
	for _, iss := range l.Log.Issues {
		if iss.Scope == issue.ScopePath {
			pathCats = append(pathCats, string(iss.Category))
			continue
		}
		partCats = append(partCats, string(iss.Category))
		if iss.Category == issue.CategorySiblingCollision {
			payload.Part.Collision = true
		}
	}
	// Sibling collision is Clean-time in GPL; accept also checks basename against peers
	// with the same Windows-aware equality as go-path-linter.
	fold := gpl.CaseInsensitiveSiblingTarget(target) || windowsCompat
	for _, sib := range siblings {
		if sib != "" && gpl.SiblingNamesEqual(sib, proposed, fold) {
			payload.Part.Collision = true
			partCats = append(partCats, string(issue.CategorySiblingCollision))
			l.Log.AddIssue(issue.Issue{
				Category: issue.CategorySiblingCollision,
				Part:     proposed,
				Detail:   sib,
				Message:  "proposed name collides with sibling",
			})
			break
		}
	}
	payload.Part.Categories = partCats
	payload.Path.Categories = pathCats
	payload.Part.ProposedClean = proposed
	payload.Part.Valid = len(partCats) == 0 && !payload.Part.Collision
	payload.Path.Valid = len(pathCats) == 0
	payload.Valid = payload.Part.Valid && payload.Path.Valid
	return payload, append([]issue.Issue(nil), l.Log.Issues...), nil
}

// FriendlyPathIssueMessages maps engine findings to short end-user sentences (no GPL jargon).
func FriendlyPathIssueMessages(issues []issue.Issue) []string {
	if len(issues) == 0 {
		return nil
	}
	out := make([]string, 0, len(issues))
	seen := make(map[string]struct{}, len(issues))
	for _, iss := range issues {
		msg := FriendlyPathIssueMessage(iss)
		if msg == "" {
			continue
		}
		if _, ok := seen[msg]; ok {
			continue
		}
		seen[msg] = struct{}{}
		out = append(out, msg)
	}
	return out
}

// FriendlyPathIssueMessage maps one engine finding to a short end-user sentence.
func FriendlyPathIssueMessage(iss issue.Issue) string {
	if iss.UserMessage != "" {
		return iss.UserMessage
	}
	switch iss.Category {
	case issue.CategoryInvalidChar:
		chars := check.FormatInvalidChars(iss.Detail)
		if chars != "" {
			return "Contains characters the destination doesn’t allow: " + chars
		}
		return "Contains characters the destination doesn’t allow."
	case issue.CategoryControlChar:
		return "Contains characters the destination doesn’t allow."
	case issue.CategoryReservedName:
		return "This name is reserved on the destination."
	case issue.CategoryLength:
		if iss.Scope == issue.ScopePath {
			return "This path is too long for the destination."
		}
		return "This name is too long for the destination."
	case issue.CategoryTrailingDot:
		return "Names can’t end with a period on the destination."
	case issue.CategoryTrailingSpace:
		return "Names can’t end with a space on the destination."
	case issue.CategoryEmptyPart:
		return "This name needs a manual rename; it can’t be left empty or removed."
	case issue.CategoryAbsolute:
		return "Enter a name, not a full path."
	case issue.CategorySiblingCollision:
		return "Name already used in this folder."
	default:
		if iss.Message == "" {
			return "This destination name isn’t allowed."
		}
		return iss.Message
	}
}

// PathIssueMessagesFromGPLJSON parses gpl_issues.issues_json into user-facing messages.
func PathIssueMessagesFromGPLJSON(raw string) []PathIssueMessage {
	if raw == "" {
		return nil
	}
	var issues []issue.Issue
	if err := json.Unmarshal([]byte(raw), &issues); err != nil || len(issues) == 0 {
		return nil
	}
	out := make([]PathIssueMessage, 0, len(issues))
	seen := make(map[string]struct{}, len(issues))
	for _, iss := range issues {
		msg := FriendlyPathIssueMessage(iss)
		if msg == "" {
			continue
		}
		key := msg + "\x00" + iss.DocsURL + "\x00" + iss.Detail
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, PathIssueMessage{
			Category: string(iss.Category),
			Message:  msg,
			Detail:   iss.Detail,
			DocsURL:  iss.DocsURL,
		})
	}
	return out
}
