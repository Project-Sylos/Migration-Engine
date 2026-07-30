// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/subtree"
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
}

// ValidatePathProposalResult is a dry-run GPL check for a proposed basename.
type ValidatePathProposalResult struct {
	Valid    bool
	Part     db.GPLScopeState
	Path     db.GPLScopeState
	Parts    []string
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
	conn, err := m.DB.GetDB()
	if err != nil {
		return nil, err
	}
	q := `
SELECT gi.src_id, COALESCE(n.path,''), COALESCE(n.name,''), COALESCE(gi.proposed_name,''), COALESCE(gi.status,''),
       'gpl_clean', COALESCE(gi.issues_json,''), gi.updated_at,
       COALESCE(cur.gpl_status,'')
FROM gpl_issues gi
LEFT JOIN src_nodes n ON n.id = gi.src_id
LEFT JOIN src_current cur ON cur.id = gi.src_id
WHERE gi.status IN ('pending', 'manual_review')
ORDER BY gi.updated_at
LIMIT $1`
	rows, err := conn.QueryContext(context.Background(), q, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PathIssueRow
	for rows.Next() {
		var r PathIssueRow
		var gplStatus string
		if err := rows.Scan(&r.NodeID, &r.Path, &r.Name, &r.ProposedPath, &r.Status, &r.Category, &r.GPLIssues, &r.EventTime, &gplStatus); err != nil {
			return nil, err
		}
		if r.Name == "" {
			r.Name = db.NormalizeNodeBasename(r.Path)
		}
		r.Ignored = gplStatus == db.GPLStatusIgnored
		r.Messages = PathIssueMessagesFromGPLJSON(r.GPLIssues)
		out = append(out, r)
	}
	return out, rows.Err()
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
	parentParts, siblings, isFile, err := m.loadPathChangeContext(nodeID)
	if err != nil {
		return out, err
	}
	payload, issues, err := validateProposedPart(m.dstGPLTarget(), parentParts, proposedPath, siblings, isFile, m.WindowsCompat())
	if err != nil {
		return out, err
	}
	out.Valid = payload.Valid
	out.Part = payload.Part
	out.Path = payload.Path
	out.Parts = payload.Parts
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

// ResetPathRemap clears an accepted destination name override, restores gpl_status=pending
// on the node (and descendants), and re-runs the GPL sweep so warnings can reappear.
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
	err = m.DB.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			now := time.Now().UnixNano()
			if err := w.SetSrcCurrentResolvedName(nodeID, "", now); err != nil {
				return err
			}
			if err := w.DeleteGPLIssue(nodeID); err != nil {
				return err
			}
			if err := subtree.InsertGPLStatusEventsForSubtree(w, "SRC", node.Path, db.GPLStatusPending, true); err != nil {
				return err
			}
			return subtree.InsertGPLStatusEventsForSubtree(w, "DST", node.Path, db.GPLStatusPending, true)
		})
	})
	if err != nil {
		return err
	}
	return m.RunGPLSweep()
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
		if row.Status != db.PathEventStatusPending || row.ProposedPath == "" {
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

func (m *Migration) applyGPLSubtreeEvents(nodeID string, write func(w *db.Writer, side, nodePath string) error) error {
	if m == nil || m.DB == nil {
		return fmt.Errorf("migration db not open")
	}
	if !m.PathChecksEnabled() {
		return nil
	}
	conn, err := m.DB.GetDB()
	if err != nil {
		return err
	}
	var nodePath string
	if err := conn.QueryRowContext(context.Background(),
		`SELECT COALESCE(path,'') FROM src_nodes WHERE id = $1`, nodeID,
	).Scan(&nodePath); err != nil {
		return fmt.Errorf("load node: %w", err)
	}
	return m.DB.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			if err := write(w, "SRC", nodePath); err != nil {
				return err
			}
			return write(w, "DST", nodePath)
		})
	})
}

// SetGPLSubtreeIgnored marks nodeID and descendants ignored or restores prior gpl_status when ignored is false.
func (m *Migration) SetGPLSubtreeIgnored(nodeID string, ignored bool) error {
	if ignored {
		return m.applyGPLSubtreeEvents(nodeID, func(w *db.Writer, side, nodePath string) error {
			return subtree.InsertGPLStatusEventsForSubtree(w, side, nodePath, db.GPLStatusIgnored, true)
		})
	}
	return m.applyGPLSubtreeEvents(nodeID, subtree.InsertGPLRestoredEventsForSubtree)
}

// IgnoreAllPathIssues marks every active (non-ignored) path-issue node's subtree as ignored.
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

func (m *Migration) loadPathChangeContext(nodeID string) (parentParts, siblings []string, isFile bool, err error) {
	conn, err := m.DB.GetDB()
	if err != nil {
		return nil, nil, false, err
	}
	ctx := context.Background()
	var parentID, nodeType string
	err = conn.QueryRowContext(ctx,
		`SELECT COALESCE(parent_id,''), COALESCE(type,'') FROM src_nodes WHERE id = $1`,
		nodeID,
	).Scan(&parentID, &nodeType)
	if err != nil {
		return nil, nil, false, fmt.Errorf("load node: %w", err)
	}
	var parentGPL string
	if parentID != "" {
		_ = conn.QueryRowContext(ctx,
			`SELECT COALESCE(gpl_state,'') FROM src_nodes WHERE id = $1`, parentID,
		).Scan(&parentGPL)
	}
	parentParts = parseGPLPartsJSON(parentGPL)
	siblings, err = m.listSRCSiblingBasenames(parentID, nodeID)
	if err != nil {
		return nil, nil, false, err
	}
	dstSiblings, err := m.listDSTSiblingBasenames(parentID)
	if err != nil {
		return nil, nil, false, err
	}
	siblings = append(siblings, dstSiblings...)
	return parentParts, siblings, nodeType == db.NodeTypeFile, nil
}

func (m *Migration) acceptPathChange(nodeID, proposedPath string, forceSkipValidation bool) error {
	if m == nil || m.DB == nil {
		return fmt.Errorf("migration db not open")
	}
	proposedPath = db.NormalizeNodeBasename(proposedPath)
	conn, err := m.DB.GetDB()
	if err != nil {
		return err
	}
	ctx := context.Background()

	var parentID, nodePath, nodeType, existingGPL string
	var depth int
	err = conn.QueryRowContext(ctx,
		`SELECT COALESCE(parent_id,''), COALESCE(path,''), COALESCE(type,''), COALESCE(gpl_state,''), depth FROM src_nodes WHERE id = $1`,
		nodeID,
	).Scan(&parentID, &nodePath, &nodeType, &existingGPL, &depth)
	if err != nil {
		return fmt.Errorf("load node: %w", err)
	}

	var parentGPL string
	if parentID != "" {
		_ = conn.QueryRowContext(ctx,
			`SELECT COALESCE(gpl_state,'') FROM src_nodes WHERE id = $1`, parentID,
		).Scan(&parentGPL)
	}
	parentParts := parseGPLPartsJSON(parentGPL)

	siblings, err := m.listSRCSiblingBasenames(parentID, nodeID)
	if err != nil {
		return err
	}
	dstSiblings, err := m.listDSTSiblingBasenames(parentID)
	if err != nil {
		return err
	}
	siblings = append(siblings, dstSiblings...)

	target := m.dstGPLTarget()
	isFile := nodeType == db.NodeTypeFile
	payload, issues, err := validateProposedPart(target, parentParts, proposedPath, siblings, isFile, m.WindowsCompat())
	if err != nil {
		return err
	}
	issuesJSON, _ := json.Marshal(issues)
	if !forceSkipValidation && len(issues) > 0 {
		for _, iss := range issues {
			if iss.Category == issue.CategorySiblingCollision {
				_ = m.DB.RunWrite(ctx, func(s *db.WriteSession) error {
					return s.WithTx(func(w *db.Writer) error {
						return w.UpdateGPLIssueWithAction(nodeID, db.GPLIssueStatusManualReview, "", string(issuesJSON), "", time.Now().UnixNano())
					})
				})
				break
			}
		}
		msgs := FriendlyPathIssueMessages(issues)
		msg := "This destination name isn’t allowed."
		if len(msgs) > 0 {
			msg = msgs[0]
		} else if issues[0].Message != "" {
			msg = issues[0].Message
		}
		return &PathValidationError{Message: msg, Issues: issues, Messages: msgs}
	}

	gplJSON, _ := json.Marshal(payload)

	var copyStatus string
	_ = conn.QueryRowContext(ctx, `
SELECT COALESCE(arg_max(copy_status, event_time), '')
FROM src_status_events
WHERE id = $1 AND COALESCE(copy_status,'') <> ''`, nodeID).Scan(&copyStatus)
	if copyStatus == "" {
		_ = conn.QueryRowContext(ctx,
			`SELECT COALESCE(copy_status,'') FROM src_current WHERE id = $1`, nodeID,
		).Scan(&copyStatus)
	}
	dstAction := ""
	if copyStatus == db.CopyStatusAlreadyExisted {
		var dstMapped int
		_ = conn.QueryRowContext(ctx, `
SELECT COUNT(*)::INT FROM id_map
WHERE src_internal_id = $1 AND status = 'active'`, nodeID).Scan(&dstMapped)
		if dstMapped > 0 {
			dstAction = db.DstActionRename
		}
	}

	err = m.DB.RunWrite(ctx, func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			now := time.Now().UnixNano()
			if err := w.UpdateGPLIssueWithAction(nodeID, db.GPLIssueStatusAccepted, proposedPath, string(issuesJSON), dstAction, now); err != nil {
				return err
			}
			if err := w.SetSrcCurrentResolvedName(nodeID, proposedPath, now); err != nil {
				return err
			}
			if err := w.UpdateNodeGPLState(nodeID, string(gplJSON)); err != nil {
				return err
			}
			if err := w.DeleteGPLIssuesForDescendants(nodePath); err != nil {
				return err
			}
			if forceSkipValidation {
				if err := subtree.InsertGPLStatusEventsForSubtree(w, "SRC", nodePath, db.GPLStatusIgnored, true); err != nil {
					return err
				}
				return subtree.InsertGPLStatusEventsForSubtree(w, "DST", nodePath, db.GPLStatusIgnored, true)
			}
			if err := subtree.InsertGPLStatusEventsForSubtree(w, "SRC", nodePath, db.GPLStatusPending, false); err != nil {
				return err
			}
			return subtree.InsertGPLStatusEventsForSubtree(w, "DST", nodePath, db.GPLStatusPending, false)
		})
	})
	if err != nil {
		return err
	}
	if dstAction == db.DstActionRename {
		// Best-effort immediate rename when the DST adapter is available; otherwise
		// dst_action stays set for RunDSTRenameSweep (e.g. Accept-all / pre-copy).
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
	conn, err := m.DB.GetDB()
	if err != nil {
		return nil, err
	}
	rows, err := conn.QueryContext(context.Background(),
		`SELECT COALESCE(NULLIF(name, ''), path) FROM src_nodes WHERE parent_id = $1 AND id <> $2`, parentID, excludeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		out = append(out, db.NormalizeNodeBasename(p))
	}
	return out, rows.Err()
}

func (m *Migration) listDSTSiblingBasenames(srcParentID string) ([]string, error) {
	conn, err := m.DB.GetDB()
	if err != nil {
		return nil, err
	}
	// dst_nodes.path mirrors the SRC display path, so the name column is the only
	// place the actual destination basename (post-GPL rename) is recorded.
	rows, err := conn.QueryContext(context.Background(), `
SELECT COALESCE(NULLIF(d.name, ''), d.path)
FROM id_map im
JOIN dst_nodes d ON d.parent_id = im.dst_internal_id
WHERE im.src_internal_id = $1 AND im.status = 'active'`, srcParentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		out = append(out, db.NormalizeNodeBasename(p))
	}
	return out, rows.Err()
}

func validateProposedPart(target gpl.Target, parentParts []string, proposed string, siblings []string, isFile, windowsCompat bool) (db.GPLStatePayload, []issue.Issue, error) {
	opts := []gpl.Option{
		gpl.WithRelative(true),
		gpl.WithAutoClean(false),
		gpl.WithRaiseErrors(false),
		gpl.WithSiblings(siblings),
		gpl.WithAutoValidate(false),
	}
	if isFile {
		opts = append(opts, gpl.WithFileAdded(true))
	}
	l, err := gpl.NewWithWindowsCompat(target, "", windowsCompat, opts...)
	if err != nil {
		return db.GPLStatePayload{}, nil, err
	}
	if len(parentParts) > 0 {
		l.SetParts(parentParts)
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
		Valid: true,
		Part:  db.GPLScopeState{Valid: true},
		Path:  db.GPLScopeState{Valid: true},
		Parts: l.Parts(),
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
	// Sibling collision is Clean-time in GPL; accept uses exact basename match against peers.
	for _, sib := range siblings {
		if sib != "" && sib == proposed {
			payload.Part.Collision = true
			partCats = append(partCats, string(issue.CategorySiblingCollision))
			l.Log.AddIssue(issue.Issue{
				Category: issue.CategorySiblingCollision,
				Part:     proposed,
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
	payload.Categories = append(append([]string(nil), partCats...), pathCats...)
	payload.ProposedClean = proposed
	payload.Collision = payload.Part.Collision
	return payload, append([]issue.Issue(nil), l.Log.Issues...), nil
}

func parseGPLPartsJSON(gplState string) []string {
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
