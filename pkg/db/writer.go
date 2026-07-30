// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// Writer is the write handle for DuckDB. Used inside RunWrite via WriteSession.WithTx.
type Writer struct {
	tx *sql.Tx
}

// NewWriter wraps an open transaction for Writer methods. Used by pkg/db/seal flushes.
func NewWriter(tx *sql.Tx) *Writer {
	return &Writer{tx: tx}
}

// Tx exposes the writer's open transaction so sibling packages (seal, subtree) can issue
// statements inside the same tx. Valid only for the lifetime of the WithTx callback.
func (w *Writer) Tx() *sql.Tx {
	return w.tx
}

// NodeStateAppendRowArgsForTable returns appender values matching the physical column layout of table.
func NodeStateAppendRowArgsForTable(table string, n *NodeState) []any {
	path, parentPath := NodeInsertPathFields(n.Path, n.ParentPath, n.Depth)
	name := NodeInsertName(n.Name, n.Path)
	args := []any{
		n.ID, n.ServiceID, n.ParentID, n.ParentServiceID, path, parentPath,
		NormalizeQueueNodeType(n.Type), n.Size, n.MTime, int32(n.Depth),
	}
	if table == TableSrcNodes {
		// xfer_offset, xfer_src_size, xfer_src_mtime, xfer_dst_ref — null on insert; ME updates later.
		args = append(args, nil, nil, nil, nil, n.GPLState, name)
	} else {
		args = append(args, name)
	}
	return args
}

// AppenderInsert inserts node metadata into src_nodes or dst_nodes (batch INSERT). No status columns.
// Explicit column lists omit xfer_* so INSERT stays valid for both tables.
func (w *Writer) AppenderInsert(table string, nodes []*NodeState) error {
	if len(nodes) == 0 {
		return nil
	}
	ctx := context.Background()
	for _, n := range nodes {
		path, parentPath := NodeInsertPathFields(n.Path, n.ParentPath, n.Depth)
		name := NodeInsertName(n.Name, n.Path)
		if table == TableSrcNodes {
			_, err := w.tx.ExecContext(ctx,
				`INSERT INTO `+table+` (id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, gpl_state, name)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
				n.ID, n.ServiceID, n.ParentID, n.ParentServiceID, path, parentPath, NormalizeQueueNodeType(n.Type), n.Size, n.MTime, n.Depth, n.GPLState, name,
			)
			if err != nil {
				return err
			}
			continue
		}
		_, err := w.tx.ExecContext(ctx,
			`INSERT INTO `+table+` (id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, name)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
			n.ID, n.ServiceID, n.ParentID, n.ParentServiceID, path, parentPath, NormalizeQueueNodeType(n.Type), n.Size, n.MTime, n.Depth, name,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// UpsertNodes inserts node metadata into src_nodes or dst_nodes. On conflict (id) does nothing so
// duplicate inserts (e.g. retry re-discovery before DST children are deleted) are safe.
func (w *Writer) UpsertNodes(table string, nodes []*NodeState) error {
	if len(nodes) == 0 {
		return nil
	}
	ctx := context.Background()
	for _, n := range nodes {
		path, parentPath := NodeInsertPathFields(n.Path, n.ParentPath, n.Depth)
		name := NodeInsertName(n.Name, n.Path)
		var err error
		if table == TableSrcNodes {
			_, err = w.tx.ExecContext(ctx,
				`INSERT INTO `+table+` (id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, gpl_state, name)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
				 ON CONFLICT (id) DO NOTHING`,
				n.ID, n.ServiceID, n.ParentID, n.ParentServiceID, path, parentPath, NormalizeQueueNodeType(n.Type), n.Size, n.MTime, n.Depth, n.GPLState, name,
			)
		} else {
			_, err = w.tx.ExecContext(ctx,
				`INSERT INTO `+table+` (id, service_id, parent_id, parent_service_id, path, parent_path, type, size, mtime, depth, name)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
				 ON CONFLICT (id) DO NOTHING`,
				n.ID, n.ServiceID, n.ParentID, n.ParentServiceID, path, parentPath, NormalizeQueueNodeType(n.Type), n.Size, n.MTime, n.Depth, name,
			)
		}
		if err != nil {
			return fmt.Errorf("insert node %s into %s: %w", n.ID, table, err)
		}
	}
	return nil
}

// SrcStatusEventAppendRowArgs returns column values for one row in src_status_events for appender.
func SrcStatusEventAppendRowArgs(e *StatusEvent) []any {
	return []any{e.ID, e.TraversalStatus, e.CopyStatus, e.DeleteStatus, e.GPLStatus, e.EventTime, int32(e.Depth), e.ErrorLogID}
}

// DstStatusEventAppendRowArgs returns column values for one row in dst_status_events for appender.
func DstStatusEventAppendRowArgs(e *StatusEvent) []any {
	return []any{e.ID, e.TraversalStatus, e.GPLStatus, e.EventTime, int32(e.Depth), e.ErrorLogID}
}

// BatchInsertSrcStatusEvents inserts status events into src_status_events inside the current transaction. Used by seal flush so events are atomic with nodes/stats.
func (w *Writer) BatchInsertSrcStatusEvents(events []StatusEvent) error {
	if len(events) == 0 {
		return nil
	}
	ctx := context.Background()
	for _, e := range events {
		_, err := w.tx.ExecContext(ctx,
			`INSERT INTO `+TableSrcStatusEvents+` (id, traversal_status, copy_status, delete_status, gpl_status, event_time, depth, error_log_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			e.ID, e.TraversalStatus, e.CopyStatus, nullIfEmpty(e.DeleteStatus), nullIfEmpty(e.GPLStatus), e.EventTime, e.Depth, nullIfEmpty(e.ErrorLogID),
		)
		if err != nil {
			return fmt.Errorf("insert src_status_event %s: %w", e.ID, err)
		}
	}
	return nil
}

// BatchInsertDstStatusEvents inserts status events into dst_status_events inside the current transaction. Used by seal flush so events are atomic with nodes/stats.
func (w *Writer) BatchInsertDstStatusEvents(events []StatusEvent) error {
	if len(events) == 0 {
		return nil
	}
	ctx := context.Background()
	for _, e := range events {
		_, err := w.tx.ExecContext(ctx,
			`INSERT INTO `+TableDstStatusEvents+` (id, traversal_status, gpl_status, event_time, depth, error_log_id) VALUES ($1, $2, $3, $4, $5, $6)`,
			e.ID, e.TraversalStatus, nullIfEmpty(e.GPLStatus), e.EventTime, e.Depth, nullIfEmpty(e.ErrorLogID),
		)
		if err != nil {
			return fmt.Errorf("insert dst_status_event %s: %w", e.ID, err)
		}
	}
	return nil
}

// InsertStatusEvent appends a status event and refreshes the current-status cache for that node.
func (w *Writer) InsertStatusEvent(table string, e *StatusEvent) error {
	ctx := context.Background()
	if table == "DST" {
		_, err := w.tx.ExecContext(ctx,
			`INSERT INTO `+TableDstStatusEvents+` (id, traversal_status, gpl_status, event_time, depth, error_log_id) VALUES ($1, $2, $3, $4, $5, $6)`,
			e.ID, e.TraversalStatus, nullIfEmpty(e.GPLStatus), e.EventTime, e.Depth, nullIfEmpty(e.ErrorLogID),
		)
		if err != nil {
			return err
		}
		return w.RefreshCurrentByIDs("DST", []string{e.ID})
	}
	_, err := w.tx.ExecContext(ctx,
		`INSERT INTO `+TableSrcStatusEvents+` (id, traversal_status, copy_status, delete_status, gpl_status, event_time, depth, error_log_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		e.ID, e.TraversalStatus, e.CopyStatus, nullIfEmpty(e.DeleteStatus), nullIfEmpty(e.GPLStatus), e.EventTime, e.Depth, nullIfEmpty(e.ErrorLogID),
	)
	if err != nil {
		return err
	}
	return w.RefreshCurrentByIDs("SRC", []string{e.ID})
}

// UpdateNodeGPLState updates src_nodes.gpl_state for a single node.
func (w *Writer) UpdateNodeGPLState(nodeID, gplState string) error {
	_, err := w.tx.ExecContext(context.Background(),
		`UPDATE `+TableSrcNodes+` SET gpl_state = $1 WHERE id = $2`, gplState, nodeID)
	return err
}

// BatchInsertGPLIssues inserts sparse GPL rows. It is intended for fixtures and
// synchronous review mutations that must land in the same transaction as other writes.
func (w *Writer) BatchInsertGPLIssues(issues []GPLIssue) error {
	ctx := context.Background()
	for _, issue := range issues {
		_, err := w.tx.ExecContext(ctx,
			`INSERT INTO `+TableGPLIssues+` (src_id, status, proposed_name, issues_json, updated_at, dst_action) VALUES ($1, $2, $3, $4, $5, $6)`,
			issue.SrcID, issue.Status, nullIfEmpty(issue.ProposedName), nullIfEmpty(issue.IssuesJSON), issue.UpdatedAt, nullIfEmpty(issue.DstAction),
		)
		if err != nil {
			return fmt.Errorf("insert gpl_issue %s: %w", issue.SrcID, err)
		}
	}
	return nil
}

func (w *Writer) UpdateGPLIssueWithAction(srcID, status, proposedName, issuesJSON, dstAction string, updatedAt int64) error {
	ctx := context.Background()
	_, err := w.tx.ExecContext(ctx,
		`INSERT INTO `+TableGPLIssues+` (src_id, status, proposed_name, issues_json, updated_at, dst_action) VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (src_id) DO UPDATE SET status = excluded.status, proposed_name = excluded.proposed_name, issues_json = excluded.issues_json, updated_at = excluded.updated_at, dst_action = excluded.dst_action`,
		srcID, status, nullIfEmpty(proposedName), nullIfEmpty(issuesJSON), updatedAt, nullIfEmpty(dstAction),
	)
	return err
}

func (w *Writer) ClearGPLIssueDstAction(srcID string) error {
	_, err := w.tx.ExecContext(context.Background(),
		`UPDATE `+TableGPLIssues+` SET dst_action = NULL WHERE src_id = $1`, srcID)
	return err
}

func (w *Writer) UpdateDstNodeName(dstID, name string) error {
	_, err := w.tx.ExecContext(context.Background(),
		`UPDATE `+TableDstNodes+` SET name = $1 WHERE id = $2`, name, dstID)
	return err
}

func (w *Writer) UpdateDstNodeServiceID(dstID, serviceID string) error {
	_, err := w.tx.ExecContext(context.Background(),
		`UPDATE `+TableDstNodes+` SET service_id = $1 WHERE id = $2`, serviceID, dstID)
	return err
}

func (w *Writer) DeleteGPLIssue(srcID string) error {
	_, err := w.tx.ExecContext(context.Background(),
		`DELETE FROM `+TableGPLIssues+` WHERE src_id = $1`, srcID)
	return err
}

func (w *Writer) DeleteGPLIssuesForDescendants(rootPath string) error {
	ctx := context.Background()
	rootPath = NormalizeSubtreeRootPathForPropagation(rootPath)
	if rootPath == "/" {
		_, err := w.tx.ExecContext(ctx,
			`DELETE FROM `+TableGPLIssues+` WHERE src_id IN (SELECT id FROM `+TableSrcNodes+` WHERE path LIKE '/%')`)
		return err
	}
	_, err := w.tx.ExecContext(ctx,
		`DELETE FROM `+TableGPLIssues+` WHERE src_id IN (SELECT id FROM `+TableSrcNodes+` WHERE path = $1 OR path LIKE $2)`,
		rootPath, rootPath+"/%")
	return err
}

func (w *Writer) BatchInsertPathEvents(events []PathEvent) error {
	if len(events) == 0 {
		return nil
	}
	issues := make([]GPLIssue, 0, len(events))
	for _, e := range events {
		issues = append(issues, GPLIssue{
			SrcID:        e.ID,
			Status:       e.Status,
			ProposedName: e.ProposedPath,
			IssuesJSON:   e.GPLIssues,
			UpdatedAt:    e.EventTime,
		})
	}
	return w.BatchInsertGPLIssues(issues)
}

// BatchInsertIDMapEvents inserts rows into id_map.
func (w *Writer) BatchInsertIDMapEvents(events []IDMapEvent) error {
	if len(events) == 0 {
		return nil
	}
	ctx := context.Background()
	for _, e := range events {
		_, err := w.tx.ExecContext(ctx,
			`INSERT INTO `+TableIDMap+` (src_internal_id, dst_internal_id, event_time, source, status) VALUES ($1, $2, $3, $4, $5)`,
			e.SrcInternalID, e.DstInternalID, e.EventTime, e.Source, e.Status,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// DeleteNode deletes the node from the given table (for retry DST cleanup).
func (w *Writer) DeleteNode(table, nodeID string) error {
	t := TableName(table)
	_, err := w.tx.ExecContext(context.Background(), `DELETE FROM `+t+` WHERE id = $1`, nodeID)
	return err
}

// CountExcludedInSubtree returns (excluded, notExcluded) counts for nodes in the SRC subtree. Excluded = copy_status in (excluded_explicit, excluded_inherited). Call inside a transaction.
func (w *Writer) LatestNonIgnoredGPLStatus(nodeID string) (string, error) {
	ctx := context.Background()
	var s string
	err := w.tx.QueryRowContext(ctx, `
SELECT COALESCE(arg_max(gpl_status, event_time), '')
FROM src_status_events
WHERE id = $1
  AND COALESCE(gpl_status, '') <> ''
  AND gpl_status <> $2`, nodeID, GPLStatusIgnored).Scan(&s)
	return s, err
}

// LatestNonExclusionCopyStatus returns the latest copy_status for nodeID that is not an exclusion
// status. Empty string means none found (caller should treat as pending). Call inside a transaction.
func (w *Writer) LatestNonExclusionCopyStatus(nodeID string) (string, error) {
	ctx := context.Background()
	var s string
	err := w.tx.QueryRowContext(ctx, `
SELECT COALESCE(arg_max(copy_status, event_time), '')
FROM src_status_events
WHERE id = $1
  AND COALESCE(copy_status, '') <> ''
  AND copy_status NOT IN `+SQLCopyStatusExcludedIN, nodeID).Scan(&s)
	return s, err
}

// LatestNonPendingTraversalStatus returns the latest traversal_status that is not pending
// for a SRC or DST node. Used when undoing a discovery retry. Call inside a transaction.
func (w *Writer) LatestNonPendingTraversalStatus(table, nodeID string) (string, error) {
	ctx := context.Background()
	evTbl := TableSrcStatusEvents
	if table == "DST" {
		evTbl = TableDstStatusEvents
	}
	var s string
	err := w.tx.QueryRowContext(ctx, `
SELECT COALESCE(arg_max(traversal_status, event_time), '')
FROM `+evTbl+`
WHERE id = $1
  AND COALESCE(traversal_status, '') <> ''
  AND traversal_status <> $2`, nodeID, StatusPending).Scan(&s)
	return s, err
}

// CountDstNodesUnderPath returns the number of DST nodes whose parent_path equals parentPath (direct children only). Call inside a transaction.
func (w *Writer) CountDstNodesUnderPath(parentPath string) (int64, error) {
	ctx := context.Background()
	normParentPath := NormalizeRootRelativePath(parentPath)
	var n int64
	err := w.tx.QueryRowContext(ctx, `SELECT COUNT(*)::BIGINT FROM dst_nodes WHERE parent_path = $1`, normParentPath).Scan(&n)
	return n, err
}

// CountDstNodesUnderPathWithTraversalStatus returns the number of DST nodes under parentPath (parent_path = parentPath) whose current traversal_status equals status. Call inside a transaction.
func (w *Writer) CountDstNodesUnderPathWithTraversalStatus(parentPath, status string) (int64, error) {
	ctx := context.Background()
	normParentPath := NormalizeRootRelativePath(parentPath)
	q := `WITH latest AS (SELECT id, arg_max(traversal_status, event_time) AS traversal_status FROM dst_status_events GROUP BY id)
SELECT COUNT(*)::BIGINT FROM dst_nodes n JOIN latest e ON n.id = e.id WHERE n.parent_path = $1 AND COALESCE(e.traversal_status,'') = $2`
	var n int64
	err := w.tx.QueryRowContext(ctx, q, normParentPath, status).Scan(&n)
	return n, err
}

// NodeTraversalStatus returns the current traversal_status for the node from the events table. Call inside a transaction.
func (w *Writer) NodeTraversalStatus(table, nodeID string) (string, error) {
	ctx := context.Background()
	evTbl := TableSrcStatusEvents
	if table == "DST" {
		evTbl = TableDstStatusEvents
	}
	var s string
	err := w.tx.QueryRowContext(ctx, `SELECT COALESCE(arg_max(traversal_status, event_time), '') FROM `+evTbl+` WHERE id = $1`, nodeID).Scan(&s)
	return s, err
}

// SealDepth0 emits status events for each depth-0 node so the events table reflects current state
// (e.g. root marked successful after round 0 completes).
func (w *Writer) SealDepth0(table string, nodes []*NodeState) error {
	eventTime := time.Now().UnixNano()
	for _, nd := range nodes {
		trav := nd.TraversalStatus
		if trav == "" {
			trav = nd.Status
		}
		ev := &StatusEvent{ID: nd.ID, TraversalStatus: trav, EventTime: eventTime, Depth: 0}
		if table == "SRC" {
			ev.CopyStatus = nd.CopyStatus
		}
		if err := w.InsertStatusEvent(table, ev); err != nil {
			return err
		}
	}
	return nil
}

// SetNodeTraversalStatus emits a traversal_status event. Single-node path: no full recompute.
func (w *Writer) SetNodeTraversalStatus(table, nodeID, status string) error {
	ctx := context.Background()
	t := TableName(table)
	var depth int
	err := w.tx.QueryRowContext(ctx, `SELECT depth FROM `+t+` WHERE id = $1`, nodeID).Scan(&depth)
	if err != nil {
		return err
	}
	ev := &StatusEvent{ID: nodeID, TraversalStatus: status, EventTime: time.Now().UnixNano(), Depth: depth}
	if table == "SRC" {
		var copyStatus string
		_ = w.tx.QueryRowContext(ctx, `SELECT COALESCE(arg_max(copy_status, event_time), '') FROM src_status_events WHERE id = $1 AND COALESCE(copy_status, '') <> ''`, nodeID).Scan(&copyStatus)
		ev.CopyStatus = copyStatus
	}
	return w.InsertStatusEvent(table, ev)
}

// SetNodeCopyStatus emits a copy_status event for SRC.
func (w *Writer) SetNodeCopyStatus(table, nodeID, status string) error {
	if table != "SRC" {
		return nil
	}
	ctx := context.Background()
	t := TableName(table)
	var depth int
	err := w.tx.QueryRowContext(ctx, `SELECT depth FROM `+t+` WHERE id = $1`, nodeID).Scan(&depth)
	if err != nil {
		return err
	}
	var trav string
	_ = w.tx.QueryRowContext(ctx, `SELECT COALESCE(arg_max(traversal_status, event_time), '') FROM src_status_events WHERE id = $1`, nodeID).Scan(&trav)
	ev := &StatusEvent{ID: nodeID, TraversalStatus: trav, CopyStatus: status, EventTime: time.Now().UnixNano(), Depth: depth}
	return w.InsertStatusEvent("SRC", ev)
}

// SetNodeDeleteStatus emits a delete_status event for SRC.
func (w *Writer) SetNodeDeleteStatus(table, nodeID, status string) error {
	if table != "SRC" {
		return nil
	}
	ctx := context.Background()
	t := TableName(table)
	var depth int
	err := w.tx.QueryRowContext(ctx, `SELECT depth FROM `+t+` WHERE id = $1`, nodeID).Scan(&depth)
	if err != nil {
		return err
	}
	var copySt, trav string
	_ = w.tx.QueryRowContext(ctx, `SELECT COALESCE(arg_max(copy_status, event_time), '') FROM src_status_events WHERE id = $1 AND COALESCE(copy_status, '') <> ''`, nodeID).Scan(&copySt)
	_ = w.tx.QueryRowContext(ctx, `SELECT COALESCE(arg_max(traversal_status, event_time), '') FROM src_status_events WHERE id = $1`, nodeID).Scan(&trav)
	ev := &StatusEvent{ID: nodeID, TraversalStatus: trav, CopyStatus: copySt, DeleteStatus: status, EventTime: time.Now().UnixNano(), Depth: depth}
	return w.InsertStatusEvent("SRC", ev)
}

// SetNodeExcluded emits a copy_status-only event for SRC: excluding sets copy_status to excluded_explicit
// (traversal_status unchanged); unexcluding restores the latest non-exclusion copy_status (pending if none).
// Also appends gpl_status=ignored on exclude and restores the prior non-ignored gpl_status on unexclude
// so destination naming warnings stay hidden while excluded.
func (w *Writer) SetNodeExcluded(table, nodeID string, excluded bool) error {
	ctx := context.Background()
	t := TableName(table)
	var depth int
	if err := w.tx.QueryRowContext(ctx, `SELECT depth FROM `+t+` WHERE id = $1`, nodeID).Scan(&depth); err != nil {
		return err
	}
	var curTraversal string
	_ = w.tx.QueryRowContext(ctx, `SELECT COALESCE(arg_max(traversal_status, event_time), '') FROM src_status_events WHERE id = $1`, nodeID).Scan(&curTraversal)
	newCopyStatus := CopyStatusExcludedExplicit
	gplStatus := GPLStatusIgnored
	if !excluded {
		restore, err := w.LatestNonExclusionCopyStatus(nodeID)
		if err != nil {
			return err
		}
		if restore == "" {
			restore = CopyStatusPending
		}
		newCopyStatus = restore
		priorGPL, err := w.LatestNonIgnoredGPLStatus(nodeID)
		if err != nil {
			return err
		}
		if priorGPL == "" {
			priorGPL = GPLStatusSuccessful
		}
		gplStatus = priorGPL
	}
	ev := &StatusEvent{
		ID:              nodeID,
		TraversalStatus: curTraversal,
		CopyStatus:      newCopyStatus,
		GPLStatus:       gplStatus,
		EventTime:       time.Now().UnixNano(),
		Depth:           depth,
	}
	return w.InsertStatusEvent("SRC", ev)
}

// DeleteDescendantsUnderPath deletes all DST/SRC nodes that are strict descendants of rootPath.
func (w *Writer) DeleteDescendantsUnderPath(table, rootPath string) error {
	ctx := context.Background()
	t := TableName(table)
	var pathCond string
	var args []any
	if rootPath == "/" {
		pathCond = `path != '/' AND path LIKE '/%'`
	} else {
		pathCond = `path LIKE $1`
		args = append(args, rootPath+"/%")
	}
	if table == "DST" {
		idsRows, err := w.tx.QueryContext(ctx, `SELECT id FROM dst_nodes WHERE `+pathCond, args...)
		if err != nil {
			return err
		}
		var ids []string
		for idsRows.Next() {
			var id string
			if err := idsRows.Scan(&id); err != nil {
				idsRows.Close()
				return err
			}
			ids = append(ids, id)
		}
		idsRows.Close()
		if err = idsRows.Err(); err != nil {
			return err
		}
		if len(ids) > 0 {
			placeholders := make([]string, len(ids))
			for i := range ids {
				placeholders[i] = fmt.Sprintf("$%d", i+1)
			}
			argList := make([]any, len(ids))
			for i, id := range ids {
				argList[i] = id
			}
			_, err = w.tx.ExecContext(ctx, `DELETE FROM dst_status_events WHERE id IN (`+strings.Join(placeholders, ",")+`)`, argList...)
			if err != nil {
				return err
			}
		}
	}
	delQ := `DELETE FROM ` + t + ` WHERE ` + pathCond
	var err error
	if len(args) == 0 {
		_, err = w.tx.ExecContext(ctx, delQ)
	} else {
		_, err = w.tx.ExecContext(ctx, delQ, args...)
	}
	return err
}

// DeleteSubtree deletes all nodes in the subtree at rootPath (path = rootPath OR path LIKE rootPath || '/%'; for rootPath "/" uses path LIKE '/%'). Table is "SRC" or "DST".
func (w *Writer) DeleteSubtree(table, rootPath string) error {
	ctx := context.Background()
	t := TableName(table)
	var err error
	if rootPath == "/" {
		_, err = w.tx.ExecContext(ctx, `DELETE FROM `+t+` WHERE path LIKE '/%'`)
	} else {
		prefix := rootPath + "/%"
		_, err = w.tx.ExecContext(ctx, `DELETE FROM `+t+` WHERE path = $1 OR path LIKE $2`, rootPath, prefix)
	}
	return err
}

// InsertLog inserts a row into logs. id must be unique (e.g. from GenerateLogID).
func (w *Writer) InsertLog(id string, level, message, component, entity, entityID, queue string) error {
	_, err := w.tx.ExecContext(context.Background(),
		`INSERT INTO logs (id, level, message, component, entity, entity_id, queue) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		id, level, message, component, entity, entityID, queue,
	)
	return err
}

// InsertTaskFailureLog inserts a task_failure log row with structured detail (bare error) separate from message.
func (w *Writer) InsertTaskFailureLog(id, level, message, detail, entity, entityID, queue string) error {
	_, err := w.tx.ExecContext(context.Background(),
		`INSERT INTO logs (id, level, message, detail, component, entity, entity_id, queue) VALUES ($1, $2, $3, $4, 'task_failure', $5, $6, $7)`,
		id, level, message, nullIfEmpty(detail), entity, entityID, queue,
	)
	return err
}

// RecordTaskError inserts a row into task_errors.
func (w *Writer) RecordTaskError(queueType, phase, nodeID, message string, attempts int, path string) error {
	_, err := w.tx.ExecContext(context.Background(),
		`INSERT INTO task_errors (queue_type, phase, node_id, message, attempts, path) VALUES ($1, $2, $3, $4, $5, $6)`,
		queueType, phase, nodeID, message, attempts, path,
	)
	return err
}

// AppendQueueStats appends queue metrics JSON into queue_stats for the given phase family.
func (w *Writer) AppendQueueStats(queueKey, phase, metricsJSON string) error {
	_, err := w.tx.ExecContext(context.Background(),
		`INSERT INTO queue_stats (queue_key, phase, metrics_json) VALUES ($1, $2, $3)`,
		queueKey, phase, metricsJSON,
	)
	return err
}

// PruneQueueStats deletes older rows, keeping only the latest event per (queue_key, phase).
func (w *Writer) PruneQueueStats() error {
	_, err := w.tx.ExecContext(context.Background(),
		`DELETE FROM queue_stats AS qs
		 WHERE EXISTS (
		   SELECT 1 FROM queue_stats AS newer
		   WHERE newer.queue_key = qs.queue_key
		     AND newer.phase = qs.phase
		     AND newer.event_time > qs.event_time
		 )`,
	)
	return err
}

// NodePath returns the path and table ("SRC" or "DST") for a node by ID, or error if not found.
func (w *Writer) NodePath(nodeID string) (path string, tbl string, err error) {
	ctx := context.Background()
	err = w.tx.QueryRowContext(ctx, `SELECT path FROM `+TableSrcNodes+` WHERE id = $1`, nodeID).Scan(&path)
	if err == nil {
		return path, "SRC", nil
	}
	if err != sql.ErrNoRows {
		return "", "", err
	}
	err = w.tx.QueryRowContext(ctx, `SELECT path FROM `+TableDstNodes+` WHERE id = $1`, nodeID).Scan(&path)
	if err == nil {
		return path, "DST", nil
	}
	return "", "", err
}

// CountPendingTraversalAtPath returns how many nodes (SRC + DST) at the given path have current traversal_status = 'pending'.
// Call inside the same transaction after status events have been written so the count reflects the new state.
func (w *Writer) CountPendingTraversalAtPath(path string) (int, error) {
	ctx := context.Background()
	q := `WITH src_latest AS (SELECT id, arg_max(traversal_status, event_time) AS s FROM ` + TableSrcStatusEvents + ` GROUP BY id),
dst_latest AS (SELECT id, arg_max(traversal_status, event_time) AS d FROM ` + TableDstStatusEvents + ` GROUP BY id)
SELECT
  (SELECT COUNT(*)::BIGINT FROM ` + TableSrcNodes + ` n LEFT JOIN src_latest e ON n.id = e.id WHERE n.path = $1 AND COALESCE(e.s,'') = 'pending')
  + (SELECT COUNT(*)::BIGINT FROM ` + TableDstNodes + ` n LEFT JOIN dst_latest e ON n.id = e.id WHERE n.path = $1 AND COALESCE(e.d,'') = 'pending')`
	var n int64
	if err := w.tx.QueryRowContext(ctx, q, path).Scan(&n); err != nil {
		return 0, err
	}
	return int(n), nil
}

// ReviewStatsDelta is one (key, delta) for the universal stats table.
type ReviewStatsDelta struct {
	Key   string
	Delta int64
}

// ApplyReviewStatsDeltas applies deltas to the universal stats table (count += delta per key). Used for incremental review stats updates.
func (w *Writer) ApplyReviewStatsDeltas(deltas []ReviewStatsDelta) error {
	ctx := context.Background()
	for _, d := range deltas {
		if d.Delta == 0 {
			continue
		}
		_, err := w.tx.ExecContext(ctx,
			`INSERT INTO `+TableStats+` (key, count) VALUES ($1, $2)
			 ON CONFLICT (key) DO UPDATE SET count = count + excluded.count`,
			d.Key, d.Delta,
		)
		if err != nil {
			return err
		}
	}
	return nil
}
