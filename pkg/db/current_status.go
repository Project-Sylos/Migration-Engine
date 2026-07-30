// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// Shared SELECT bodies that define "current status" from the event log (single source of truth for rebuilds).

func srcCurrentSelectFromEventsSQL(scopeSQL string) string {
	if scopeSQL == "" {
		scopeSQL = "TRUE"
	}
	return `
SELECT
	cur.id,
	cur.traversal_status,
	cur.copy_status,
	cur.delete_status,
	cur.error_log_id,
	cur.gpl_status,
	COALESCE(pr.proposed_path, '') AS resolved_dst_name,
	cur.event_time,
	cur.depth
FROM (
	SELECT
		e.id,
		COALESCE(arg_max(e.traversal_status, e.event_time), '') AS traversal_status,
		COALESCE(arg_max(e.copy_status, e.event_time) FILTER (WHERE COALESCE(e.copy_status, '') <> ''), '') AS copy_status,
		COALESCE(arg_max(e.delete_status, e.event_time) FILTER (WHERE COALESCE(e.delete_status, '') <> ''), '') AS delete_status,
		COALESCE(arg_max(e.error_log_id, e.event_time) FILTER (WHERE COALESCE(e.error_log_id, '') <> ''), '') AS error_log_id,
		COALESCE(arg_max(e.gpl_status, e.event_time) FILTER (WHERE COALESCE(e.gpl_status, '') <> ''), '') AS gpl_status,
		MAX(e.event_time) AS event_time,
		CAST(arg_max(e.depth, e.event_time) AS INTEGER) AS depth
	FROM ` + TableSrcStatusEvents + ` e
	WHERE ` + scopeSQL + `
	GROUP BY e.id
) cur
LEFT JOIN (
	SELECT gi.src_id AS id, gi.proposed_name AS proposed_path
	FROM ` + TableGPLIssues + ` gi
	WHERE gi.status = 'accepted'
) pr ON pr.id = cur.id`
}

func dstCurrentSelectFromEventsSQL(scopeSQL string) string {
	if scopeSQL == "" {
		scopeSQL = "TRUE"
	}
	return `
SELECT
	e.id,
	COALESCE(arg_max(e.traversal_status, e.event_time), '') AS traversal_status,
	COALESCE(arg_max(e.error_log_id, e.event_time) FILTER (WHERE COALESCE(e.error_log_id, '') <> ''), '') AS error_log_id,
	COALESCE(arg_max(e.gpl_status, e.event_time) FILTER (WHERE COALESCE(e.gpl_status, '') <> ''), '') AS gpl_status,
	MAX(e.event_time) AS event_time,
	CAST(arg_max(e.depth, e.event_time) AS INTEGER) AS depth
FROM ` + TableDstStatusEvents + ` e
WHERE ` + scopeSQL + `
GROUP BY e.id`
}

const srcCurrentUpsertConflictSQL = `
ON CONFLICT (id) DO UPDATE SET
	traversal_status = EXCLUDED.traversal_status,
	copy_status = EXCLUDED.copy_status,
	delete_status = EXCLUDED.delete_status,
	error_log_id = EXCLUDED.error_log_id,
	gpl_status = EXCLUDED.gpl_status,
	resolved_dst_name = EXCLUDED.resolved_dst_name,
	event_time = EXCLUDED.event_time,
	depth = EXCLUDED.depth`

const dstCurrentUpsertConflictSQL = `
ON CONFLICT (id) DO UPDATE SET
	traversal_status = EXCLUDED.traversal_status,
	error_log_id = EXCLUDED.error_log_id,
	gpl_status = EXCLUDED.gpl_status,
	event_time = EXCLUDED.event_time,
	depth = EXCLUDED.depth`

type currentSideLayout struct {
	currentTable string
	nodesTable   string
	eventsTable  string
	insertCols   string
	selectSQL    func(string) string
	conflictSQL  string
	upsertLabel  string
}

func currentSideLayoutFor(side string) currentSideLayout {
	if side == "DST" {
		return currentSideLayout{
			currentTable: TableDstCurrent,
			nodesTable:   TableDstNodes,
			eventsTable:  TableDstStatusEvents,
			insertCols:   "id, traversal_status, error_log_id, gpl_status, event_time, depth",
			selectSQL:    dstCurrentSelectFromEventsSQL,
			conflictSQL:  dstCurrentUpsertConflictSQL,
			upsertLabel:  "dst_current",
		}
	}
	return currentSideLayout{
		currentTable: TableSrcCurrent,
		nodesTable:   TableSrcNodes,
		eventsTable:  TableSrcStatusEvents,
		insertCols:   "id, traversal_status, copy_status, delete_status, error_log_id, gpl_status, resolved_dst_name, event_time, depth",
		selectSQL:    srcCurrentSelectFromEventsSQL,
		conflictSQL:  srcCurrentUpsertConflictSQL,
		upsertLabel:  "src_current",
	}
}

func (db *DB) upsertCurrentFromEvents(ctx context.Context, side, scopeSQL string, args []any) error {
	return db.RunWrite(ctx, func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			return w.upsertCurrentFromEvents(side, scopeSQL, args)
		})
	})
}

func (w *Writer) upsertCurrentFromEvents(side, scopeSQL string, args []any) error {
	layout := currentSideLayoutFor(side)
	ctx := context.Background()
	q := `INSERT INTO ` + layout.currentTable + ` (` + layout.insertCols + `)` +
		layout.selectSQL(scopeSQL) + layout.conflictSQL
	if _, err := w.tx.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("upsert %s: %w", layout.upsertLabel, err)
	}
	return nil
}

// RebuildCurrentAtDepth re-derives src_current or dst_current for all ids with events at the given depth.
func (db *DB) RebuildCurrentAtDepth(side string, depth int) error {
	ctx := context.Background()
	return db.upsertCurrentFromEvents(ctx, side, `e.depth = $1`, []any{depth})
}

// RebuildCurrentByIDs re-derives src_current or dst_current for the given ids.
func (db *DB) RebuildCurrentByIDs(side string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	ctx := context.Background()
	ph, args := placeholders1(ids)
	return db.upsertCurrentFromEvents(ctx, side, `e.id IN (`+ph+`)`, args)
}

// RebuildCurrentSince rebuilds src_current or dst_current for ids that have events at or after eventTimeNanos.
func (db *DB) RebuildCurrentSince(side string, eventTimeNanos int64) error {
	ctx := context.Background()
	layout := currentSideLayoutFor(side)
	ids, err := db.distinctEventIDsSince(ctx, layout.eventsTable, eventTimeNanos)
	if err != nil {
		return err
	}
	return db.RebuildCurrentByIDs(side, ids)
}

func (db *DB) distinctEventIDsSince(ctx context.Context, eventTable string, eventTimeNanos int64) ([]string, error) {
	conn, err := db.GetDB()
	if err != nil {
		return nil, err
	}
	rows, err := conn.QueryContext(ctx, `SELECT DISTINCT id FROM `+eventTable+` WHERE event_time >= $1`, eventTimeNanos)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// RebuildAllCurrent rebuilds both src_current and dst_current from the full event log.
// Uses upsert (DuckDB cannot DELETE+INSERT the same PK in one transaction).
func (db *DB) RebuildAllCurrent() error {
	ctx := context.Background()
	for _, side := range []string{"SRC", "DST"} {
		if err := db.upsertCurrentFromEvents(ctx, side, `TRUE`, nil); err != nil {
			return err
		}
	}
	return db.RunWrite(ctx, func(s *WriteSession) error {
		return s.WithTx(func(w *Writer) error {
			for _, side := range []string{"SRC", "DST"} {
				layout := currentSideLayoutFor(side)
				if _, err := w.tx.ExecContext(ctx,
					`DELETE FROM `+layout.currentTable+` WHERE id NOT IN (SELECT DISTINCT id FROM `+layout.eventsTable+`)`); err != nil {
					return fmt.Errorf("prune %s: %w", layout.upsertLabel, err)
				}
			}
			return nil
		})
	})
}

// RebuildCurrentByDepth rebuilds both sides for one depth (after a round seal).
func (db *DB) RebuildCurrentByDepth(depth int) error {
	for _, side := range []string{"SRC", "DST"} {
		if err := db.RebuildCurrentAtDepth(side, depth); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) RefreshCurrentByIDs(side string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	ph, args := placeholders1(ids)
	return w.upsertCurrentFromEvents(side, `e.id IN (`+ph+`)`, args)
}

func (w *Writer) RefreshCurrentByPathPrefix(side, rootPath string) error {
	layout := currentSideLayoutFor(side)
	scope := `e.id IN (SELECT id FROM ` + layout.nodesTable + ` WHERE path LIKE '/%')`
	insArgs := []any{}
	if rootPath != "/" {
		scope = `e.id IN (SELECT id FROM ` + layout.nodesTable + ` WHERE path = $1 OR path LIKE $2)`
		insArgs = []any{rootPath, rootPath + "/%"}
	}
	return w.upsertCurrentFromEvents(side, scope, insArgs)
}

// SetSrcCurrentResolvedName sets resolved_dst_name on src_current for one id (review accept/remap).
func (w *Writer) SetSrcCurrentResolvedName(nodeID, resolvedName string, eventTime int64) error {
	ctx := context.Background()
	_, err := w.tx.ExecContext(ctx, `
INSERT INTO `+TableSrcCurrent+` (id, traversal_status, copy_status, delete_status, error_log_id, gpl_status, resolved_dst_name, event_time, depth)
SELECT n.id,
	COALESCE(c.traversal_status, ''),
	COALESCE(c.copy_status, ''),
	COALESCE(c.delete_status, ''),
	COALESCE(c.error_log_id, ''),
	COALESCE(c.gpl_status, ''),
	$2,
	$3,
	n.depth
FROM `+TableSrcNodes+` n
LEFT JOIN `+TableSrcCurrent+` c ON c.id = n.id
WHERE n.id = $1
ON CONFLICT (id) DO UPDATE SET
	resolved_dst_name = EXCLUDED.resolved_dst_name,
	event_time = EXCLUDED.event_time
`, nodeID, resolvedName, eventTime)
	return err
}

func placeholders1(ids []string) (string, []any) {
	ph := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		ph[i] = "$" + strconv.Itoa(i+1)
		args[i] = id
	}
	return strings.Join(ph, ","), args
}
