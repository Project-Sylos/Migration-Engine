// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// FailureLog is a row from logs for task failure display.
type FailureLog struct {
	ID        string
	Level     string
	Message   string
	Detail    string // bare error text for UI; empty when detail was not stored separately
	Component string
}

// FailureLogDisplayText returns the user-facing error string (detail when present, else full message).
func FailureLogDisplayText(log FailureLog) string {
	if strings.TrimSpace(log.Detail) != "" {
		return log.Detail
	}
	return log.Message
}

// LatestFailureLogIDsByNodeIDs returns the error_log_id from the newest status event
// (by event_time) that recorded a failure log for each node id.
func LatestFailureLogIDsByNodeIDs(ctx context.Context, d *DB, eventTable string, nodeIDs []string) (map[string]string, error) {
	out := make(map[string]string)
	if d == nil || len(nodeIDs) == 0 {
		return out, nil
	}
	switch eventTable {
	case tableSrcStatusEvents, tableDstStatusEvents:
	default:
		return nil, fmt.Errorf("unsupported status event table %q", eventTable)
	}

	placeholders := make([]string, len(nodeIDs))
	args := make([]any, len(nodeIDs))
	for i, id := range nodeIDs {
		placeholders[i] = "$" + strconv.Itoa(i+1)
		args[i] = id
	}
	q := `SELECT id, arg_max(error_log_id, event_time) AS error_log_id
FROM ` + eventTable + `
WHERE id IN (` + strings.Join(placeholders, ",") + `)
  AND error_log_id IS NOT NULL AND error_log_id != ''
GROUP BY id`

	rows, err := d.conn.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var nodeID, logID string
		if err := rows.Scan(&nodeID, &logID); err != nil {
			return nil, err
		}
		if logID != "" {
			out[nodeID] = logID
		}
	}
	return out, rows.Err()
}

// GetFailureLogsByIDs loads logs rows keyed by id.
func GetFailureLogsByIDs(ctx context.Context, d *DB, logIDs []string) (map[string]FailureLog, error) {
	out := make(map[string]FailureLog)
	if d == nil || len(logIDs) == 0 {
		return out, nil
	}

	unique := make([]string, 0, len(logIDs))
	seen := make(map[string]struct{}, len(logIDs))
	for _, id := range logIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	if len(unique) == 0 {
		return out, nil
	}

	placeholders := make([]string, len(unique))
	args := make([]any, len(unique))
	for i, id := range unique {
		placeholders[i] = "$" + strconv.Itoa(i+1)
		args[i] = id
	}
	q := `SELECT id, level, message, COALESCE(detail, ''), component FROM logs WHERE id IN (` + strings.Join(placeholders, ",") + `)`

	rows, err := d.conn.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var row FailureLog
		if err := rows.Scan(&row.ID, &row.Level, &row.Message, &row.Detail, &row.Component); err != nil {
			return nil, err
		}
		out[row.ID] = row
	}
	return out, rows.Err()
}
