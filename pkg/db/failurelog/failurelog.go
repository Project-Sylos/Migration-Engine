// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package failurelog

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"

	"github.com/google/uuid"
)

// AttachTaskFailureLog prepares a logs-table row to be written at seal flush alongside a failed status event.
func AttachTaskFailureLog(e *db.StatusEvent, phase, queueName, nodeID, path string, attempts int, lastError string) {
	if e == nil {
		return
	}
	if lastError == "" {
		lastError = "unknown error"
	}
	e.ErrorLogID = uuid.New().String()
	e.ErrorLogDetail = lastError
	e.ErrorLogMessage = fmt.Sprintf(
		"%s task failed after %d attempt(s): path=%s node_id=%s error=%s",
		phase, attempts, path, nodeID, lastError,
	)
	e.ErrorLogQueue = queueName
}

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

// LatestFailureLogIDsByNodeIDs returns error_log_id from src_current / dst_current for each node id.
func LatestFailureLogIDsByNodeIDs(ctx context.Context, d *db.DB, eventTable string, nodeIDs []string) (map[string]string, error) {
	out := make(map[string]string)
	if d == nil || len(nodeIDs) == 0 {
		return out, nil
	}
	currentTable := db.TableSrcCurrent
	switch eventTable {
	case db.TableSrcStatusEvents, db.TableSrcCurrent:
		currentTable = db.TableSrcCurrent
	case db.TableDstStatusEvents, db.TableDstCurrent:
		currentTable = db.TableDstCurrent
	default:
		return nil, fmt.Errorf("unsupported status event table %q", eventTable)
	}

	placeholders := make([]string, len(nodeIDs))
	args := make([]any, len(nodeIDs))
	for i, id := range nodeIDs {
		placeholders[i] = "$" + strconv.Itoa(i+1)
		args[i] = id
	}
	q := `SELECT id, COALESCE(error_log_id, '') FROM ` + currentTable + `
WHERE id IN (` + strings.Join(placeholders, ",") + `)
  AND error_log_id IS NOT NULL AND error_log_id != ''`

	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	rows, err := conn.QueryContext(ctx, q, args...)
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
func GetFailureLogsByIDs(ctx context.Context, d *db.DB, logIDs []string) (map[string]FailureLog, error) {
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

	conn, err := d.GetDB()
	if err != nil {
		return nil, err
	}
	rows, err := conn.QueryContext(ctx, q, args...)
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
