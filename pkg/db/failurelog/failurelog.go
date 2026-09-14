// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package failurelog

import (
	"context"
	"fmt"
	"strings"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"

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

func sideFromEventTable(eventTable string) string {
	switch eventTable {
	case db.TableDstStatusEvents, db.TableDstCurrent:
		return opsdb.SideDST
	default:
		return opsdb.SideSRC
	}
}

// LatestFailureLogIDsByNodeIDs returns error_log_id for each node id from
// materialized *_current when populated (A), otherwise from status events (B).
// On the Badger ops branch, reads error_log_id from st:* overlays.
func LatestFailureLogIDsByNodeIDs(ctx context.Context, d *db.DB, eventTable string, nodeIDs []string) (map[string]string, error) {
	out := make(map[string]string)
	if d == nil || len(nodeIDs) == 0 {
		return out, nil
	}
	if d.Ops() != nil {
		side := sideFromEventTable(eventTable)
		stMap, err := d.Ops().BatchGetStatus(side, nodeIDs)
		if err != nil {
			return nil, err
		}
		for _, nodeID := range nodeIDs {
			if st, ok := stMap[nodeID]; ok && st.ErrorLogID != "" {
				out[nodeID] = st.ErrorLogID
			}
		}
		return out, nil
	}
	return out, nil
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

	if d.Ops() != nil {
		recs, err := d.Ops().GetLogsByIDs(unique)
		if err != nil {
			return nil, err
		}
		for id, rec := range recs {
			out[id] = FailureLog{
				ID:        rec.ID,
				Level:     rec.Level,
				Message:   rec.Message,
				Detail:    rec.Detail,
				Component: rec.Component,
			}
		}
		return out, nil
	}
	return out, nil
}
