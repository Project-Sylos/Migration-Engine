// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"

	"github.com/google/uuid"
)

// AttachTaskFailureLog prepares a logs-table row to be written at seal flush alongside a failed status event.
func AttachTaskFailureLog(e *StatusEvent, phase, queueName, nodeID, path string, attempts int, lastError string) {
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

func flushFailureLogsFromEvents(w *Writer, events []StatusEvent, defaultQueue string) error {
	if w == nil {
		return nil
	}
	for _, e := range events {
		if e.ErrorLogID == "" || e.ErrorLogMessage == "" {
			continue
		}
		queue := e.ErrorLogQueue
		if queue == "" {
			queue = defaultQueue
		}
		if err := w.InsertTaskFailureLog(e.ErrorLogID, "error", e.ErrorLogMessage, e.ErrorLogDetail, defaultQueue, e.ID, queue); err != nil {
			return fmt.Errorf("insert failure log %s: %w", e.ErrorLogID, err)
		}
	}
	return nil
}
