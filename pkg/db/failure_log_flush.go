// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

func taskFailureLog(e StatusEvent, defaultQueue string) (opsdb.LogRecord, bool) {
	if e.ErrorLogID == "" || e.ErrorLogMessage == "" {
		return opsdb.LogRecord{}, false
	}
	queue := e.ErrorLogQueue
	if queue == "" {
		queue = defaultQueue
	}
	return opsdb.LogRecord{
		ID:        e.ErrorLogID,
		Level:     "error",
		Message:   e.ErrorLogMessage,
		Detail:    e.ErrorLogDetail,
		Component: "task_failure",
		Entity:    defaultQueue,
		EntityID:  e.ID,
		Queue:     queue,
		Action:    "task_failure",
		NodeID:    e.ID,
		At:        time.Now(),
	}, true
}

// FlushFailureLogEvent writes one prepared failure log row to the per-migration logs sidecar.
func FlushFailureLogEvent(database *DB, e StatusEvent, defaultQueue string) error {
	if database == nil || database.Logs() == nil {
		return nil
	}
	rec, ok := taskFailureLog(e, defaultQueue)
	if !ok {
		return nil
	}
	return database.Logs().AppendLogs([]opsdb.LogRecord{rec})
}
