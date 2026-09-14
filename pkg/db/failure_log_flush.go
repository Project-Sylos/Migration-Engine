// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"
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
		At:        time.Now(),
	}, true
}

// FlushFailureLogEvent writes one prepared failure log row to Badger.
func FlushFailureLogEvent(database *DB, e StatusEvent, defaultQueue string) error {
	if database == nil || database.Ops() == nil {
		return nil
	}
	rec, ok := taskFailureLog(e, defaultQueue)
	if !ok {
		return nil
	}
	return database.Ops().AppendLogs([]opsdb.LogRecord{rec})
}

// FlushFailureLogsFromEvents writes prepared failure-log fields from status events into Badger.
func FlushFailureLogsFromEvents(database *DB, events []StatusEvent, defaultQueue string) error {
	if database == nil || database.Ops() == nil {
		return nil
	}
	recs := make([]opsdb.LogRecord, 0, len(events))
	for _, e := range events {
		rec, ok := taskFailureLog(e, defaultQueue)
		if !ok {
			continue
		}
		recs = append(recs, rec)
	}
	if len(recs) == 0 {
		return nil
	}
	return database.Ops().AppendLogs(recs)
}

// FlushFailureLogsToDB writes failure logs to the given DB in one batch.
func FlushFailureLogsToDB(logsDB *DB, events []StatusEvent, defaultQueue string) error {
	if logsDB == nil || len(events) == 0 {
		return nil
	}
	has := false
	for _, e := range events {
		if e.ErrorLogID != "" && e.ErrorLogMessage != "" {
			has = true
			break
		}
	}
	if !has {
		return nil
	}
	if err := FlushFailureLogsFromEvents(logsDB, events, defaultQueue); err != nil {
		return fmt.Errorf("flush failure logs: %w", err)
	}
	return nil
}
