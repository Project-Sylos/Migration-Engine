// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import "fmt"

// FlushFailureLogsFromEvents writes prepared failure-log fields from status events into the logs table.
// Lives in root db so seal can flush without importing failurelog (avoids db↔seal test import cycles).
func FlushFailureLogsFromEvents(w *Writer, events []StatusEvent, defaultQueue string) error {
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
