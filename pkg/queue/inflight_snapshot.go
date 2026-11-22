// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"sync/atomic"
	"time"
)

// InFlightTask is a memory-only projection of a leased task for live UI.
type InFlightTask struct {
	NodeID     string `json:"nodeId"`
	Path       string `json:"path"`
	WorkerID   string `json:"workerId"`
	TaskType   string `json:"taskType"`
	ItemKind   string `json:"itemKind,omitempty"`
	Round      int    `json:"round,omitempty"`
	CopyPass   int    `json:"copyPass,omitempty"`
	LeaseAgeMs int64  `json:"leaseAgeMs,omitempty"`
	BytesDone  int64  `json:"bytesDone,omitempty"`
	BytesTotal int64  `json:"bytesTotal,omitempty"`
}

// InFlightSnapshot returns up to limit leased tasks without touching Badger.
func (q *Queue) InFlightSnapshot(limit int) []InFlightTask {
	if q == nil {
		return nil
	}
	if limit <= 0 {
		limit = 32
	}
	q.mu.RLock()
	defer q.mu.RUnlock()
	out := make([]InFlightTask, 0, min(limit, len(q.inProgress)))
	now := time.Now()
	for _, flight := range q.inProgress {
		if len(out) >= limit {
			break
		}
		tb := flight.task
		if tb == nil {
			continue
		}
		path := tb.DisplayPath
		if path == "" {
			path = tb.SrcLogicalPath
		}
		if path == "" {
			path = tb.LocationPath()
		}
		kind := ""
		var bytesTotal int64
		switch {
		case tb.Type == TaskTypeCopyFile || tb.Type == TaskTypeDeleteFile || tb.IsFile():
			kind = "file"
			bytesTotal = tb.File.Size
		case tb.Type == TaskTypeCopyFolder || tb.Type == TaskTypeDeleteFolder || tb.IsFolder():
			kind = "folder"
		}
		leaseAge := int64(0)
		if !tb.LeaseTime.IsZero() {
			leaseAge = now.Sub(tb.LeaseTime).Milliseconds()
		}
		out = append(out, InFlightTask{
			NodeID:     tb.ID,
			Path:       path,
			WorkerID:   flight.owner,
			TaskType:   tb.Type,
			ItemKind:   kind,
			Round:      tb.Round,
			CopyPass:   tb.CopyPass,
			LeaseAgeMs: leaseAge,
			BytesDone:  atomic.LoadInt64(&tb.BytesTransferred),
			BytesTotal: bytesTotal,
		})
	}
	return out
}
