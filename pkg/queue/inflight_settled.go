// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import "sync/atomic"

// SettledInFlightTask is a one-poll flash of a task that left the prior active snapshot.
type SettledInFlightTask struct {
	NodeID     string `json:"nodeId"`
	Path       string `json:"path"`
	TaskType   string `json:"taskType,omitempty"`
	ItemKind   string `json:"itemKind,omitempty"`
	Success    bool   `json:"success"`
	BytesDone  int64  `json:"bytesDone,omitempty"`
	BytesTotal int64  `json:"bytesTotal,omitempty"`
}

type settledHint struct {
	Path       string
	TaskType   string
	ItemKind   string
	BytesTotal int64
}

type settledOutcome struct {
	Path       string
	TaskType   string
	ItemKind   string
	Success    bool
	BytesDone  int64
	BytesTotal int64
}

func taskItemKind(tb *TaskBase) (kind string, bytesTotal int64) {
	if tb == nil {
		return "", 0
	}
	switch {
	case tb.Type == TaskTypeCopyFile || tb.Type == TaskTypeDeleteFile || tb.IsFile():
		return "file", tb.File.Size
	case tb.Type == TaskTypeCopyFolder || tb.Type == TaskTypeDeleteFolder || tb.IsFolder():
		return "folder", 0
	default:
		return "", 0
	}
}

func taskDisplayPath(tb *TaskBase) string {
	if tb == nil {
		return ""
	}
	path := tb.DisplayPath
	if path == "" {
		path = tb.SrcLogicalPath
	}
	if path == "" {
		path = tb.LocationPath()
	}
	return path
}

// RememberSettledOutcome records a terminal leave for the next queue-metrics settled window.
// Retry requeues and scale-down abandons must not call this.
func (q *Queue) RememberSettledOutcome(task *TaskBase, success bool) {
	if q == nil || task == nil || task.ID == "" {
		return
	}
	kind, bytesTotal := taskItemKind(task)
	out := settledOutcome{
		Path:       taskDisplayPath(task),
		TaskType:   task.Type,
		ItemKind:   kind,
		Success:    success,
		BytesDone:  atomic.LoadInt64(&task.BytesTransferred),
		BytesTotal: bytesTotal,
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.recentOutcomes == nil {
		q.recentOutcomes = make(map[string]settledOutcome)
	}
	q.recentOutcomes[task.ID] = out
}

// SnapshotSettledWindow returns active leases plus a one-shot settled flash for ids that
// left since the previous snapshot. Still-active ids stay tracked; settled ids are forgotten
// after emit. Snapshot and window swap use separate locks (never nested).
func (q *Queue) SnapshotSettledWindow(limit int) (active []InFlightTask, settled []SettledInFlightTask) {
	if q == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 32
	}
	active = q.InFlightSnapshot(limit)

	activeIDs := make(map[string]struct{}, len(active))
	next := make(map[string]settledHint, len(active))
	for _, t := range active {
		if t.NodeID == "" {
			continue
		}
		activeIDs[t.NodeID] = struct{}{}
		next[t.NodeID] = settledHint{
			Path: t.Path, TaskType: t.TaskType, ItemKind: t.ItemKind, BytesTotal: t.BytesTotal,
		}
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.lastSentActive == nil {
		q.lastSentActive = make(map[string]settledHint)
	}
	for id, hint := range q.lastSentActive {
		if _, still := activeIDs[id]; still {
			continue
		}
		out, ok := q.recentOutcomes[id]
		if !ok {
			continue
		}
		delete(q.recentOutcomes, id)
		if out.Path == "" {
			out.Path = hint.Path
		}
		if out.TaskType == "" {
			out.TaskType = hint.TaskType
		}
		if out.ItemKind == "" {
			out.ItemKind = hint.ItemKind
		}
		if out.BytesTotal == 0 {
			out.BytesTotal = hint.BytesTotal
		}
		settled = append(settled, SettledInFlightTask{
			NodeID: id, Path: out.Path, TaskType: out.TaskType, ItemKind: out.ItemKind,
			Success: out.Success, BytesDone: out.BytesDone, BytesTotal: out.BytesTotal,
		})
	}
	q.lastSentActive = next
	if len(q.recentOutcomes) > limit*2 {
		for id := range q.recentOutcomes {
			if _, activeNow := activeIDs[id]; activeNow {
				continue
			}
			delete(q.recentOutcomes, id)
			if len(q.recentOutcomes) <= limit {
				break
			}
		}
	}
	return active, settled
}
