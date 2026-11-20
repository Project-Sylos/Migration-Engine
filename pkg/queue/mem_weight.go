// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import "codeberg.org/Sylos/Migration-Engine/pkg/memweight"

// SetMemWeightLedger attaches the shared ledger (nil clears).
// Call before workers start; the pointer is read without q.mu.
func (q *Queue) SetMemWeightLedger(l *memweight.Ledger) {
	if q == nil {
		return
	}
	q.memWeight = l
}

// AdmitMemWeight returns the admit weight for a task (1, plus expected DST children when present).
func AdmitMemWeight(task *TaskBase) int64 {
	if task == nil {
		return 0
	}
	w := int64(1)
	w += int64(len(task.ExpectedFolders) + len(task.ExpectedFiles))
	return w
}

func (q *Queue) notePendingAdmit(task *TaskBase) {
	if q == nil || task == nil {
		return
	}
	w := AdmitMemWeight(task)
	task.MemWeight = w
	if q.memWeight != nil {
		q.memWeight.Add(memweight.Pending, w)
	}
}

func (q *Queue) noteLeaseMove(task *TaskBase) {
	if q == nil || task == nil || task.MemWeight <= 0 {
		return
	}
	if q.memWeight != nil {
		q.memWeight.Move(memweight.Pending, memweight.InFlight, task.MemWeight)
	}
}

// NoteDiscoveredChildren grows in-flight weight by n after a listing/compare fills DiscoveredChildren.
func (q *Queue) NoteDiscoveredChildren(task *TaskBase, n int) {
	if q == nil || task == nil || n <= 0 {
		return
	}
	w := int64(n)
	task.MemWeight += w
	if q.memWeight != nil {
		q.memWeight.Add(memweight.InFlight, w)
	}
}

// ReleaseTaskInFlightWeight drops the attempt weight from inFlight and clears MemWeight.
func (q *Queue) ReleaseTaskInFlightWeight(task *TaskBase) {
	if q == nil || task == nil || task.MemWeight <= 0 {
		return
	}
	w := task.MemWeight
	task.MemWeight = 0
	if q.memWeight != nil {
		q.memWeight.Release(memweight.InFlight, w)
	}
}

// releasePendingBufferWeight drops weight for every task currently in pendingBuff (caller holds q.mu).
func (q *Queue) releasePendingBufferWeightLocked() {
	if q == nil || q.memWeight == nil {
		return
	}
	var total int64
	for _, t := range q.pendingBuff {
		if t == nil || t.MemWeight <= 0 {
			continue
		}
		total += t.MemWeight
		t.MemWeight = 0
	}
	if total > 0 {
		q.memWeight.Release(memweight.Pending, total)
	}
}
