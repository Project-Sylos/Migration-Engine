// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

const (
	PathRenamePhaseIdle      = ""
	PathRenamePhaseUpdating  = "updating"
	PathRenamePhaseReverting = "reverting"
	PathRenamePhaseGPLSweep  = "name-check"
	PathRenamePhaseDSTRename = "dst-rename"
	PathRenamePhaseRefresh   = "refresh"
)

// PathRenameProgress is a snapshot of an in-flight AcceptPathChange / remap cascade.
type PathRenameProgress struct {
	Active      bool   `json:"active"`
	Phase       string `json:"phase"` // updating | reverting | name-check | dst-rename | refresh
	Done        int64  `json:"done"`
	Total       int64  `json:"total"`
	NodeID      string `json:"nodeId,omitempty"`
	Message     string `json:"message,omitempty"`
	CanSoftStop bool   `json:"canSoftStop"`
	CanHardStop bool   `json:"canHardStop"`
}

type pathRenameJob struct {
	mu sync.Mutex

	active bool
	phase  string
	done   atomic.Int64
	total  atomic.Int64
	nodeID string
	msg    string

	softStop atomic.Bool
	hardStop atomic.Bool

	// Snapshot for soft-revert after a partial update.
	side         string
	oldName      string
	newName      string
	oldServiceID string
	newServiceID string
	fsRenamed    bool
}

func (m *Migration) pathRename() *pathRenameJob {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pathRenameJob == nil {
		m.pathRenameJob = &pathRenameJob{}
	}
	return m.pathRenameJob
}

// PathRenameProgressSnapshot returns live cascade progress for Path Review UI polling.
func (m *Migration) PathRenameProgressSnapshot() PathRenameProgress {
	j := m.pathRename()
	if j == nil {
		return PathRenameProgress{}
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.active {
		return PathRenameProgress{}
	}
	phase := j.phase
	return PathRenameProgress{
		Active:      true,
		Phase:       phase,
		Done:        j.done.Load(),
		Total:       j.total.Load(),
		NodeID:      j.nodeID,
		Message:     j.msg,
		CanSoftStop: phase == PathRenamePhaseUpdating,
		CanHardStop: phase == PathRenamePhaseReverting,
	}
}

// RequestPathRenameSoftStop asks an in-flight updating cascade to stop and revert.
func (m *Migration) RequestPathRenameSoftStop() error {
	j := m.pathRename()
	if j == nil {
		return fmt.Errorf("no path rename in progress")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.active || j.phase != PathRenamePhaseUpdating {
		return fmt.Errorf("soft stop only available while updating related paths")
	}
	j.softStop.Store(true)
	j.msg = "Stopping and reverting rename…"
	return nil
}

// RequestPathRenameHardStop asks an in-flight revert to abort immediately (may leave partial paths).
func (m *Migration) RequestPathRenameHardStop() error {
	j := m.pathRename()
	if j == nil {
		return fmt.Errorf("no path rename in progress")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.active || j.phase != PathRenamePhaseReverting {
		return fmt.Errorf("hard stop only available while reverting a cancelled rename")
	}
	j.hardStop.Store(true)
	j.msg = "Hard-stopping revert…"
	return nil
}

func (j *pathRenameJob) setProgress(done, total int64) {
	j.done.Store(done)
	if total > 0 {
		j.total.Store(total)
	}
}

// beginPhase switches the visible post-cascade step and resets counters.
func (j *pathRenameJob) beginPhase(phase, msg string, total int64) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.active {
		return
	}
	j.phase = phase
	j.msg = msg
	j.done.Store(0)
	if total > 0 {
		j.total.Store(total)
	} else {
		j.total.Store(0)
	}
	j.softStop.Store(false)
	j.hardStop.Store(false)
}

func (j *pathRenameJob) isActive() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.active
}

// PathRenameBeginPhase updates the live progress job when AcceptPathChange is in the post-cascade steps.
func (m *Migration) PathRenameBeginPhase(phase, msg string, total int64) {
	j := m.pathRename()
	if j == nil || !j.isActive() {
		return
	}
	j.beginPhase(phase, msg, total)
}

// PathRenameSetProgress updates done/total on the live progress job.
func (m *Migration) PathRenameSetProgress(done, total int64) {
	j := m.pathRename()
	if j == nil || !j.isActive() {
		return
	}
	j.setProgress(done, total)
}

func (j *pathRenameJob) end() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.active = false
	j.phase = PathRenamePhaseIdle
	j.msg = ""
	j.nodeID = ""
	j.done.Store(0)
	j.total.Store(0)
	j.softStop.Store(false)
	j.hardStop.Store(false)
	j.fsRenamed = false
}

// EndPathRenameProgress clears an in-flight rename progress job (cascade + finishing).
func (m *Migration) EndPathRenameProgress() {
	j := m.pathRename()
	if j == nil {
		return
	}
	j.end()
}

// ErrPathRenameSoftStopped is returned when soft-stop completed a full revert.
var ErrPathRenameSoftStopped = errors.New("path rename cancelled and reverted")

// ErrPathRenameHardStopped is returned when hard-stop aborted mid-revert.
var ErrPathRenameHardStopped = errors.New("path rename hard-stopped; some related paths may be inconsistent")
