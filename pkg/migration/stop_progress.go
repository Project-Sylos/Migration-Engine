// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"
	"time"
)

// Stop step ids for soft / force stop progress.
const (
	StopStepDraining = "draining"
	StopStepSaving   = "saving"
	StopStepDone     = "done"
	StopStepAborted  = "aborted"
)

// StopStepStatus is pending, active, done, skipped, or failed.
type StopStepStatus string

const (
	StopStatusPending StopStepStatus = "pending"
	StopStatusActive  StopStepStatus = "active"
	StopStatusDone    StopStepStatus = "done"
	StopStatusSkipped StopStepStatus = "skipped"
	StopStatusFailed  StopStepStatus = "failed"
)

// StopStep is one checklist row for the stop UI.
type StopStep struct {
	ID     string         `json:"id"`
	Label  string         `json:"label"`
	Status StopStepStatus `json:"status"`
}

// StopProgress is the live soft/force stop projection for API/UI.
type StopProgress struct {
	Active     bool       `json:"active"`
	Mode       string     `json:"mode,omitempty"` // soft | force
	Step       string     `json:"step,omitempty"`
	Label      string     `json:"label,omitempty"`
	Detail     string     `json:"detail,omitempty"` // live wait reason (in-flight / DB)
	InProgress int        `json:"inProgress,omitempty"`
	Steps      []StopStep `json:"steps,omitempty"`
}

func softStopChecklist(phase string) []StopStep {
	drainLabel := "Finishing current work"
	switch phase {
	case PhaseDeleting:
		drainLabel = "Finishing current removals"
	case PhaseCopying:
		drainLabel = "Finishing current copies"
	case PhaseTraversing:
		drainLabel = "Finishing current folders and files"
	}
	return []StopStep{
		{ID: StopStepDraining, Label: drainLabel, Status: StopStatusPending},
		{ID: StopStepSaving, Label: "Saving your progress", Status: StopStatusPending},
		{ID: StopStepDone, Label: "Stopped", Status: StopStatusPending},
	}
}

func (m *Migration) clearStopProgress() {
	if m == nil {
		return
	}
	m.stopProgressMu.Lock()
	defer m.stopProgressMu.Unlock()
	m.stopProgress = StopProgress{}
}

// GetStopProgress returns a copy of the current stop progress (empty if inactive).
// Merges live DB / observer wait labels into Detail when set.
func (m *Migration) GetStopProgress() StopProgress {
	if m == nil {
		return StopProgress{}
	}
	m.stopProgressMu.Lock()
	out := cloneStopProgress(m.stopProgress)
	m.stopProgressMu.Unlock()
	if !out.Active {
		return out
	}
	if out.Detail == "" {
		if o := m.activeQueueObs.Load(); o != nil {
			out.Detail = o.WaitReason()
		}
	}
	if out.Detail == "" && m.DB != nil {
		out.Detail = m.DB.ActivityLabel()
	}
	return out
}

func cloneStopProgress(p StopProgress) StopProgress {
	out := p
	if len(p.Steps) > 0 {
		out.Steps = make([]StopStep, len(p.Steps))
		copy(out.Steps, p.Steps)
	}
	return out
}

func (m *Migration) beginSoftStopProgress(phase string) {
	m.stopProgressMu.Lock()
	defer m.stopProgressMu.Unlock()
	steps := softStopChecklist(phase)
	steps[0].Status = StopStatusActive
	m.stopProgress = StopProgress{
		Active: true,
		Mode:   "soft",
		Step:   StopStepDraining,
		Label:  steps[0].Label,
		Detail: "Waiting for the run loop to begin soft stop…",
		Steps:  steps,
	}
}

func (m *Migration) beginForceStopProgress() {
	m.stopProgressMu.Lock()
	defer m.stopProgressMu.Unlock()
	m.stopProgress = StopProgress{
		Active: true,
		Mode:   "force",
		Step:   StopStepAborted,
		Label:  "Force stopping migration",
		Steps: []StopStep{
			{ID: StopStepAborted, Label: "Force stopping (may not resume)", Status: StopStatusActive},
		},
	}
}

// setStopStepDetail marks stepID active, earlier steps done, and updates label/inProgress/detail.
func (m *Migration) setStopStepDetail(stepID string, inProgress int, detail string) {
	if m == nil {
		return
	}
	m.stopProgressMu.Lock()
	defer m.stopProgressMu.Unlock()
	if !m.stopProgress.Active && stepID != StopStepAborted {
		return
	}
	m.stopProgress.InProgress = inProgress
	m.stopProgress.Step = stepID
	if detail != "" {
		m.stopProgress.Detail = detail
	}
	activeIdx := -1
	for i := range m.stopProgress.Steps {
		if m.stopProgress.Steps[i].ID == stepID {
			activeIdx = i
			break
		}
	}
	if activeIdx < 0 && stepID == StopStepAborted {
		m.stopProgress.Mode = "force"
		m.stopProgress.Active = true
		m.stopProgress.Label = "Force stopped"
		m.stopProgress.Detail = ""
		m.stopProgress.Steps = []StopStep{
			{ID: StopStepAborted, Label: "Force stopped", Status: StopStatusDone},
		}
		m.stopProgress.InProgress = 0
		return
	}
	for i := range m.stopProgress.Steps {
		s := &m.stopProgress.Steps[i]
		switch {
		case i < activeIdx:
			if s.Status != StopStatusSkipped && s.Status != StopStatusFailed {
				s.Status = StopStatusDone
			}
		case i == activeIdx:
			if stepID == StopStepDone || stepID == StopStepAborted {
				s.Status = StopStatusDone
				m.stopProgress.Label = s.Label
			} else {
				s.Status = StopStatusActive
				m.stopProgress.Label = s.Label
			}
		default:
			if s.Status == StopStatusActive {
				s.Status = StopStatusPending
			}
		}
	}
	if stepID == StopStepDone {
		m.stopProgress.Label = "Stopped"
		m.stopProgress.Detail = ""
		m.stopProgress.InProgress = 0
	}
	if stepID == StopStepSaving && detail == "" {
		m.stopProgress.Detail = "Saving your progress…"
	}
}

func (m *Migration) setStopWaitDetail(detail string) {
	if m == nil {
		return
	}
	m.stopProgressMu.Lock()
	if m.stopProgress.Active {
		m.stopProgress.Detail = detail
	}
	m.stopProgressMu.Unlock()
	if o := m.activeQueueObs.Load(); o != nil {
		o.SetWaitReason(detail)
	}
}

func (m *Migration) reportStopProgress(step string, inProgress int) {
	detail := ""
	if o := m.activeQueueObs.Load(); o != nil {
		detail = o.WaitReason()
	}
	if detail == "" && m.DB != nil {
		detail = m.DB.ActivityLabel()
	}
	m.setStopStepDetail(step, inProgress, detail)
}

func (m *Migration) pauseLiveQueues() int {
	if m == nil {
		return 0
	}
	o := m.activeQueueObs.Load()
	if o == nil {
		return 0
	}
	return o.PauseRegisteredQueues()
}

// startSoftStopProgressPump refreshes in-progress counts / wait detail until soft suspend clears.
func (m *Migration) startSoftStopProgressPump() {
	if m == nil {
		return
	}
	go func() {
		t := time.NewTicker(200 * time.Millisecond)
		defer t.Stop()
		for range t.C {
			if !m.softSuspendRequested.Load() {
				return
			}
			m.stopProgressMu.Lock()
			active := m.stopProgress.Active && m.stopProgress.Mode == "soft"
			step := m.stopProgress.Step
			m.stopProgressMu.Unlock()
			if !active {
				return
			}
			n := 0
			if o := m.activeQueueObs.Load(); o != nil {
				n = o.InProgressTotal()
			}
			detail := ""
			if m.DB != nil {
				detail = m.DB.ActivityLabel()
			}
			if detail == "" {
				if o := m.activeQueueObs.Load(); o != nil {
					detail = o.WaitReason()
				}
			}
			if step == StopStepDraining {
				if detail == "" {
					if n > 0 {
						detail = fmt.Sprintf("Finishing %d in-flight item%s", n, pluralS(n))
					} else {
						detail = "Waiting for soft-stop drain to start…"
					}
				}
				m.setStopStepDetail(StopStepDraining, n, detail)
				continue
			}
			if detail != "" {
				m.setStopWaitDetail(detail)
			}
		}
	}()
}

func pluralS(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

func drainWaitDetail(n int, kind string) string {
	if n <= 0 {
		return "No in-flight " + kind + "; preparing to save…"
	}
	return fmt.Sprintf("Finishing %d in-flight %s", n, kind)
}
