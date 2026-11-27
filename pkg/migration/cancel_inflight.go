// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

// CancelInFlightResult is the Progress Monitor soft-cancel outcome for a copy-file lease.
type CancelInFlightResult struct {
	OK      bool   `json:"ok"`
	Result  string `json:"result"`
	Message string `json:"message,omitempty"`
}

const (
	CancelInFlightCancelled        = "cancelled"
	CancelInFlightAlreadySucceeded = "already_succeeded"
	CancelInFlightAlreadyFailed    = "already_failed"
	CancelInFlightNotInFlight      = "not_inflight"
)

// CancelInFlightCopy soft-cancels a live copy-file lease. Stale clicks return
// already_succeeded / already_failed / not_inflight without mutating status.
func (m *Migration) CancelInFlightCopy(nodeID string) CancelInFlightResult {
	if m == nil || nodeID == "" {
		return CancelInFlightResult{
			OK:      false,
			Result:  CancelInFlightNotInFlight,
			Message: "No longer in progress",
		}
	}
	o := m.activeQueueObs.Load()
	if o == nil {
		return m.cancelInFlightFromStatus(nodeID)
	}
	if o.RequestUserCancel(nodeID) {
		return CancelInFlightResult{
			OK:      true,
			Result:  CancelInFlightCancelled,
			Message: "Cancel requested",
		}
	}
	return m.cancelInFlightFromStatus(nodeID)
}

func (m *Migration) cancelInFlightFromStatus(nodeID string) CancelInFlightResult {
	notInFlight := CancelInFlightResult{
		OK:      false,
		Result:  CancelInFlightNotInFlight,
		Message: "No longer in progress",
	}
	if m == nil || m.DB == nil || nodeID == "" {
		return notInFlight
	}
	st, ok, err := m.DB.Ops().GetStatus(opsdb.SideSRC, nodeID)
	if err != nil || !ok {
		return notInFlight
	}
	switch st.CopyStatus {
	case db.CopyStatusSuccessful, db.CopyStatusAlreadyExisted:
		return CancelInFlightResult{
			OK:      false,
			Result:  CancelInFlightAlreadySucceeded,
			Message: "Task already completed",
		}
	case db.CopyStatusFailed, db.CopyStatusExcludedExplicit, db.CopyStatusExcludedInherited:
		return CancelInFlightResult{
			OK:      false,
			Result:  CancelInFlightAlreadyFailed,
			Message: "Task already failed",
		}
	default:
		return notInFlight
	}
}
