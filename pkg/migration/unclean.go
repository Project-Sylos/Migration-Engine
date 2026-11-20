// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"encoding/json"
	"fmt"
)

const runtimeUncleanJSONKey = "unclean_interrupt"

// UncleanInterruptV1 marks a migration that stopped without a soft-stop checkpoint
// (crash, force-stop, or missing suspend_v1). Resume runs a recovery sweep.
type UncleanInterruptV1 struct {
	Active bool   `json:"active"`
	Kind   string `json:"kind,omitempty"` // traversal | copy | delete
}

// HasUncleanInterrupt reports whether recovery mode is active.
func (m *Migration) HasUncleanInterrupt() bool {
	u, ok := parseUncleanInterruptV1(m.runtimeStateJSON())
	return ok && u.Active
}

// UncleanInterruptKind returns the interrupted phase kind when unclean is active.
func (m *Migration) UncleanInterruptKind() string {
	u, ok := parseUncleanInterruptV1(m.runtimeStateJSON())
	if !ok || !u.Active {
		return ""
	}
	return u.Kind
}

// SetUncleanInterrupt persists unclean_interrupt=true for the given kind.
func (m *Migration) SetUncleanInterrupt(kind string) error {
	if m == nil || m.store == nil {
		return fmt.Errorf("migration store unavailable")
	}
	patch, err := uncleanRuntimeMergePatch(UncleanInterruptV1{Active: true, Kind: kind})
	if err != nil {
		return err
	}
	if err := m.store.updateRuntimeState(m.ID, patch); err != nil {
		return err
	}
	m.refreshRuntimeState()
	return nil
}

// ClearUncleanInterrupt clears the recovery flag after the recovery round completes.
func (m *Migration) ClearUncleanInterrupt() error {
	if m == nil || m.store == nil {
		return nil
	}
	patch, err := uncleanRuntimeMergePatch(UncleanInterruptV1{Active: false})
	if err != nil {
		return err
	}
	if err := m.store.updateRuntimeState(m.ID, patch); err != nil {
		return err
	}
	m.refreshRuntimeState()
	return nil
}

func parseUncleanInterruptV1(runtimeStateJSON string) (UncleanInterruptV1, bool) {
	if runtimeStateJSON == "" || runtimeStateJSON == "{}" {
		return UncleanInterruptV1{}, false
	}
	var top map[string]any
	if err := json.Unmarshal([]byte(runtimeStateJSON), &top); err != nil {
		return UncleanInterruptV1{}, false
	}
	raw, ok := top[runtimeUncleanJSONKey]
	if !ok || raw == nil {
		return UncleanInterruptV1{}, false
	}
	inner, err := json.Marshal(raw)
	if err != nil {
		return UncleanInterruptV1{}, false
	}
	var u UncleanInterruptV1
	if err := json.Unmarshal(inner, &u); err != nil {
		return UncleanInterruptV1{}, false
	}
	return u, true
}

func uncleanRuntimeMergePatch(u UncleanInterruptV1) (string, error) {
	asMap := map[string]any{
		"active": u.Active,
	}
	if u.Kind != "" {
		asMap["kind"] = u.Kind
	}
	merged := map[string]any{runtimeUncleanJSONKey: asMap}
	raw, err := json.Marshal(merged)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func uncleanKindForPhase(phase string) string {
	switch phase {
	case PhaseTraversing, PhaseTraversalSuspended, PhaseTraversalFinalizing, PhaseTraversalFinalizeFailed:
		return "traversal"
	case PhaseCopying, PhaseCopySuspended, PhaseCopyFinalizing, PhaseCopyFinalizeFailed:
		return "copy"
	case PhaseDeleting, PhaseDeleteSuspended, PhaseDeleteFinalizing, PhaseDeleteFinalizeFailed:
		return "delete"
	default:
		return ""
	}
}

func suspendedPhaseForKind(kind string) string {
	switch kind {
	case "traversal":
		return PhaseTraversalSuspended
	case "copy":
		return PhaseCopySuspended
	case "delete":
		return PhaseDeleteSuspended
	default:
		return ""
	}
}

func suspendedPhaseForInProgress(phase string) string {
	switch phase {
	case PhaseTraversing:
		return PhaseTraversalSuspended
	case PhaseCopying:
		return PhaseCopySuspended
	case PhaseDeleting:
		return PhaseDeleteSuspended
	case PhaseAborted:
		return "" // caller must use unclean kind or infer
	default:
		return ""
	}
}
