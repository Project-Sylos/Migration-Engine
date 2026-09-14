// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"encoding/json"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
)

func jsonMapInt(m map[string]any, key string) (int, bool) {
	if m == nil {
		return 0, false
	}
	v, ok := m[key]
	if !ok || v == nil {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return int(n), true
	case int:
		return n, true
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			return 0, false
		}
		return int(i), true
	default:
		return 0, false
	}
}

func suspendV1Object(runtimeJSON string) map[string]any {
	if runtimeJSON == "" || runtimeJSON == "{}" {
		return nil
	}
	var top map[string]any
	if err := json.Unmarshal([]byte(runtimeJSON), &top); err != nil {
		return nil
	}
	raw, ok := top[runtimeSuspendJSONKey]
	if !ok || raw == nil {
		return nil
	}
	obj, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	return obj
}

func latestQueueStatsRound(database *db.DB, queueKey string) int {
	raw, err := stats.GetLatestQueueStats(database, queueKey, db.QueueStatsPhaseTraversal)
	if err != nil || len(raw) == 0 {
		return 0
	}
	var st struct {
		Round int `json:"round"`
	}
	if err := json.Unmarshal(raw, &st); err != nil {
		return 0
	}
	return st.Round
}

func phaseBlocksSuspendRepair(phase string) bool {
	return phase == PhaseTraversing || phase == PhaseCopying || phase == PhaseDeleting ||
		phase == PhaseTraversalFinalizing || phase == PhaseCopyFinalizing || phase == PhaseDeleteFinalizing
}

func loadRepairMigrationRow(database *db.DB, migrationID string) (id, phase, runtimeJSON string, err error) {
	ops := database.Ops()
	if ops == nil {
		return "", "", "", fmt.Errorf("ops store required")
	}
	if migrationID != "" {
		rec, ok, err := ops.GetMigrationMeta(migrationID)
		if err != nil {
			return "", "", "", fmt.Errorf("load migration %s: %w", migrationID, err)
		}
		if !ok {
			return "", "", "", fmt.Errorf("load migration %s: not found", migrationID)
		}
		return rec.MigrationID, rec.Phase, rec.RuntimeStateJSON, nil
	}
	all, err := ops.ListMigrationMeta()
	if err != nil {
		return "", "", "", err
	}
	if len(all) == 0 {
		return "", "", "", fmt.Errorf("no migrations row")
	}
	if len(all) > 1 {
		return "", "", "", fmt.Errorf("multiple migrations in db; pass -id")
	}
	return all[0].MigrationID, all[0].Phase, all[0].RuntimeStateJSON, nil
}

// RepairTraversalSuspendV1 rewrites suspend_v1 last rounds and keyset cursors for a paused
// traversal so Resume continues that round instead of walking from 0. Does not change phase.
func RepairTraversalSuspendV1(database *db.DB, srcFallbackRound int, migrationID string) (RuntimeSuspendV1, error) {
	if database == nil {
		return RuntimeSuspendV1{}, fmt.Errorf("database required")
	}
	if srcFallbackRound < 0 {
		srcFallbackRound = 7
	}
	id, phase, runtimeJSON, err := loadRepairMigrationRow(database, migrationID)
	if err != nil {
		return RuntimeSuspendV1{}, err
	}
	if phaseBlocksSuspendRepair(phase) {
		return RuntimeSuspendV1{}, fmt.Errorf("migration %s is live (%s); stop it first", id, phase)
	}
	switch phase {
	case PhaseTraversalSuspended, PhaseTraversalReview, PhaseTraversalFinalizeFailed:
	default:
		return RuntimeSuspendV1{}, fmt.Errorf("migration %s phase %s; want traversal-suspended or awaiting-traversal-review", id, phase)
	}

	s, ok := parseRuntimeSuspendV1(runtimeJSON)
	if !ok {
		s = RuntimeSuspendV1{Version: 1, Kind: "traversal"}
	}
	s.Version = 1
	s.Kind = "traversal"
	if s.SuspendedAtUnix == 0 {
		s.SuspendedAtUnix = time.Now().Unix()
	}

	raw := suspendV1Object(runtimeJSON)
	savedSrc := srcFallbackRound
	if n, present := jsonMapInt(raw, "last_round_src"); present && n > 0 {
		savedSrc = n
	}
	savedDst := 0
	if n, present := jsonMapInt(raw, "last_round_dst"); present {
		savedDst = n
	}
	s.LastRoundSrc = resumeRoundForSide(database, "SRC", savedSrc)
	s.LastRoundDst = resumeRoundForSide(database, "DST", savedDst)
	if s.LastRoundDst == 0 {
		s.LastRoundDst = latestQueueStatsRound(database, "dst-traversal")
		s.LastRoundDst = resumeRoundForSide(database, "DST", s.LastRoundDst)
	}
	s.SrcKeysetCursor = keysetCursorBeforeFirstPendingFolder(database, "SRC", s.LastRoundSrc)
	s.DstKeysetCursor = keysetCursorBeforeFirstPendingFolder(database, "DST", s.LastRoundDst)

	store := newMigrationStore(database, nil)
	patch, err := suspendRuntimeMergePatch(s)
	if err != nil {
		return RuntimeSuspendV1{}, err
	}
	if err := store.updateRuntimeState(id, patch); err != nil {
		return RuntimeSuspendV1{}, err
	}
	if phase != PhaseTraversalSuspended {
		if err := store.updateMigrationField(id, "phase", PhaseTraversalSuspended, "repair-suspend-phase"); err != nil {
			return RuntimeSuspendV1{}, err
		}
	}
	return s, nil
}
