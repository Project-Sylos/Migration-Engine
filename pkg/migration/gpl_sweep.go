// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/go-path-linter/pkg/gpl"
)

// RunGPLSweep processes gpl_status=pending nodes level-by-level (BFS), refreshing
// path-scoped GPL findings on SRC and acknowledging DST pending rows.
// Call after AcceptPathProposal / RemapPathManual so descendant path-length issues stay correct.
// No-op when source and destination are the same service type (path checks not required).
func (m *Migration) RunGPLSweep() error {
	if m == nil || m.DB == nil {
		return fmt.Errorf("migration db not open")
	}
	if !m.PathChecksEnabled() {
		return nil
	}
	target := m.dstGPLTarget()
	if err := runGPLSweepSide(m.DB, "SRC", target); err != nil {
		return err
	}
	return runGPLSweepSide(m.DB, "DST", target)
}

func runGPLSweepSide(database *db.DB, side string, target gpl.Target) error {
	maxDepth, err := database.GetMaxDepth(side)
	if err != nil {
		return err
	}
	const batch = 1000
	for depth := 0; depth <= maxDepth; depth++ {
		after := ""
		for {
			rows, err := db.ListNodesGPLKeyset(database, side, depth, after, db.GPLStatusPending, batch)
			if err != nil {
				return err
			}
			if len(rows) == 0 {
				break
			}
			for _, fr := range rows {
				if err := processGPLSweepRow(database, side, target, fr, depth); err != nil {
					return err
				}
			}
			after = rows[len(rows)-1].Key
			if len(rows) < batch {
				break
			}
		}
	}
	return nil
}

func processGPLSweepRow(database *db.DB, side string, target gpl.Target, fr db.FetchResult, depth int) error {
	if fr.State == nil {
		return nil
	}
	status := db.GPLStatusSuccessful
	if side == "SRC" {
		task := &queue.TaskBase{
			ID:              fr.State.ID,
			Type:            queue.TaskTypeGPL,
			Round:           depth,
			GPLState:        fr.State.GPLState,
			ParentGPLState:  fr.ParentGPLState,
			ResolvedDstName: fr.ResolvedDstPath,
		}
		name := fr.State.Name
		if name == "" {
			name = db.NormalizeNodeBasename(fr.State.Path)
		}
		if fr.State.Type == db.NodeTypeFile {
			task.File.DisplayName = name
			task.File.LocationPath = fr.State.Path
			task.File.DepthLevel = fr.State.Depth
		} else {
			task.Folder.DisplayName = name
			task.Folder.LocationPath = fr.State.Path
			task.Folder.DepthLevel = fr.State.Depth
		}
		if err := queue.ProcessGPLTaskSRC(database, target, task); err != nil {
			status = db.GPLStatusFailed
		}
	}
	return database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			ev := &db.StatusEvent{
				ID:            fr.State.ID,
				GPLStatus:     status,
				EventTime:     time.Now().UnixNano(),
				Depth:         fr.State.Depth,
				PrevGPLStatus: db.GPLStatusPending,
			}
			return w.InsertStatusEvent(side, ev)
		})
	})
}
