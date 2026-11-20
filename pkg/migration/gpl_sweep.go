// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	queuegpl "codeberg.org/Sylos/Migration-Engine/pkg/queue/gpl"
	"codeberg.org/Sylos/go-path-linter/pkg/gpl"
)

// RunGPLSweep revalidates gpl_status=pending nodes under rootPath via idx:path order
// (parents before descendants). Empty rootPath means the whole tree ("/").
// Call after AcceptPathChange so descendant path-length issues stay correct.
func (m *Migration) RunGPLSweep(rootPath string) error {
	if m == nil || m.DB == nil {
		return fmt.Errorf("migration db not open")
	}
	if !m.PathChecksEnabled() {
		return nil
	}
	if rootPath == "" {
		rootPath = "/"
	}
	target := m.dstGPLTarget()
	compat := m.WindowsCompat()
	if err := m.runGPLSweepSide(opsdb.SideSRC, rootPath, "Rechecking destination names (source)…", target, compat); err != nil {
		return err
	}
	return m.runGPLSweepSide(opsdb.SideDST, rootPath, "Rechecking destination names (destination)…", target, compat)
}

func (m *Migration) runGPLSweepSide(side, rootPath, label string, target gpl.Target, windowsCompat bool) error {
	ops := m.DB.Ops()
	if ops == nil {
		return fmt.Errorf("ops store not open")
	}
	total := gplSweepProgressTotal(ops, side, rootPath)
	m.PathRenameBeginPhase(PathRenamePhaseGPLSweep, label, total)
	m.PathRenameSetProgress(0, total)

	var done int64
	// Fresh gpl_state after we revalidate a node, so children in later pages (and
	// later in the same page) see the updated parent path_len.
	gplStateByID := map[string]string{}

	err := ops.ApplySubtreeScan(side, rootPath, opsdb.SubtreeScanOpts{}, 512, func(chunk opsdb.SubtreeChunk) error {
		parentIDs := make([]string, 0, 32)
		seenParent := map[string]struct{}{}
		for _, id := range chunk.IDs {
			n, ok := chunk.Nodes[id]
			if !ok {
				continue
			}
			st := chunk.Status[id]
			if st.GPLStatus != db.GPLStatusPending {
				if n.GPLState != "" {
					gplStateByID[id] = n.GPLState
				}
				continue
			}
			if n.ParentID != "" {
				if _, ok := gplStateByID[n.ParentID]; !ok {
					if _, seen := seenParent[n.ParentID]; !seen {
						seenParent[n.ParentID] = struct{}{}
						parentIDs = append(parentIDs, n.ParentID)
					}
				}
			}
		}
		if len(parentIDs) > 0 {
			parents, err := ops.BatchGetNode(side, parentIDs)
			if err != nil {
				return err
			}
			for id, p := range parents {
				if _, ok := gplStateByID[id]; !ok {
					gplStateByID[id] = p.GPLState
				}
			}
		}

		var srcIDs []string
		if side == opsdb.SideSRC {
			for _, id := range chunk.IDs {
				if chunk.Status[id].GPLStatus == db.GPLStatusPending {
					srcIDs = append(srcIDs, id)
				}
			}
		}
		gplByID, err := ops.BatchGetGPL(srcIDs)
		if err != nil {
			return err
		}

		for _, id := range chunk.IDs {
			n, ok := chunk.Nodes[id]
			if !ok {
				continue
			}
			st := chunk.Status[id]
			if st.GPLStatus != db.GPLStatusPending {
				continue
			}
			status := db.GPLStatusSuccessful
			if side == opsdb.SideSRC {
				parentGPL := gplStateByID[n.ParentID]
				task := &queue.TaskBase{
					ID:              id,
					Type:            queue.TaskTypeGPL,
					Round:           n.Depth,
					GPLState:        n.GPLState,
					ParentGPLState:  parentGPL,
					ResolvedDstName: gplByID[id].ProposedName,
				}
				name := n.Name
				if name == "" {
					name = db.NormalizeNodeBasename(n.Path)
				}
				if n.Type == opsdb.NodeTypeFile {
					task.File.DisplayName = name
					task.File.LocationPath = n.Path
					task.File.DepthLevel = n.Depth
				} else {
					task.Folder.DisplayName = name
					task.Folder.LocationPath = n.Path
					task.Folder.DepthLevel = n.Depth
				}
				merged, err := queuegpl.ProcessGPLTaskSRC(m.DB, target, task, windowsCompat)
				if err != nil {
					status = db.GPLStatusFailed
				} else if merged != "" {
					gplStateByID[id] = merged
				}
			} else if n.GPLState != "" {
				gplStateByID[id] = n.GPLState
			}
			m.DB.AppendStatusEvent(side, db.StatusEvent{
				ID:            id,
				GPLStatus:     status,
				EventTime:     time.Now().UnixNano(),
				Depth:         n.Depth,
				PrevGPLStatus: db.GPLStatusPending,
			}, false)
			done++
			if done%16 == 0 {
				m.PathRenameSetProgress(done, total)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	m.PathRenameSetProgress(done, total)
	return nil
}

func gplSweepProgressTotal(ops *opsdb.Store, side, rootPath string) int64 {
	if ops == nil || rootPath == "" || rootPath == "/" {
		return 0
	}
	id, err := ops.GetNodeIDByPath(side, rootPath)
	if err != nil || id == "" {
		return 0
	}
	st, ok, err := ops.GetStatus(side, id)
	if err != nil || !ok {
		return 0
	}
	if st.ChildCount > 0 {
		return st.ChildCount
	}
	return 0
}

// RunGPLSweepForNode revalidates pending GPL under the node's id_path.
func (m *Migration) RunGPLSweepForNode(nodeID string) error {
	if m == nil || m.DB == nil {
		return fmt.Errorf("migration db not open")
	}
	node, err := pull.GetNodeByID(m.DB, "SRC", nodeID)
	if err != nil {
		return err
	}
	if node == nil {
		return fmt.Errorf("node %s not found", nodeID)
	}
	return m.RunGPLSweep(node.Path)
}
