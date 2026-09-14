// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package shared

import (
	"fmt"
	"math/rand"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

// CountSubtree returns aggregate counts for the subtree at rootPath.
func CountSubtree(database *db.DB, queueType string, rootPath string) (pull.SubtreeStats, error) {
	return pull.CountSubtree(database, queueType, rootPath)
}

func opsSide(table string) string {
	if table == "DST" {
		return opsdb.SideDST
	}
	return opsdb.SideSRC
}

// DeleteSubtree deletes all nodes in the subtree at rootPath.
func DeleteSubtree(database *db.DB, queueType string, rootPath string) error {
	if database == nil || database.Ops() == nil {
		return fmt.Errorf("ops store not open")
	}
	ids, err := database.Ops().ListSubtreeIDs(opsSide(queueType), rootPath, 0)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := database.Ops().DeleteNode(opsSide(queueType), id); err != nil {
			return err
		}
	}
	return nil
}

func appendTraversalStatus(database *db.DB, queueType string, node *db.NodeState, status string) error {
	if node == nil {
		return fmt.Errorf("nil node")
	}
	database.AppendStatusEvent(queueType, db.StatusEvent{
		ID:                node.ID,
		TraversalStatus:   status,
		EventTime:         time.Now().UnixNano(),
		Depth:             node.Depth,
		PrevTraversalStatus: node.TraversalStatus,
		NodeType:          node.Type,
	}, false)
	return database.Flush(nil)
}

// MarkNodeAsPending marks a node as pending in the database.
func MarkNodeAsPending(database *db.DB, queueType string, nodePath string) error {
	nodeState, err := pull.GetNodeByPath(database, queueType, nodePath)
	if err != nil {
		return fmt.Errorf("failed to find node: %w", err)
	}
	if nodeState == nil {
		return fmt.Errorf("node not found: %s", nodePath)
	}
	return appendTraversalStatus(database, queueType, nodeState, db.StatusPending)
}

// MarkNodeAsFailed marks a node as failed in the database.
func MarkNodeAsFailed(database *db.DB, queueType string, nodePath string) error {
	nodeState, err := pull.GetNodeByPath(database, queueType, nodePath)
	if err != nil {
		return fmt.Errorf("failed to find node: %w", err)
	}
	if nodeState == nil {
		return fmt.Errorf("node not found: %s", nodePath)
	}
	return appendTraversalStatus(database, queueType, nodeState, db.StatusFailed)
}

func setCopyExcluded(database *db.DB, queueType string, node *db.NodeState, excluded bool) error {
	if queueType != "SRC" || node == nil {
		return fmt.Errorf("exclusion is SRC-only")
	}
	status := db.CopyStatusExcludedExplicit
	if !excluded {
		status = db.CopyStatusPending
	}
	database.AppendStatusEvent(queueType, db.StatusEvent{
		ID:              node.ID,
		CopyStatus:      status,
		EventTime:       time.Now().UnixNano(),
		Depth:           node.Depth,
		PrevCopyStatus:  node.CopyStatus,
		NodeType:        node.Type,
		Size:            node.Size,
	}, false)
	return database.Flush(nil)
}

// MarkNodeAsExcluded marks a node as excluded in the database.
func MarkNodeAsExcluded(database *db.DB, queueType string, nodePath string) error {
	nodeState, err := pull.GetNodeByPath(database, queueType, nodePath)
	if err != nil {
		return fmt.Errorf("failed to find node: %w", err)
	}
	if nodeState == nil {
		return fmt.Errorf("node not found: %s", nodePath)
	}
	return setCopyExcluded(database, queueType, nodeState, true)
}

// MarkNodeAsUnexcluded marks a node as not excluded in the database.
func MarkNodeAsUnexcluded(database *db.DB, queueType string, nodePath string) error {
	nodeState, err := pull.GetNodeByPath(database, queueType, nodePath)
	if err != nil {
		return fmt.Errorf("failed to find node: %w", err)
	}
	if nodeState == nil {
		return fmt.Errorf("node not found: %s", nodePath)
	}
	return setCopyExcluded(database, queueType, nodeState, false)
}

// PickRandomExcludedTopLevelChild picks a random top-level child that is currently excluded.
// Only selects folders (not files) that are excluded.
func PickRandomExcludedTopLevelChild(database *db.DB, queueType string, rootPath string) (*db.NodeState, error) {
	children, err := GetTopLevelChildren(database, queueType, rootPath)
	if err != nil {
		return nil, err
	}

	if len(children) == 0 {
		return nil, fmt.Errorf("no top-level children found")
	}

	excludedFolders := make([]*db.NodeState, 0, len(children))
	for _, child := range children {
		if child.Type == "folder" && child.Excluded {
			excludedFolders = append(excludedFolders, child)
		}
	}

	if len(excludedFolders) == 0 {
		return nil, fmt.Errorf("no excluded top-level folder children found")
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return excludedFolders[r.Intn(len(excludedFolders))], nil
}

// PickFirstExcludedTopLevelChild picks the first top-level child that is currently excluded.
// Only selects folders (not files) that are excluded. Useful for deterministic tests.
func PickFirstExcludedTopLevelChild(database *db.DB, queueType string, rootPath string) (*db.NodeState, error) {
	children, err := GetTopLevelChildren(database, queueType, rootPath)
	if err != nil {
		return nil, err
	}

	if len(children) == 0 {
		return nil, fmt.Errorf("no top-level children found")
	}

	for _, child := range children {
		if child.Type == "folder" && child.Excluded {
			return child, nil
		}
	}

	return nil, fmt.Errorf("no excluded top-level folder children found")
}

// GetTopLevelChildren returns direct children of the node at rootPath (by parent_path = rootPath).
func GetTopLevelChildren(database *db.DB, queueType string, rootPath string) ([]*db.NodeState, error) {
	rootNode, err := pull.GetNodeByPath(database, queueType, rootPath)
	if err != nil {
		return nil, fmt.Errorf("failed to find root node: %w", err)
	}
	if rootNode == nil {
		return nil, fmt.Errorf("node not found: %s", rootPath)
	}
	return pull.GetChildrenByParentPath(database, queueType, rootPath, 10_000)
}

// PickRandomTopLevelChild picks a random top-level child from the root. Only selects folders (not files).
func PickRandomTopLevelChild(database *db.DB, queueType string, rootPath string) (*db.NodeState, error) {
	children, err := GetTopLevelChildren(database, queueType, rootPath)
	if err != nil {
		return nil, err
	}

	if len(children) == 0 {
		return nil, fmt.Errorf("no top-level children found")
	}

	folders := make([]*db.NodeState, 0, len(children))
	for _, child := range children {
		if child.Type == "folder" {
			folders = append(folders, child)
		}
	}

	if len(folders) == 0 {
		return nil, fmt.Errorf("no top-level folder children found (only files)")
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return folders[r.Intn(len(folders))], nil
}

// CountPendingNodes counts all pending nodes across all levels for a queue type (from stats table).
func CountPendingNodes(database *db.DB, queueType string) (int, error) {
	levels, err := pull.GetAllLevels(database, queueType)
	if err != nil {
		return 0, err
	}

	totalPending := 0
	for _, level := range levels {
		c, err := stats.GetStatsCountAtDepth(database, queueType, level, db.StatsKey(db.StatsKindTraversal, db.StatusPending))
		if err != nil {
			continue
		}
		totalPending += int(c)
	}

	return totalPending, nil
}

// CountExcludedNodes counts all excluded nodes in the table (excluded = true).
func CountExcludedNodes(database *db.DB, queueType string) (int, error) {
	return pull.CountExcluded(database, queueType)
}

// CountExcludedInSubtree counts excluded nodes within the subtree at rootPath.
func CountExcludedInSubtree(database *db.DB, queueType string, rootPath string) (int, error) {
	return pull.CountExcludedInSubtree(database, queueType, rootPath)
}
