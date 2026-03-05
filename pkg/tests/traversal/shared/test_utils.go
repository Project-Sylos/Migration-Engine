// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package shared

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

// CountSubtree returns aggregate counts for the subtree at rootPath using a single SQL query (path prefix). Uses db.CountSubtree.
func CountSubtree(database *db.DB, queueType string, rootPath string) (db.SubtreeStats, error) {
	return db.CountSubtree(database, queueType, rootPath)
}

// DeleteSubtree deletes all nodes in the subtree at rootPath and recomputes stats for affected depths. Uses Writer.DeleteSubtree.
func DeleteSubtree(database *db.DB, queueType string, rootPath string) error {
	return database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			return w.DeleteSubtree(queueType, rootPath)
		})
	})
}

// MarkNodeAsPending marks a node as pending in the database (direct live-table update + stats recompute).
func MarkNodeAsPending(database *db.DB, queueType string, nodePath string) error {
	nodeState, err := db.GetNodeByPath(database, queueType, nodePath)
	if err != nil {
		return fmt.Errorf("failed to find node: %w", err)
	}
	if nodeState == nil {
		return fmt.Errorf("node not found: %s", nodePath)
	}
	return database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			return w.SetNodeTraversalStatus(queueType, nodeState.ID, db.StatusPending)
		})
	})
}

// MarkNodeAsFailed marks a node as failed in the database (direct live-table update + stats recompute).
func MarkNodeAsFailed(database *db.DB, queueType string, nodePath string) error {
	nodeState, err := db.GetNodeByPath(database, queueType, nodePath)
	if err != nil {
		return fmt.Errorf("failed to find node: %w", err)
	}
	if nodeState == nil {
		return fmt.Errorf("node not found: %s", nodePath)
	}
	return database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			return w.SetNodeTraversalStatus(queueType, nodeState.ID, db.StatusFailed)
		})
	})
}

// MarkNodeAsExcluded marks a node as excluded in the database (direct live-table update).
func MarkNodeAsExcluded(database *db.DB, queueType string, nodePath string) error {
	nodeState, err := db.GetNodeByPath(database, queueType, nodePath)
	if err != nil {
		return fmt.Errorf("failed to find node: %w", err)
	}
	if nodeState == nil {
		return fmt.Errorf("node not found: %s", nodePath)
	}
	return database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			return w.SetNodeExcluded(queueType, nodeState.ID, true)
		})
	})
}

// MarkNodeAsUnexcluded marks a node as not excluded in the database (direct live-table update).
func MarkNodeAsUnexcluded(database *db.DB, queueType string, nodePath string) error {
	nodeState, err := db.GetNodeByPath(database, queueType, nodePath)
	if err != nil {
		return fmt.Errorf("failed to find node: %w", err)
	}
	if nodeState == nil {
		return fmt.Errorf("node not found: %s", nodePath)
	}
	return database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			return w.SetNodeExcluded(queueType, nodeState.ID, false)
		})
	})
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
	rootNode, err := db.GetNodeByPath(database, queueType, rootPath)
	if err != nil {
		return nil, fmt.Errorf("failed to find root node: %w", err)
	}
	if rootNode == nil {
		return nil, fmt.Errorf("node not found: %s", rootPath)
	}
	return db.GetChildrenByParentPath(database, queueType, rootPath, 40_000)
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
	levels, err := db.GetAllLevels(database, queueType)
	if err != nil {
		return 0, err
	}

	totalPending := 0
	for _, level := range levels {
		c, err := database.GetStatsCountAtDepth(queueType, level, db.StatsKeyTraversalStatus(db.StatusPending))
		if err != nil {
			continue
		}
		totalPending += int(c)
	}

	return totalPending, nil
}

// CountExcludedNodes counts all excluded nodes in the table (excluded = true).
func CountExcludedNodes(database *db.DB, queueType string) (int, error) {
	return db.CountExcluded(database, queueType)
}

// CountExcludedInSubtree counts excluded nodes within the subtree at rootPath (single SQL query).
func CountExcludedInSubtree(database *db.DB, queueType string, rootPath string) (int, error) {
	return db.CountExcludedInSubtree(database, queueType, rootPath)
}
