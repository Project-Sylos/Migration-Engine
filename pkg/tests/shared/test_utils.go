// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package shared

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/Project-Sylos/Sylos-DB/pkg/bolt"
	"github.com/Project-Sylos/Sylos-DB/pkg/store"
)

// SubtreeStats contains statistics about a subtree.
type SubtreeStats = store.SubtreeStats

// CountSubtree performs a DFS traversal to count all nodes in a subtree.
func CountSubtree(s *store.Store, queueType string, rootPath string) (SubtreeStats, error) {
	return s.CountSubtree(queueType, rootPath)
}

// DeleteSubtree deletes all nodes in a subtree (except the root node itself).
func DeleteSubtree(s *store.Store, queueType string, rootPath string) error {
	return s.DeleteSubtree(queueType, rootPath)
}

// MarkNodeAsPending marks a node as pending.
func MarkNodeAsPending(s *store.Store, queueType string, nodePath string) error {
	node, err := s.GetNodeByPath(queueType, nodePath)
	if err != nil {
		return err
	}
	return s.TransitionNodeStatus(queueType, node.Depth, node.TraversalStatus, bolt.StatusPending, node.ID)
}

// MarkNodeAsFailed marks a node as failed.
func MarkNodeAsFailed(s *store.Store, queueType string, nodePath string) error {
	node, err := s.GetNodeByPath(queueType, nodePath)
	if err != nil {
		return err
	}
	return s.TransitionNodeStatus(queueType, node.Depth, node.TraversalStatus, bolt.StatusFailed, node.ID)
}

// MarkNodeAsExcluded marks a node as explicitly excluded.
func MarkNodeAsExcluded(s *store.Store, queueType string, nodePath string) error {
	node, err := s.GetNodeByPath(queueType, nodePath)
	if err != nil {
		return err
	}
	return s.SetNodeExclusionFlag(queueType, node.ID, true)
}

// MarkNodeAsUnexcluded marks a node as not excluded.
func MarkNodeAsUnexcluded(s *store.Store, queueType string, nodePath string) error {
	node, err := s.GetNodeByPath(queueType, nodePath)
	if err != nil {
		return err
	}
	return s.SetNodeExclusionFlag(queueType, node.ID, false)
}

// GetTopLevelChildren returns all children of the root node.
func GetTopLevelChildren(s *store.Store, queueType string, rootPath string) ([]*bolt.NodeState, error) {
	rootNode, err := s.GetNodeByPath(queueType, rootPath)
	if err != nil {
		return nil, err
	}

	childStatesIface, err := s.GetChildren(queueType, rootNode.ID, "states")
	if err != nil {
		return nil, err
	}

	childStates, ok := childStatesIface.([]*bolt.NodeState)
	if !ok {
		return nil, fmt.Errorf("failed to convert children to NodeStates")
	}

	return childStates, nil
}

// PickRandomTopLevelChild picks a random top-level child folder.
func PickRandomTopLevelChild(s *store.Store, queueType string, rootPath string) (*bolt.NodeState, error) {
	children, err := GetTopLevelChildren(s, queueType, rootPath)
	if err != nil {
		return nil, err
	}

	// Filter to folders only
	folders := make([]*bolt.NodeState, 0)
	for _, child := range children {
		if child.Type == "folder" {
			folders = append(folders, child)
		}
	}

	if len(folders) == 0 {
		return nil, fmt.Errorf("no folders found")
	}

	// Pick random
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return folders[r.Intn(len(folders))], nil
}

// PickRandomExcludedTopLevelChild picks a random excluded top-level child folder.
func PickRandomExcludedTopLevelChild(s *store.Store, queueType string, rootPath string) (*bolt.NodeState, error) {
	children, err := GetTopLevelChildren(s, queueType, rootPath)
	if err != nil {
		return nil, err
	}

	// Filter to excluded folders
	excludedFolders := make([]*bolt.NodeState, 0)
	for _, child := range children {
		if child.Type == "folder" && (child.ExplicitExcluded || child.InheritedExcluded) {
			excludedFolders = append(excludedFolders, child)
		}
	}

	if len(excludedFolders) == 0 {
		return nil, fmt.Errorf("no excluded folders found")
	}

	// Pick random
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return excludedFolders[r.Intn(len(excludedFolders))], nil
}

// PickFirstExcludedTopLevelChild picks the first excluded top-level child folder.
func PickFirstExcludedTopLevelChild(s *store.Store, queueType string, rootPath string) (*bolt.NodeState, error) {
	children, err := GetTopLevelChildren(s, queueType, rootPath)
	if err != nil {
		return nil, err
	}

	// Find first excluded folder
	for _, child := range children {
		if child.Type == "folder" && (child.ExplicitExcluded || child.InheritedExcluded) {
			return child, nil
		}
	}

	return nil, fmt.Errorf("no excluded folders found")
}

// CountPendingNodes returns the total number of pending nodes.
// Now uses Store API directly - no wrapper needed.
func CountPendingNodes(s *store.Store, queueType string) (int, error) {
	return s.GetQueueDepth(queueType)
}

// CountExcludedNodes returns the total number of excluded nodes.
// Now uses Store API directly - no wrapper needed.
func CountExcludedNodes(s *store.Store, queueType string) (int, error) {
	return s.CountExcludedNodes(queueType)
}

// CountExcludedInSubtree returns the number of excluded nodes in a subtree.
// Now uses Store API directly - no wrapper needed.
func CountExcludedInSubtree(s *store.Store, queueType string, rootPath string) (int, error) {
	return s.CountExcludedInSubtree(queueType, rootPath)
}
