// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"encoding/json"
	"fmt"
)

// NodeState represents the state of a node stored in BadgerDB.
// This is used during traversal and copy phases.
type NodeState struct {
	ID         string `json:"id"`
	ParentID   string `json:"parent_id"`
	ParentPath string `json:"parent_path"` // Parent's relative path (for querying children)
	Name       string `json:"name"`
	Path       string `json:"path"` // Relative to root (normalized, used for cross-service matching)
	Type       string `json:"type"` // "file" or "folder"
	Size       int64  `json:"size,omitempty"`
	MTime      string `json:"mtime"` // Last modified time
	Depth      int    `json:"depth"`
	CopyNeeded bool   `json:"copy_needed"`      // Set during traversal if copy is required
	Status     string `json:"status,omitempty"` // Comparison status for dst nodes: "Pending", "Missing", "NotOnSrc", "Successful"
}

// Serialize converts NodeState to bytes for storage in BadgerDB.
func (ns *NodeState) Serialize() ([]byte, error) {
	return json.Marshal(ns)
}

// DeserializeNodeState creates a NodeState from bytes stored in BadgerDB.
func DeserializeNodeState(data []byte) (*NodeState, error) {
	var ns NodeState
	if err := json.Unmarshal(data, &ns); err != nil {
		return nil, fmt.Errorf("failed to deserialize NodeState: %w", err)
	}
	return &ns, nil
}
