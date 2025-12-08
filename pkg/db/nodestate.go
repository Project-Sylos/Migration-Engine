// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"encoding/json"
	"fmt"
)

// NodeState represents the state of a node stored in BoltDB.
// This is used during traversal and copy phases.
type NodeState struct {
	ID                string `json:"id"`
	ParentID          string `json:"parent_id"`
	ParentPath        string `json:"parent_path"` // Parent's relative path (for querying children)
	Name              string `json:"name"`
	Path              string `json:"path"` // Relative to root (normalized, used for cross-service matching)
	Type              string `json:"type"` // "file" or "folder"
	Size              int64  `json:"size,omitempty"`
	MTime             string `json:"mtime"` // Last modified time
	Depth             int    `json:"depth"`
	CopyNeeded        bool   `json:"copy_needed"`        // Set during traversal if copy is required
	TraversalStatus   string `json:"traversal_status"`   // "pending", "successful", "failed", "not_on_src"
	CopyStatus        string `json:"copy_status"`        // "pending", "successful", "failed" (for future copy phase)
	Status            string `json:"status,omitempty"`   // Legacy: Comparison status for dst nodes
	ExplicitExcluded  bool   `json:"explicit_excluded"`  // Set by API, not modified by engine
	InheritedExcluded bool   `json:"inherited_excluded"` // Set by exclusion sweep engine
}

// Serialize converts NodeState to bytes for storage in BoltDB.
func (ns *NodeState) Serialize() ([]byte, error) {
	return json.Marshal(ns)
}

// DeserializeNodeState creates a NodeState from bytes stored in BoltDB.
func DeserializeNodeState(data []byte) (*NodeState, error) {
	var ns NodeState
	if err := json.Unmarshal(data, &ns); err != nil {
		return nil, fmt.Errorf("failed to deserialize NodeState: %w", err)
	}
	return &ns, nil
}

// SerializeStringSlice converts a string slice to bytes.
func SerializeStringSlice(slice []string) ([]byte, error) {
	return json.Marshal(slice)
}

// DeserializeStringSlice converts bytes to a string slice.
func DeserializeStringSlice(data []byte, slice *[]string) error {
	return json.Unmarshal(data, slice)
}
