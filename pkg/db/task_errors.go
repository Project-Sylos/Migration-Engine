// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

// TaskErrorEntry is a single task error (traversal or copy failure) stored in the errors bucket.
type TaskErrorEntry struct {
	ID        string `json:"id"`        // UUID
	NodeID    string `json:"node_id"`   // ULID of the node
	Phase     string `json:"phase"`     // src_traversal, src_copy, dst_traversal, dst_copy
	Message   string `json:"message"`   // Error message
	Timestamp string `json:"timestamp"` // RFC3339Nano
	Attempt   int    `json:"attempt"`   // Attempt number when error occurred
	Path      string `json:"path,omitempty"` // Node path at time of error (optional)
}

// taskErrorPhaseKey returns the phase sub-bucket key from queueType ("SRC"/"DST") and phase ("traversal"/"copy").
func taskErrorPhaseKey(queueType, phase string) string {
	return strings.ToLower(queueType) + "_" + phase
}

// RecordTaskError records a task error and appends its ref to the node's Errors slice.
// queueType is "SRC" or "DST"; phase is "traversal" or "copy".
// Returns the generated error ID or an error.
func RecordTaskError(db *DB, queueType, phase, nodeID, message string, attempt int, path string) (errorID string, err error) {
	phaseKey := taskErrorPhaseKey(queueType, phase)
	errorID = GenerateLogID()
	now := time.Now().Format(time.RFC3339Nano)
	entry := TaskErrorEntry{
		ID:        errorID,
		NodeID:    nodeID,
		Phase:     phaseKey,
		Message:   message,
		Timestamp: now,
		Attempt:   attempt,
		Path:      path,
	}
	entryBytes, err := json.Marshal(entry)
	if err != nil {
		return "", fmt.Errorf("serialize task error: %w", err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		phaseBucket := GetErrorsPhaseBucket(tx, phaseKey)
		if phaseBucket == nil {
			return fmt.Errorf("errors phase bucket not found: %s", phaseKey)
		}
		if err := phaseBucket.Put([]byte(errorID), entryBytes); err != nil {
			return fmt.Errorf("put task error: %w", err)
		}

		// Find node in level shards (nodes are level-sharded)
		levelsBucket := getBucket(tx, []string{TraversalDataBucket, queueType, SubBucketLevels})
		if levelsBucket == nil {
			return nil // No levels; error entry is still stored
		}
		nodeIDBytes := []byte(nodeID)
		cursor := levelsBucket.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			levelBucket := levelsBucket.Bucket(k)
			if levelBucket == nil {
				continue
			}
			nodesBucket := levelBucket.Bucket([]byte(SubBucketNodes))
			if nodesBucket == nil {
				continue
			}
			nodeData := nodesBucket.Get(nodeIDBytes)
			if nodeData == nil {
				continue
			}
			ns, err := DeserializeNodeState(nodeData)
			if err != nil {
				return fmt.Errorf("deserialize node state: %w", err)
			}
			if ns.Errors == nil {
				ns.Errors = []ErrorRef{}
			}
			ns.Errors = append(ns.Errors, ErrorRef{ID: errorID, Phase: phaseKey})
			updated, err := ns.Serialize()
			if err != nil {
				return fmt.Errorf("serialize node state: %w", err)
			}
			return nodesBucket.Put(nodeIDBytes, updated)
		}
		return nil // Node not found; error entry is still stored
	})
	if err != nil {
		return "", err
	}
	return errorID, nil
}

// GetTaskError retrieves a task error by ID and phase.
func GetTaskError(db *DB, phase, errorID string) (*TaskErrorEntry, error) {
	var entry *TaskErrorEntry
	err := db.View(func(tx *bolt.Tx) error {
		phaseBucket := GetErrorsPhaseBucket(tx, phase)
		if phaseBucket == nil {
			return nil
		}
		data := phaseBucket.Get([]byte(errorID))
		if data == nil {
			return nil
		}
		var e TaskErrorEntry
		if err := json.Unmarshal(data, &e); err != nil {
			return err
		}
		entry = &e
		return nil
	})
	return entry, err
}

// GetTaskErrorsByNodeID retrieves all task errors for a node using its Errors refs.
// Returns entries in the order of node.Errors.
func GetTaskErrorsByNodeID(db *DB, node *NodeState) ([]*TaskErrorEntry, error) {
	if node == nil || len(node.Errors) == 0 {
		return nil, nil
	}
	var result []*TaskErrorEntry
	for _, ref := range node.Errors {
		entry, err := GetTaskError(db, ref.Phase, ref.ID)
		if err != nil {
			return result, err
		}
		if entry != nil {
			result = append(result, entry)
		}
	}
	return result, nil
}
