// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
)

// ExclusionWorker executes exclusion tasks by propagating exclusion/unexclusion through subtrees.
// Workers read from DB (not filesystem) and write via output buffer.
type ExclusionWorker struct {
	id          string
	queue       *Queue
	boltDB      *db.DB
	queueName   string          // "src" or "dst" for logging
	shutdownCtx context.Context // Context for shutdown signaling (optional)
}

// NewExclusionWorker creates a worker that executes exclusion tasks.
func NewExclusionWorker(
	id string,
	queue *Queue,
	boltInstance *db.DB,
	queueName string,
	shutdownCtx context.Context,
) *ExclusionWorker {
	return &ExclusionWorker{
		id:          id,
		queue:       queue,
		boltDB:      boltInstance,
		queueName:   queueName,
		shutdownCtx: shutdownCtx,
	}
}

// Run is the main worker loop for exclusion tasks.
func (w *ExclusionWorker) Run() {
	if logservice.LS != nil {
		_ = logservice.LS.Log("info", "Exclusion worker started", "worker", w.id, w.queueName)
	}

	for {
		// Check for shutdown first (force exit)
		if w.shutdownCtx != nil {
			select {
			case <-w.shutdownCtx.Done():
				if logservice.LS != nil {
					_ = logservice.LS.Log("info", "Exclusion worker exiting - shutdown requested", "worker", w.id, w.queueName)
				}
				return
			default:
			}
		}

		// Check lifecycle state
		if w.queue.IsPaused() {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Check if queue is exhausted
		if w.queue.IsExhausted() {
			if logservice.LS != nil {
				_ = logservice.LS.Log("info", "Exclusion worker exiting - queue exhausted", "worker", w.id, w.queueName)
			}
			return
		}

		// Try to lease a task from the queue
		task := w.queue.Lease()
		if task == nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// Execute the exclusion task
		err := w.execute(task)
		if err != nil {
			if logservice.LS != nil {
				_ = logservice.LS.Log("debug",
					fmt.Sprintf("Exclusion worker task execution failed: path=%s round=%d error=%v",
						task.LocationPath(), task.Round, err),
					"worker", w.id, w.queueName)
			}
			w.queue.ReportTaskResult(task, TaskExecutionResultFailed)
			nodeID := task.ID
			willRetry := w.queue.isInPendingSet(nodeID)
			w.logError(task, err, willRetry)
		} else {
			w.queue.ReportTaskResult(task, TaskExecutionResultSuccessful)
		}
	}
}

// execute performs the exclusion propagation work.
// Workers read from DB children bucket and write children to exclusion-holding bucket.
func (w *ExclusionWorker) execute(task *TaskBase) error {
	if task.Type != TaskTypeExclusion {
		return fmt.Errorf("exclusion worker received non-exclusion task")
	}

	if !task.IsFolder() {
		// Files don't have children, just update the node
		return w.executeFileExclusion(task)
	}

	nodeID := task.ID
	if nodeID == "" {
		return fmt.Errorf("task missing ULID")
	}
	queueType := getQueueType(w.queueName)

	// Get queue's output buffer for write operations
	outputBuffer := w.queue.getOutputBuffer()

	// Read children ULIDs from children bucket (DB lookup)
	childIDs, err := w.getChildrenIDs(queueType, nodeID)
	if err != nil {
		return fmt.Errorf("failed to get children IDs for node %s: %w", nodeID, err)
	}

	// Process based on exclusion mode
	if task.ExclusionMode == "exclude" {
		return w.executeExclude(task, childIDs, queueType, outputBuffer)
	} else {
		return w.executeUnexclude(task, nodeID, childIDs, queueType, outputBuffer)
	}
}

// getChildrenIDs reads child ULIDs from the children bucket for a given parent ULID.
func (w *ExclusionWorker) getChildrenIDs(queueType string, parentID string) ([]string, error) {
	var childIDs []string

	err := w.boltDB.View(func(tx *bolt.Tx) error {
		childrenBucket := db.GetChildrenBucket(tx, queueType)
		if childrenBucket == nil {
			return nil // No children bucket, no children
		}

		parentIDBytes := []byte(parentID)
		childrenData := childrenBucket.Get(parentIDBytes)
		if childrenData == nil {
			return nil // No children for this parent
		}

		// Unmarshal the list of child ULIDs
		if err := json.Unmarshal(childrenData, &childIDs); err != nil {
			return fmt.Errorf("failed to unmarshal children list: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Return empty slice if no children found (not nil)
	if childIDs == nil {
		return []string{}, nil
	}

	return childIDs, nil
}

// executeExclude handles exclusion propagation.
// Marks the parent node as inherited excluded (if not already explicitly excluded),
// then adds all children (files and folders) to the exclusion-holding bucket.
// Note: This function should only be called for folders. Files are handled by executeFileExclusion.
func (w *ExclusionWorker) executeExclude(task *TaskBase, childIDs []string, queueType string, outputBuffer *db.OutputBuffer) error {
	// Safety check: files don't have children, so don't process them here
	if !task.IsFolder() {
		// Files should be handled by executeFileExclusion, not here
		// If we somehow get here, it means a file was in the exclusion-holding bucket
		// This shouldn't happen, but if it does, mark it as inherited excluded
		if outputBuffer != nil {
			outputBuffer.AddExclusionUpdate(queueType, task.ID, true) // inherited_excluded = true
		}
		task.DiscoveredChildren = make([]ChildResult, 0)
		return nil
	}

	// Step 1: Mark the parent node as inherited excluded (if not already explicitly excluded)
	// Check if parent is already explicitly excluded - if so, skip marking
	var parentExplicitlyExcluded bool
	nodeState, err := db.GetNodeState(w.boltDB, queueType, task.ID)
	if err != nil || nodeState == nil {
		return fmt.Errorf("node not found for id %s: %w", task.ID, err)
	}
	parentExplicitlyExcluded = nodeState.ExplicitExcluded

	// Only mark as inherited excluded if not already explicitly excluded
	if !parentExplicitlyExcluded && outputBuffer != nil {
		outputBuffer.AddExclusionUpdate(queueType, task.ID, true) // inherited_excluded = true
	}

	// Step 2: Get all children (files and folders) and add them to exclusion-holding bucket
	var childEntries []db.ExclusionEntry

	err = w.boltDB.View(func(tx *bolt.Tx) error {
		nodesBucket := db.GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found")
		}

		for _, childID := range childIDs {
			nodeData := nodesBucket.Get([]byte(childID))
			if nodeData == nil {
				continue // Child may have been deleted
			}

			childState, err := db.DeserializeNodeState(nodeData)
			if err != nil {
				continue // Skip invalid nodes
			}

			// Add ALL children (both files and folders) to the exclusion-holding bucket
			childEntries = append(childEntries, db.ExclusionEntry{
				NodeID: childID,
				Depth:  childState.Depth,
			})
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to get child depths: %w", err)
	}

	// Step 3: Write all children to exclusion-holding bucket
	if len(childEntries) > 0 {
		mode := "exclude" // executeExclude always uses exclusion bucket
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug", fmt.Sprintf("executeExclude: Adding %d children to %s-holding bucket (parent: %s)", len(childEntries), mode, task.ID), "worker", w.id, w.queueName)
		}
		err = w.boltDB.Update(func(tx *bolt.Tx) error {
			holdingBucket, err := db.GetOrCreateHoldingBucket(tx, queueType, mode)
			if err != nil {
				return err
			}

			// Write each child ULID with its depth as value
			for _, entry := range childEntries {
				depthBytes := make([]byte, 8)
				binary.BigEndian.PutUint64(depthBytes, uint64(entry.Depth))
				if err := holdingBucket.Put([]byte(entry.NodeID), depthBytes); err != nil {
					return fmt.Errorf("failed to write child to %s-holding: %w", mode, err)
				}
			}

			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to write children to %s-holding bucket: %w", mode, err)
		}
	}

	// No discovered children needed for exclusion tasks
	task.DiscoveredChildren = make([]ChildResult, 0)

	return nil
}

// executeUnexclude handles unexclusion propagation.
// Note: This function should only be called for folders. Files are handled by executeFileExclusion.
func (w *ExclusionWorker) executeUnexclude(task *TaskBase, nodeID string, childIDs []string, queueType string, outputBuffer *db.OutputBuffer) error {
	// Safety check: files don't have children, so don't process them here
	if !task.IsFolder() {
		// Files should be handled by executeFileExclusion, not here
		// If we somehow get here, it means a file was in the unexclusion-holding bucket
		// This shouldn't happen, but if it does, check if it should be unexcluded

		// Get parent ID to check for excluded ancestor
		nodeState, err := db.GetNodeState(w.boltDB, queueType, nodeID)
		if err != nil || nodeState == nil {
			return fmt.Errorf("node not found: %s", nodeID)
		}
		parentID := nodeState.ParentID

		hasExcludedAncestor, err := w.checkExcludedAncestorByID(parentID, queueType)
		if err != nil {
			return fmt.Errorf("failed to check excluded ancestor: %w", err)
		}

		if !hasExcludedAncestor && outputBuffer != nil {
			outputBuffer.AddExclusionUpdate(queueType, task.ID, false) // inherited_excluded = false
		}

		task.DiscoveredChildren = make([]ChildResult, 0)
		return nil
	}
	// Step 1: Check if parent node still has an excluded ancestor
	nodeState, err := db.GetNodeState(w.boltDB, queueType, nodeID)
	if err != nil || nodeState == nil {
		return fmt.Errorf("node not found: %s", nodeID)
	}
	parentID := nodeState.ParentID

	// Check if node still has an excluded ancestor
	hasExcludedAncestor, err := w.checkExcludedAncestorByID(parentID, queueType)
	if err != nil {
		return fmt.Errorf("failed to check excluded ancestor: %w", err)
	}

	// Step 2: Mark the parent node as inherited excluded = false (if no excluded ancestor)
	if !hasExcludedAncestor && outputBuffer != nil {
		outputBuffer.AddExclusionUpdate(queueType, task.ID, false) // inherited_excluded = false
	}

	if hasExcludedAncestor {
		// Still has excluded ancestor, don't process children
		task.DiscoveredChildren = make([]ChildResult, 0)
		return nil
	}

	// Step 3: Get all children (files and folders) and add them to unexclusion-holding bucket
	var childEntries []db.ExclusionEntry

	err = w.boltDB.View(func(tx *bolt.Tx) error {
		nodesBucket := db.GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found")
		}

		for _, childID := range childIDs {
			nodeData := nodesBucket.Get([]byte(childID))
			if nodeData == nil {
				continue // Child may have been deleted
			}

			childState, err := db.DeserializeNodeState(nodeData)
			if err != nil {
				continue // Skip invalid nodes
			}

			// Add ALL children (both files and folders) to the unexclusion-holding bucket
			childEntries = append(childEntries, db.ExclusionEntry{
				NodeID: childID,
				Depth:  childState.Depth,
			})
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to get child depths: %w", err)
	}

	// Step 4: Write all children to unexclusion-holding bucket
	if len(childEntries) > 0 {
		mode := "unexclude" // executeUnexclude always uses unexclusion bucket
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug", fmt.Sprintf("executeUnexclude: Adding %d children to %s-holding bucket (parent: %s)", len(childEntries), mode, task.ID), "worker", w.id, w.queueName)
		}
		err = w.boltDB.Update(func(tx *bolt.Tx) error {
			holdingBucket, err := db.GetOrCreateHoldingBucket(tx, queueType, mode)
			if err != nil {
				return err
			}

			for _, entry := range childEntries {
				depthBytes := make([]byte, 8)
				binary.BigEndian.PutUint64(depthBytes, uint64(entry.Depth))
				if err := holdingBucket.Put([]byte(entry.NodeID), depthBytes); err != nil {
					return fmt.Errorf("failed to write child to %s-holding: %w", mode, err)
				}
			}

			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to write children to %s-holding bucket: %w", mode, err)
		}
	}

	task.DiscoveredChildren = make([]ChildResult, 0)
	return nil
}

// executeFileExclusion handles exclusion for files (no children to propagate).
func (w *ExclusionWorker) executeFileExclusion(task *TaskBase) error {
	nodeID := task.ID
	if nodeID == "" {
		return fmt.Errorf("task missing ULID")
	}
	queueType := getQueueType(w.queueName)

	outputBuffer := w.queue.getOutputBuffer()

	// For unexclude mode, check if ancestor is still excluded
	if task.ExclusionMode == "unexclude" {
		// Get parent ID from node state
		nodeState, err := db.GetNodeState(w.boltDB, queueType, nodeID)
		if err != nil || nodeState == nil {
			return fmt.Errorf("node not found: %s", nodeID)
		}

		parentID := nodeState.ParentID
		hasExcludedAncestor, err := w.checkExcludedAncestorByID(parentID, queueType)
		if err != nil {
			return fmt.Errorf("failed to check excluded ancestor: %w", err)
		}
		if hasExcludedAncestor {
			// Still has excluded ancestor, don't clear inherited_excluded
			task.DiscoveredChildren = make([]ChildResult, 0)
			return nil
		}
	}

	// Queue write op to update file exclusion state via output buffer
	if outputBuffer != nil {
		inheritedExcluded := (task.ExclusionMode == "exclude")
		outputBuffer.AddExclusionUpdate(queueType, nodeID, inheritedExcluded)
	}

	task.DiscoveredChildren = make([]ChildResult, 0)
	return nil
}

// checkExcludedAncestorByID checks if any ancestor is still excluded by walking up the parent chain using ULIDs.
func (w *ExclusionWorker) checkExcludedAncestorByID(startID string, queueType string) (bool, error) {
	if startID == "" {
		return false, nil // Root has no parent
	}

	currentID := startID
	for {
		// Get node state by ULID
		nodeState, err := db.GetNodeState(w.boltDB, queueType, currentID)
		if err != nil || nodeState == nil {
			break // Can't continue up chain
		}

		// Check if this node is in the exclusion-holding bucket
		exists, mode, err := db.CheckHoldingEntry(w.boltDB, queueType, nodeState.ID)
		if err != nil {
			return false, err
		}
		// If found in exclusion bucket, ancestor is excluded
		if exists && mode == "exclude" {
			return true, nil // Found excluded ancestor
		}

		if nodeState.ParentID == "" || nodeState.ParentID == currentID {
			break // Reached root or circular reference
		}

		currentID = nodeState.ParentID
	}

	return false, nil // No excluded ancestor found
}

// logError logs a failed exclusion task execution.
func (w *ExclusionWorker) logError(task *TaskBase, err error, willRetry bool) {
	if logservice.LS == nil {
		return
	}
	path := task.LocationPath()
	retryMsg := "will retry"
	if !willRetry {
		retryMsg = "max retries exceeded"
	}

	_ = logservice.LS.Log(
		"error",
		fmt.Sprintf("Failed to process exclusion for %s: %v (%s)", path, err, retryMsg),
		"worker",
		w.id,
		w.queueName,
	)
}
