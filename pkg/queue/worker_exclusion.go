// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
	"github.com/Project-Sylos/Sylos-DB/pkg/bolt"
	"github.com/Project-Sylos/Sylos-DB/pkg/store"
)

// ExclusionWorker executes exclusion tasks by propagating exclusion/unexclusion through subtrees.
type ExclusionWorker struct {
	id          string
	queue       *Queue
	store       *store.Store
	queueName   string          // "src" or "dst" for logging
	shutdownCtx context.Context // Context for shutdown signaling (optional)
}

// NewExclusionWorker creates a worker that executes exclusion tasks.
func NewExclusionWorker(
	id string,
	queue *Queue,
	storeInstance *store.Store,
	queueName string,
	shutdownCtx context.Context,
) *ExclusionWorker {
	return &ExclusionWorker{
		id:          id,
		queue:       queue,
		store:       storeInstance,
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

	// Read children ULIDs from children bucket (DB lookup)
	childIDs, err := w.getChildrenIDs(queueType, nodeID)
	if err != nil {
		return fmt.Errorf("failed to get children IDs for node %s: %w", nodeID, err)
	}

	// Process based on exclusion mode
	if task.ExclusionMode == "exclude" {
		return w.executeExclude(task, childIDs, queueType)
	} else {
		return w.executeUnexclude(task, nodeID, childIDs, queueType)
	}
}

// getChildrenIDs reads child ULIDs from the children bucket for a given parent ULID.
func (w *ExclusionWorker) getChildrenIDs(queueType string, parentID string) ([]string, error) {
	if w.store == nil {
		return []string{}, nil
	}
	result, err := w.store.GetChildren(queueType, parentID, "ids")
	if err != nil {
		// No children is not fatal
		return []string{}, nil
	}
	childIDs, ok := result.([]string)
	if !ok || childIDs == nil {
		return []string{}, nil
	}
	return childIDs, nil
}

// executeExclude handles exclusion propagation.
// Marks the parent node as inherited excluded (if not already explicitly excluded),
// then adds all children (files and folders) to the exclusion-holding bucket.
// Note: This function should only be called for folders. Files are handled by executeFileExclusion.
func (w *ExclusionWorker) executeExclude(task *TaskBase, childIDs []string, queueType string) error {
	// Safety check: files don't have children, so don't process them here
	if !task.IsFolder() {
		// Files should be handled by executeFileExclusion, not here
		// If we somehow get here, it means a file was in the exclusion-holding bucket
		// This shouldn't happen, but if it does, mark it as inherited excluded
		queueExclusionUpdate(w.store, queueType, task.ID, true) // inherited_excluded = true
		task.DiscoveredChildren = make([]ChildResult, 0)
		return nil
	}

	// Step 1: Mark the parent node as inherited excluded (if not already explicitly excluded)
	// Check if parent is already explicitly excluded - if so, skip marking
	var parentExplicitlyExcluded bool
	nodeState, err := w.store.GetNode(queueType, task.ID)
	if err != nil || nodeState == nil {
		return fmt.Errorf("node not found for id %s: %w", task.ID, err)
	}
	parentExplicitlyExcluded = nodeState.ExplicitExcluded

	// Only mark as inherited excluded if not already explicitly excluded
	if !parentExplicitlyExcluded {
		queueExclusionUpdate(w.store, queueType, task.ID, true) // inherited_excluded = true

		// Move node from previous status bucket to excluded status bucket
		if task.PreviousStatus != "" && task.PreviousStatus != bolt.StatusExcluded {
			queueStatusUpdate(w.store, queueType, nodeState.Depth, task.PreviousStatus, bolt.StatusExcluded, task.ID)
		}
	}

	// Step 2: Get all children (files and folders) and add them to exclusion-holding bucket
	var childEntries []bolt.ExclusionEntry
	for _, childID := range childIDs {
		childState, err := w.store.GetNode(queueType, childID)
		if err != nil || childState == nil {
			continue
		}
		childEntries = append(childEntries, bolt.ExclusionEntry{
			NodeID: childID,
			Depth:  childState.Depth,
		})
	}

	// Step 3: Write all children to exclusion-holding bucket
	if len(childEntries) > 0 {
		mode := "exclude" // executeExclude always uses exclusion bucket
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug", fmt.Sprintf("executeExclude: Adding %d children to %s-holding bucket (parent: %s)", len(childEntries), mode, task.ID), "worker", w.id, w.queueName)
		}
		for _, entry := range childEntries {
			if err := w.store.AddHoldingEntry(queueType, entry.NodeID, entry.Depth, mode); err != nil {
				return fmt.Errorf("failed to write child to %s-holding: %w", mode, err)
			}
		}
	}

	// No discovered children needed for exclusion tasks
	task.DiscoveredChildren = make([]ChildResult, 0)

	return nil
}

// executeUnexclude handles unexclusion propagation.
// Note: This function should only be called for folders. Files are handled by executeFileExclusion.
func (w *ExclusionWorker) executeUnexclude(task *TaskBase, nodeID string, childIDs []string, queueType string) error {
	// Safety check: files don't have children, so don't process them here
	if !task.IsFolder() {
		// Files should be handled by executeFileExclusion, not here
		// If we somehow get here, it means a file was in the unexclusion-holding bucket
		// This shouldn't happen, but if it does, check if it should be unexcluded

		// Get parent ID to check for excluded ancestor
		nodeState, err := w.store.GetNode(queueType, nodeID)
		if err != nil || nodeState == nil {
			return fmt.Errorf("node not found: %s", nodeID)
		}
		parentID := nodeState.ParentID

		hasExcludedAncestor, err := w.checkExcludedAncestorByID(parentID, queueType)
		if err != nil {
			return fmt.Errorf("failed to check excluded ancestor: %w", err)
		}

		if !hasExcludedAncestor {
			queueExclusionUpdate(w.store, queueType, task.ID, false) // inherited_excluded = false

			// Move from excluded status back to successful (unconditional for unexclusion tasks)
			// We've already verified the node is in excluded bucket during pull
			queueStatusUpdate(w.store, queueType, nodeState.Depth, bolt.StatusExcluded, bolt.StatusSuccessful, task.ID)
		}

		task.DiscoveredChildren = make([]ChildResult, 0)
		return nil
	}
	// Step 1: Check if parent node still has an excluded ancestor
	nodeState, err := w.store.GetNode(queueType, nodeID)
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
	if !hasExcludedAncestor {
		queueExclusionUpdate(w.store, queueType, task.ID, false) // inherited_excluded = false

		// Move from excluded status back to successful (unconditional for unexclusion tasks)
		// We've already verified the node is in excluded bucket during pull
		queueStatusUpdate(w.store, queueType, nodeState.Depth, bolt.StatusExcluded, bolt.StatusSuccessful, task.ID)
	}

	if hasExcludedAncestor {
		// Still has excluded ancestor, don't process children
		task.DiscoveredChildren = make([]ChildResult, 0)
		return nil
	}

	// Step 3: Get all children (files and folders) and add them to unexclusion-holding bucket
	var childEntries []bolt.ExclusionEntry
	for _, childID := range childIDs {
		childState, err := w.store.GetNode(queueType, childID)
		if err != nil || childState == nil {
			continue
		}
		childEntries = append(childEntries, bolt.ExclusionEntry{
			NodeID: childID,
			Depth:  childState.Depth,
		})
	}

	// Step 4: Write all children to unexclusion-holding bucket
	if len(childEntries) > 0 {
		mode := "unexclude" // executeUnexclude always uses unexclusion bucket
		if logservice.LS != nil {
			_ = logservice.LS.Log("debug", fmt.Sprintf("executeUnexclude: Adding %d children to %s-holding bucket (parent: %s)", len(childEntries), mode, task.ID), "worker", w.id, w.queueName)
		}
		for _, entry := range childEntries {
			if err := w.store.AddHoldingEntry(queueType, entry.NodeID, entry.Depth, mode); err != nil {
				return fmt.Errorf("failed to write child to %s-holding: %w", mode, err)
			}
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

	// For unexclude mode, check if ancestor is still excluded
	if task.ExclusionMode == "unexclude" {
		// Get parent ID from node state
		nodeState, err := w.store.GetNode(queueType, nodeID)
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

	// Queue write op to update file exclusion state
	inheritedExcluded := (task.ExclusionMode == "exclude")
	queueExclusionUpdate(w.store, queueType, nodeID, inheritedExcluded)

	// Move node from previous status bucket to excluded status bucket (or back from excluded)
	nodeState, _ := w.store.GetNode(queueType, nodeID)
	if nodeState != nil {
		if task.ExclusionMode == "exclude" && task.PreviousStatus != "" && task.PreviousStatus != bolt.StatusExcluded {
			// Moving to excluded status
			queueStatusUpdate(w.store, queueType, nodeState.Depth, task.PreviousStatus, bolt.StatusExcluded, nodeID)
		} else if task.ExclusionMode == "unexclude" {
			// Moving back from excluded to successful (unconditional for unexclusion tasks)
			// We've already verified the node is in excluded bucket during pull
			queueStatusUpdate(w.store, queueType, nodeState.Depth, bolt.StatusExcluded, bolt.StatusSuccessful, nodeID)
		}
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
		nodeState, err := w.store.GetNode(queueType, currentID)
		if err != nil || nodeState == nil {
			break // Can't continue up chain
		}

		// Check if this node is in the exclusion-holding bucket
		exists, mode, err := w.store.CheckHoldingEntry(queueType, nodeState.ID)
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
