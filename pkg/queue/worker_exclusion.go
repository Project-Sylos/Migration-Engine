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
	"github.com/Project-Sylos/Sylos-FS/pkg/types"
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
			w.queue.mu.RLock()
			path := task.LocationPath()
			_, willRetry := w.queue.pendingSet[path]
			w.queue.mu.RUnlock()
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

	path := task.LocationPath()
	pathHash := db.HashPath(path)
	queueType := getQueueType(w.queueName)

	// Get queue's output buffer for write operations
	w.queue.mu.RLock()
	outputBuffer := w.queue.outputBuffer
	w.queue.mu.RUnlock()

	// Read children hash keys from children bucket (DB lookup)
	childHashes, err := w.getChildrenHashes(queueType, pathHash)
	if err != nil {
		return fmt.Errorf("failed to get children hashes for %s: %w", path, err)
	}

	// Process based on exclusion mode
	if task.ExclusionMode == "exclude" {
		return w.executeExclude(task, childHashes, queueType, outputBuffer)
	} else {
		return w.executeUnexclude(task, pathHash, childHashes, queueType, outputBuffer)
	}
}

// getChildrenHashes reads child hash keys from the children bucket for a given parent path hash.
func (w *ExclusionWorker) getChildrenHashes(queueType string, parentPathHash string) ([]string, error) {
	var childHashes []string

	err := w.boltDB.View(func(tx *bolt.Tx) error {
		childrenBucket := db.GetChildrenBucket(tx, queueType)
		if childrenBucket == nil {
			return nil // No children bucket, no children
		}

		parentHashBytes := []byte(parentPathHash)
		childrenData := childrenBucket.Get(parentHashBytes)
		if childrenData == nil {
			return nil // No children for this parent
		}

		// Unmarshal the list of child hash keys
		if err := json.Unmarshal(childrenData, &childHashes); err != nil {
			return fmt.Errorf("failed to unmarshal children list: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Return empty slice if no children found (not nil)
	if childHashes == nil {
		return []string{}, nil
	}

	return childHashes, nil
}

// executeExclude handles exclusion propagation.
// Reads children from DB, writes them to exclusion-holding bucket, and queues parent exclusion update.
func (w *ExclusionWorker) executeExclude(task *TaskBase, childHashes []string, queueType string, outputBuffer *db.OutputBuffer) error {
	// Need to get child depths to write to exclusion-holding bucket
	// For each child hash, get its depth from node state
	var childEntries []db.ExclusionEntry

	err := w.boltDB.View(func(tx *bolt.Tx) error {
		nodesBucket := db.GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found")
		}

		for _, childHash := range childHashes {
			nodeData := nodesBucket.Get([]byte(childHash))
			if nodeData == nil {
				continue // Child may have been deleted
			}

			childState, err := db.DeserializeNodeState(nodeData)
			if err != nil {
				continue // Skip invalid nodes
			}

			// Only folders propagate exclusion
			if childState.Type == types.NodeTypeFolder {
				childEntries = append(childEntries, db.ExclusionEntry{
					PathHash: childHash,
					Depth:    childState.Depth,
				})
			}
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to get child depths: %w", err)
	}

	// Write child hashes to exclusion-holding bucket (bulk insert)
	// Workers write directly to DB for exclusion-holding bucket operations
	if len(childEntries) > 0 {
		err = w.boltDB.Update(func(tx *bolt.Tx) error {
			holdingBucket, err := db.GetOrCreateExclusionHoldingBucket(tx, queueType)
			if err != nil {
				return err
			}

			// Write each child hash with its depth as value
			for _, entry := range childEntries {
				depthBytes := make([]byte, 8)
				binary.BigEndian.PutUint64(depthBytes, uint64(entry.Depth))
				if err := holdingBucket.Put([]byte(entry.PathHash), depthBytes); err != nil {
					return fmt.Errorf("failed to write child to exclusion-holding: %w", err)
				}
			}

			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to write children to exclusion-holding bucket: %w", err)
		}
	}

	// Queue write op to mark parent as excluded via output buffer
	if outputBuffer != nil {
		path := task.LocationPath()
		outputBuffer.AddExclusionUpdate(queueType, path, true) // inherited_excluded = true
	}

	// No discovered children needed for exclusion tasks
	task.DiscoveredChildren = make([]ChildResult, 0)

	return nil
}

// executeUnexclude handles unexclusion propagation.
func (w *ExclusionWorker) executeUnexclude(task *TaskBase, pathHash string, childHashes []string, queueType string, outputBuffer *db.OutputBuffer) error {
	// Get parent path to check for excluded ancestor
	path := task.LocationPath()

	// Get node state to find parent path
	var parentPath string
	err := w.boltDB.View(func(tx *bolt.Tx) error {
		nodesBucket := db.GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found")
		}

		nodeData := nodesBucket.Get([]byte(pathHash))
		if nodeData == nil {
			return fmt.Errorf("node not found: %s", pathHash)
		}

		nodeState, err := db.DeserializeNodeState(nodeData)
		if err != nil {
			return fmt.Errorf("failed to deserialize node state: %w", err)
		}

		parentPath = nodeState.ParentPath
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to get node state: %w", err)
	}

	// Check if node still has an excluded ancestor
	hasExcludedAncestor, err := w.checkExcludedAncestor(parentPath, queueType)
	if err != nil {
		return fmt.Errorf("failed to check excluded ancestor: %w", err)
	}

	if hasExcludedAncestor {
		// Don't clear inherited_excluded, no children enqueued
		task.DiscoveredChildren = make([]ChildResult, 0)
		return nil
	}

	// Get child depths and write to exclusion-holding bucket
	var childEntries []db.ExclusionEntry

	err = w.boltDB.View(func(tx *bolt.Tx) error {
		nodesBucket := db.GetNodesBucket(tx, queueType)
		if nodesBucket == nil {
			return fmt.Errorf("nodes bucket not found")
		}

		for _, childHash := range childHashes {
			nodeData := nodesBucket.Get([]byte(childHash))
			if nodeData == nil {
				continue
			}

			childState, err := db.DeserializeNodeState(nodeData)
			if err != nil {
				continue
			}

			// Only folders propagate unexclusion
			if childState.Type == types.NodeTypeFolder {
				childEntries = append(childEntries, db.ExclusionEntry{
					PathHash: childHash,
					Depth:    childState.Depth,
				})
			}
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to get child depths: %w", err)
	}

	// Write child hashes to exclusion-holding bucket
	if len(childEntries) > 0 {
		err = w.boltDB.Update(func(tx *bolt.Tx) error {
			holdingBucket, err := db.GetOrCreateExclusionHoldingBucket(tx, queueType)
			if err != nil {
				return err
			}

			for _, entry := range childEntries {
				depthBytes := make([]byte, 8)
				binary.BigEndian.PutUint64(depthBytes, uint64(entry.Depth))
				if err := holdingBucket.Put([]byte(entry.PathHash), depthBytes); err != nil {
					return fmt.Errorf("failed to write child to exclusion-holding: %w", err)
				}
			}

			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to write children to exclusion-holding bucket: %w", err)
		}
	}

	// Queue write op to mark parent as unexcluded via output buffer
	if outputBuffer != nil {
		outputBuffer.AddExclusionUpdate(queueType, path, false) // inherited_excluded = false
	}

	task.DiscoveredChildren = make([]ChildResult, 0)
	return nil
}

// executeFileExclusion handles exclusion for files (no children to propagate).
func (w *ExclusionWorker) executeFileExclusion(task *TaskBase) error {
	path := task.LocationPath()
	pathHash := db.HashPath(path)
	queueType := getQueueType(w.queueName)

	w.queue.mu.RLock()
	outputBuffer := w.queue.outputBuffer
	w.queue.mu.RUnlock()

	// For unexclude mode, check if ancestor is still excluded
	if task.ExclusionMode == "unexclude" {
		// Get parent path from node state
		var parentPath string
		err := w.boltDB.View(func(tx *bolt.Tx) error {
			nodesBucket := db.GetNodesBucket(tx, queueType)
			if nodesBucket == nil {
				return fmt.Errorf("nodes bucket not found")
			}

			nodeData := nodesBucket.Get([]byte(pathHash))
			if nodeData == nil {
				return fmt.Errorf("node not found: %s", pathHash)
			}

			nodeState, err := db.DeserializeNodeState(nodeData)
			if err != nil {
				return fmt.Errorf("failed to deserialize node state: %w", err)
			}

			parentPath = nodeState.ParentPath
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to get node state: %w", err)
		}

		hasExcludedAncestor, err := w.checkExcludedAncestor(parentPath, queueType)
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
		outputBuffer.AddExclusionUpdate(queueType, path, inheritedExcluded)
	}

	task.DiscoveredChildren = make([]ChildResult, 0)
	return nil
}

// checkExcludedAncestor checks if any ancestor is still excluded by walking up the parent chain.
func (w *ExclusionWorker) checkExcludedAncestor(startPath string, queueType string) (bool, error) {
	if startPath == "" {
		return false, nil // Root has no parent
	}

	currentPath := startPath
	for {
		pathHash := db.HashPath(currentPath)
		exists, err := db.CheckExclusionHoldingEntry(w.boltDB, queueType, pathHash)
		if err != nil {
			return false, err
		}
		if exists {
			return true, nil // Found excluded ancestor
		}

		// Get parent path
		nodeState, err := db.GetNodeState(w.boltDB, queueType, []byte(pathHash))
		if err != nil || nodeState == nil {
			break // Can't continue up chain
		}

		if nodeState.ParentPath == "" || nodeState.ParentPath == currentPath {
			break // Reached root or circular reference
		}

		currentPath = nodeState.ParentPath
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
