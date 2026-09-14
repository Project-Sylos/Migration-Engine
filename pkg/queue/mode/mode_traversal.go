// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package mode

import (
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/failurelog"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/filter"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/gpl"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// PullTraversalTasks refills the queue from DuckDB for the current round (ID-offset pagination, ~10K batch).
// SRC: ListNodesPendingAtDepthKeyset (folders from *_current); DST: ListDstBatchWithSrcChildren.
func PullTraversalTasks(q *queue.Queue, force bool) queue.PullResult {
	database := q.Database()
	if database == nil {
		return queue.PullResult{Status: queue.PullAborted}
	}
	if !q.TryBeginPulling() {
		return queue.PullResult{Round: q.GetRound(), Status: queue.PullSkipped}
	}
	defer q.SetPulling(false)
	if q.State() == queue.QueueStateCompleted {
		return queue.PullResult{Status: queue.PullAborted}
	}
	// Allow first refill attempt without prior cache hydration (DB-backed frontier).
	if !q.TraversalCacheLoaded() {
		q.SetTraversalCacheLoaded(true)
	}

	taskType := queue.TaskTypeSrcTraversal
	if q.Name() == "dst" {
		taskType = queue.TaskTypeDstTraversal
	}
	snapshot := q.StateSnapshot()
	if !force {
		if snapshot.State != queue.QueueStateRunning || snapshot.PendingCount > snapshot.PullLowWM {
			return queue.PullResult{Round: snapshot.Round, Status: queue.PullSkipped}
		}
	} else if snapshot.State == queue.QueueStatePaused {
		return queue.PullResult{Round: snapshot.Round, Status: queue.PullAborted}
	}
	currentRound := snapshot.Round
	coordinator := q.Coordinator()
	if q.Name() == "dst" && coordinator != nil {
		if !coordinator.CanDstStartRound(currentRound) {
			return queue.PullResult{Round: currentRound, Status: queue.PullSkipped}
		}
	}

	batchSize := q.EffectiveRefillBatchSize()
	var count int
	if q.Name() == "dst" {
		afterID := q.GetKeysetCursor()
		taskQuota := batchSize + 1
		childQuota := q.EffectiveDstPullChildQuota(batchSize)
		q.BeginDBPull()
		pullStart := time.Now()
		dstBatch, childrenByDstID, srcParentDeleteByDstID, lastEnqueuedID, exhausted, err := pull.ListDstBatchWithSrcChildrenQuota(database, currentRound, afterID, pull.DstPullQuota{
			MaxTasks:    taskQuota,
			MaxChildren: childQuota,
		}, db.StatusPending)
		q.RecordDBPull(len(dstBatch), time.Since(pullStart))
		q.ReleaseDBPull()
		if err != nil {
			if q.GetRound() != currentRound {
				return queue.PullResult{Round: currentRound, QueriedDB: true, Status: queue.PullStaleRound}
			}
			q.SetLastPullWasPartial(true)
			q.RecordPull(currentRound, 0, true)
			q.SetFirstPullForRound(false)
			return queue.PullResult{Round: currentRound, Yield: 0, Partial: true, QueriedDB: true, Status: queue.PullOK}
		}
		if q.GetRound() != currentRound {
			return queue.PullResult{Round: currentRound, QueriedDB: true, Status: queue.PullStaleRound}
		}
		processLimit := batchSize
		if len(dstBatch) <= batchSize {
			processLimit = len(dstBatch)
		}
		expectedFoldersMap, expectedFilesMap, srcIDMap, srcIDToMeta := queue.BuildExpectedMapsFromDstWithChildren(dstBatch[:processLimit], childrenByDstID)
		for i := 0; i < processLimit; i++ {
			fr := dstBatch[i]
			task := queue.NodeStateToTask(fr.State, taskType)
			if task != nil {
				if task.ID == "" {
					task.ID = fr.Key
				}
				if task.IsFolder() {
					task.ExpectedFolders = expectedFoldersMap[fr.Key]
					task.ExpectedFiles = expectedFilesMap[fr.Key]
					task.ExpectedSrcIDMap = srcIDMap[fr.Key]
					task.ExpectedSrcNodeMeta = srcIDToMeta
					task.SrcParentDeleteStatus = srcParentDeleteByDstID[fr.Key]
				}
				_ = q.Add(task)
				count++
			}
		}
		if processLimit > 0 && processLimit < len(dstBatch) {
			q.SetKeysetCursor(dstBatch[processLimit-1].Key)
		} else if lastEnqueuedID != "" {
			q.SetKeysetCursor(lastEnqueuedID)
		} else if processLimit > 0 {
			q.SetKeysetCursor(dstBatch[processLimit-1].Key)
		}
		partial := exhausted && len(dstBatch) <= batchSize
		q.SetLastPullWasPartial(partial)
		q.RecordPull(currentRound, count, partial)
		q.SetFirstPullForRound(false)
		return queue.PullResult{Round: currentRound, Yield: count, Partial: partial, QueriedDB: true, Status: queue.PullOK}
	} else {
		requestLimit := batchSize + 1
		afterID := q.GetKeysetCursor()
		queueType := queue.GetQueueType(q.Name())
		q.BeginDBPull()
		pullStart := time.Now()
		results, err := pull.ListNodesPendingAtDepthKeyset(database, queueType, currentRound, afterID, requestLimit, db.NodeTypeFolder)
		q.RecordDBPull(len(results), time.Since(pullStart))
		q.ReleaseDBPull()
		if err != nil {
			if q.GetRound() != currentRound {
				return queue.PullResult{Round: currentRound, QueriedDB: true, Status: queue.PullStaleRound}
			}
			q.SetLastPullWasPartial(true)
			q.RecordPull(currentRound, 0, true)
			q.SetFirstPullForRound(false)
			return queue.PullResult{Round: currentRound, Yield: 0, Partial: true, QueriedDB: true, Status: queue.PullOK}
		}
		if q.GetRound() != currentRound {
			return queue.PullResult{Round: currentRound, QueriedDB: true, Status: queue.PullStaleRound}
		}
		processLimit := batchSize
		if len(results) <= batchSize {
			processLimit = len(results)
		}
		for i := 0; i < processLimit; i++ {
			fr := results[i]
			task := queue.NodeStateToTask(fr.State, taskType)
			if task != nil {
				if task.ID == "" {
					task.ID = fr.Key
				}
				_ = q.Add(task)
				count++
			}
		}
		if len(results) > 0 {
			cursorIdx := processLimit - 1
			if cursorIdx < 0 {
				cursorIdx = 0
			}
			q.SetKeysetCursor(results[cursorIdx].Key)
		}
		partial := len(results) <= batchSize
		q.SetLastPullWasPartial(partial)
		q.RecordPull(currentRound, count, partial)
		q.SetFirstPullForRound(false)
		return queue.PullResult{Round: currentRound, Yield: count, Partial: partial, QueriedDB: true, Status: queue.PullOK}
	}
}

// CompleteTraversalTask handles successful completion of traversal/retry tasks.
// This includes child discovery, status updates, and buffer operations.
func CompleteTraversalTask(q *queue.Queue, task *queue.TaskBase, executionDelta time.Duration) {
	// Record execution time delta
	q.RecordExecutionTime(executionDelta)
	currentRound := task.Round
	nodeID := task.ID

	task.Locked = false

	// Update discovery counters (thread-safe)
	totalChildren := len(task.DiscoveredChildren)
	foldersCount := 0
	filesCount := 0
	if totalChildren > 0 {
		for _, child := range task.DiscoveredChildren {
			if child.IsFile {
				filesCount++
			} else {
				foldersCount++
			}
		}
	}
	markTraversalComplete := func() {
		task.Status = "successful"
		q.IncrementRoundStatsCompleted(currentRound)
		q.IncrementTasksCompletedTotal()
		q.RecordTaskCompletion(currentRound, true)
		if totalChildren > 0 {
			q.AddDiscoveredTotals(int64(filesCount), int64(foldersCount))
		}
	}

	database := q.Database()
	if database == nil {
		markTraversalComplete()
		q.RemoveInProgress(nodeID)
		return
	}

	queueType := queue.GetQueueType(q.Name())

	nextRound := currentRound + 1

	// Prepare child nodes for insertion
	parentPath := types.NormalizeLocationPath(task.LocationPath())
	var childNodesToInsert []db.InsertOperation

	// Collect discovered children for insertion
	// Deterministic IDs based on (queueType, nodeType, path) ensure no duplicates -
	// the same logical node will always get the same ID, making this race-safe
	parentDisplayPath := task.DisplayPath
	if parentDisplayPath == "" && currentRound == 0 {
		parentDisplayPath = filter.RootDisplayPath()
	}
	for _, child := range task.DiscoveredChildren {
		// For DST queues, skip folder children here - they'll be handled separately
		if queueType == "DST" && !child.IsFile {
			continue
		}

		// Create NodeState with deterministic ID (no need to check for existing children -
		// deterministic IDs naturally dedupe at all layers)
		childState := queue.ChildResultToNodeState(child, parentPath, nextRound, queueType, nodeID)
		if childState == nil {
			continue
		}

		if queueType == "SRC" {
			allow := queue.ParseIncludeOnlyJSON(task.IncludeOnlyJSON)
			sid := child.File.ServiceID
			if !child.IsFile {
				sid = child.Folder.ServiceID
			}
			if !queue.ChildAllowedByIncludeOnly(allow, sid) {
				continue
			}
		}

		// Populate traversal status in the NodeState metadata
		childState.TraversalStatus = child.Status
		childState.Status = child.Status

		if queueType == "SRC" {
			queue.StampDisplayPath(parentDisplayPath, currentRound, childState)
		} else {
			childState.DisplayPath = filter.DisplayPathForChild(parentDisplayPath, currentRound, childState.Name)
		}

		childNodesToInsert = append(childNodesToInsert, db.InsertOperation{
			QueueType: queueType,
			Level:     nextRound,
			Status:    childState.TraversalStatus,
			State:     childState,
		})

	}

	// Handle DST queue special case: create tasks for child folders
	if q.Name() == "dst" {
		type dstChildFolder struct {
			folder        types.Folder
			srcID         string
			status        string
			srcCopyStatus string
		}
		var childFolders []dstChildFolder
		for _, child := range task.DiscoveredChildren {
			if !child.IsFile {
				f := child.Folder
				f.DepthLevel = nextRound
				childFolders = append(childFolders, dstChildFolder{
					folder:        f,
					srcID:         child.SrcID,
					status:        child.Status,
					srcCopyStatus: child.SrcCopyStatus,
				})
			}
		}

		for _, child := range childFolders {
			// Generate deterministic ID from (queueType, parentID, nodeType, basename)
			deterministicID := db.MintNodeID(queueType, nodeID, types.NodeTypeFolder, child.folder.DisplayName)
			idPath := db.JoinIDPath(parentPath, deterministicID)

			// Create task state for DST child folder
			taskState := &db.NodeState{
				ID:              deterministicID,
				ServiceID:       child.folder.ServiceID,
				ParentID:        nodeID,
				ParentServiceID: child.folder.ParentId,
				ParentPath:      parentPath,
				Name:            child.folder.DisplayName,
				DisplayPath:     filter.DisplayPathForChild(parentDisplayPath, currentRound, child.folder.DisplayName),
				Path:            idPath,
				Type:            types.NodeTypeFolder,
				Size:            0,
				MTime:           child.folder.LastUpdated,
				Depth:           nextRound,
				TraversalStatus: child.status,
			}
			if child.srcID != "" {
				taskState.SrcID = child.srcID
			}

			childNodesToInsert = append(childNodesToInsert, db.InsertOperation{
				QueueType: queueType,
				Level:     nextRound,
				Status:    child.status,
				State:     taskState,
			})

		}
	}

	// Push discovered children and completed-node status to appender buffer (async flush until round advance).
	if len(childNodesToInsert) > 0 {
		if queueType == "SRC" {
			states := make([]*db.NodeState, 0, len(childNodesToInsert))
			for _, op := range childNodesToInsert {
				if op.State != nil {
					states = append(states, op.State)
				}
			}
			parentLen := gpl.ParseGPLPathLen(task.GPLState)
			checkTarget := gpl.ResolvePathCheckTarget(q.ScalingSrcProvider(), q.ScalingDstProvider(), q.PathCheckProfile())
			skipChecks := checkTarget == ""
			gpl.ApplyGPLToSRCChildren(database, gpl.GPLTargetFromProvider(checkTarget), parentLen, states, skipChecks, q.WindowsCompat())
		}
		if err := database.AppendDiscoveredNodes(childNodesToInsert); err != nil {
			if logservice.LS != nil {
				logErr := logservice.LS.Log("error",
					fmt.Sprintf("discovery persist failed for %s (id=%s children=%d): %v",
						task.LocationPath(), nodeID, len(childNodesToInsert), err),
					"queue", q.Name(), q.Name())
				if logErr != nil {
					fmt.Println("error logging", logErr)
				}
			}
			task.LastError = fmt.Sprintf("discovery persist: %v", err)
			FailTraversalTask(q, task, 0)
			return
		}
		if task.IsFolder() && len(childNodesToInsert) > 0 {
			side := opsdb.SideSRC
			if queueType == "DST" {
				side = opsdb.SideDST
			}
			database.AppendKidsPackReplace(side, nodeID, srckidsFromInsertOps(childNodesToInsert))
		}
		for _, op := range childNodesToInsert {
			if op.State != nil && op.State.SrcID != "" {
				database.AppendIDMapEvent(db.IDMapEvent{
					SrcInternalID: op.State.SrcID,
					DstInternalID: op.State.ID,
					Source:        db.IDMapSourceDSTCompare,
					Status:        db.IDMapStatusActive,
					Depth:         op.Level,
				})
			}
		}
	}

	markTraversalComplete()

	prevTrav := task.SrcTraversalStatus
	if prevTrav == "" {
		prevTrav = db.StatusPending
	}
	nodeType := db.NodeTypeFolder
	if task.IsFile() {
		nodeType = db.NodeTypeFile
	}

	fromRetry := opsdb.IsDiscoveryRetryMark(task.ExclusionSource)
	database.AppendStatusEvent(queueType, db.StatusEvent{
		ID:                  nodeID,
		TraversalStatus:     db.StatusSuccessful,
		CopyStatus:          task.CopyStatus,
		EventTime:           time.Now().UnixNano(),
		Depth:               currentRound,
		PrevTraversalStatus: prevTrav,
		PrevCopyStatus:      task.CopyStatus,
		NodeType:            nodeType,
		// Carry mark so review deltas can restore parked copy/pending; seal clears it.
		ExclusionSource: task.ExclusionSource,
	}, fromRetry)

	// DST comparison: persist SRC copy status for each matched child so path review
	// and the copy frontier (pend:copy) stay aligned with copy_status.
	if q.Name() == "dst" {
		eventTime := time.Now().UnixNano()
		for _, child := range task.DiscoveredChildren {
			if child.SrcID == "" || child.SrcCopyStatus == "" {
				continue
			}
			meta := task.ExpectedSrcNodeMeta[child.SrcID]
			copyChanged := child.SrcCopyStatus != meta.CopyStatus
			if !copyChanged && !db.CopyStatusIsComplete(child.SrcCopyStatus) {
				continue
			}
			var size int64
			nodeType := db.NodeTypeFolder
			if child.IsFile {
				size = child.File.Size
				nodeType = db.NodeTypeFile
			}
			deleteStatus := ""
			if copyChanged && db.CopyStatusIsComplete(child.SrcCopyStatus) {
				deleteStatus = db.DeleteStatusAfterCopyComplete(task.SrcParentDeleteStatus, meta.DeleteStatus)
			}
			// Leave TraversalStatus empty so Badger status merge keeps the live value
			// (pull-time ExpectedSrcNodeMeta may still say pending after SRC completed).
			database.AppendStatusEvent("SRC", db.StatusEvent{
				ID:               child.SrcID,
				CopyStatus:       child.SrcCopyStatus,
				DeleteStatus:     deleteStatus,
				EventTime:        eventTime,
				Depth:            nextRound,
				PrevCopyStatus:   meta.CopyStatus,
				PrevDeleteStatus: meta.DeleteStatus,
				Size:             size,
				NodeType:         nodeType,
			}, false)
		}
		gpl.AppendDSTSiblingCollisionIssues(database, q, task)
	}

	// For SRC FOLDER tasks in retry mode: Re-queue DST task via status event and schedule DST child deletions.
	if q.Name() == "src" && fromRetry && task.IsFolder() && task.RetryDstCleanup != nil {
		c := task.RetryDstCleanup
		database.AppendStatusEvent("DST", db.StatusEvent{
			ID:                  c.DstID,
			TraversalStatus:     db.StatusPending,
			EventTime:           time.Now().UnixNano(),
			Depth:               task.Round,
			PrevTraversalStatus: c.DstOldStatus,
			NodeType:            db.NodeTypeFolder,
		}, false)
		deletions := make([]db.NodeDeletion, 0, len(c.Children))
		for _, ch := range c.Children {
			deletions = append(deletions, db.NodeDeletion{Table: "DST", NodeID: ch.ID})
		}
		if err := database.AddNodeDeletions(deletions); err != nil {
			fmt.Println("error adding node deletions", err)
		}
	}

	q.RemoveInProgress(nodeID)
}

// FailTraversalTask handles failure of traversal/retry tasks.
// Retries up to maxRetries, then marks as failed.
func FailTraversalTask(q *queue.Queue, task *queue.TaskBase, executionDelta time.Duration) {
	// Record execution time delta (even for failures)
	q.RecordExecutionTime(executionDelta)
	currentRound := task.Round
	nodeID := task.ID
	maxRetries := q.MaxRetries()

	if logservice.LS != nil {
		err := logservice.LS.Log("debug",
			fmt.Sprintf("Failing task: id=%s path=%s round=%d type=%s currentAttempts=%d maxRetries=%d",
				nodeID, task.LocationPath(), currentRound, task.Type, task.Attempts, maxRetries),
			"queue", q.Name(), q.Name())
		if err != nil {
			fmt.Println("error logging", err)
		}
	}

	task.Attempts++

	// Permanent FS / policy failures should not burn retry budget or wedge the round.
	if queue.IsNonRetryableTraversalError(task.LastError) {
		task.Attempts = maxRetries
	}

	// Check if we should retry
	if task.Attempts < maxRetries {
		task.Locked = false
		q.RemoveInProgress(nodeID)
		if !q.Add(task) {
			if logservice.LS != nil {
				_ = logservice.LS.Log("error",
					fmt.Sprintf("retry re-enqueue rejected for %s (id=%s) - task lost", task.LocationPath(), nodeID),
					"queue", q.Name(), q.Name())
			}
		}
		if logservice.LS != nil {
			err := logservice.LS.Log("debug",
				fmt.Sprintf("Retrying task: id=%s path=%s round=%d attempt=%d/%d",
					nodeID, task.LocationPath(), currentRound, task.Attempts, maxRetries),
				"queue", q.Name(), q.Name())
			if err != nil {
				fmt.Println("error logging", err)
			}
		}
		return // Will retry
	}

	// Max retries reached - task is truly done
	task.Locked = false
	task.Status = "failed"

	q.IncrementRoundStatsCompleted(currentRound)
	q.IncrementTasksCompletedTotal()
	q.RecordTaskCompletion(currentRound, false)

	if logservice.LS != nil {
		err := logservice.LS.Log("error",
			fmt.Sprintf("Failed to traverse folder %s (id=%s) after %d attempts (max retries exceeded) round=%d",
				task.LocationPath(), nodeID, task.Attempts, currentRound),
			"queue", q.Name(), q.Name())
		if err != nil {
			fmt.Println("error logging", err)
		}
	}

	database := q.Database()
	if database == nil {
		q.RemoveInProgress(nodeID)
		return
	}

	queueType := queue.GetQueueType(q.Name())

	// Record task error if present
	if task.LastError != "" {
		database.AppendTaskError(queueType, "traversal", nodeID, task.LastError, task.Attempts, task.LocationPath())
	}

	if nodeID != "" {
		prevTrav := task.SrcTraversalStatus
		if prevTrav == "" {
			prevTrav = db.StatusPending
		}
		nodeType := db.NodeTypeFolder
		if task.IsFile() {
			nodeType = db.NodeTypeFile
		}
		ev := db.StatusEvent{
			ID:                  nodeID,
			TraversalStatus:     db.StatusFailed,
			EventTime:           time.Now().UnixNano(),
			Depth:               currentRound,
			PrevTraversalStatus: prevTrav,
			NodeType:            nodeType,
		}
		failurelog.AttachTaskFailureLog(&ev, "traversal", q.Name(), nodeID, task.LocationPath(), task.Attempts, task.LastError)
		ev.ExclusionSource = task.ExclusionSource
		database.AppendStatusEvent(queueType, ev, opsdb.IsDiscoveryRetryMark(task.ExclusionSource))
	}

	q.RemoveInProgress(nodeID)
}

// CheckTraversalCompletion checks if traversal/retry phase should complete.
// Phase complete only when this round never yielded any work (ItemsYielded == 0) and the
// latest keyset pull was a terminal empty partial. An empty refill after the round already
// produced items means keyspace exhaustion → round advance, not phase complete.
func CheckTraversalCompletion(q *queue.Queue, currentRound int) bool {
	if q.State() == queue.QueueStateWaiting {
		return false
	}
	if coordinator := q.Coordinator(); coordinator != nil {
		if !coordinator.CanDstStartRound(currentRound) && q.Name() == "dst" {
			return false
		}
	}
	if !q.TraversalCacheLoaded() {
		return false
	}

	if !q.RoundHasCountedPull(currentRound) {
		if res := q.PullWithRetryIfNeeded(true); !res.OK() {
			return false
		}
	}
	info := q.RoundInfoReadOnly(currentRound)
	if info == nil {
		return false
	}

	mode := q.GetMode()

	switch mode {
	case queue.QueueModeTraversal:
		// Empty frontier for phase complete: last pull empty+partial AND this round never yielded.
		if !info.LastPartialPull || info.LastBatchYield != 0 || info.ItemsYielded != 0 {
			return false
		}
		return q.MarkComplete("No pending tasks found for round %d - traversal complete (empty frontier)", currentRound)
	case queue.QueueModeRetry:
		// Per algorithms.md: only apply "pull -> see nothing -> end" when currentRound >= maxKnownDepth.
		// Otherwise a round may have 0 retry items while deeper levels still do; return false so we advance the round.
		if !info.LastPartialPull || info.LastBatchYield != 0 || info.ItemsYielded != 0 {
			return false
		}
		maxKnownDepth := q.GetMaxKnownDepth()
		if maxKnownDepth >= 0 && currentRound < maxKnownDepth {
			return false
		}
		if maxKnownDepth >= 0 && currentRound > maxKnownDepth {
			return q.MarkComplete("Retry sweep complete - past maxKnownDepth (%d), no pending at round %d", maxKnownDepth, currentRound)
		}
		return q.MarkComplete("Retry sweep complete - no pending at round %d", currentRound)
	}
	return false
}

// AdvanceTraversalRound handles traversal/retry-specific round advancement logic.
// For traversal/retry modes, simply increments the round by 1.
func AdvanceTraversalRound(q *queue.Queue) {
	// Ensure state is running if it was waiting
	state := q.State()
	if state == queue.QueueStateWaiting {
		q.SetState(queue.QueueStateRunning)
	}

	// Advance round by 1 for traversal/retry modes
	currentRound := q.GetRound()
	newRound := currentRound + 1

	q.SetRound(newRound)
	q.SetExpectedFromStatsBucket(newRound)

	// Reset lastPullWasPartial since we're advancing to a new round
	q.SetLastPullWasPartial(false)
	// firstPullForRound is set to true in SetRound (above) so the new round gets "first pull" semantics for completion.
	// This queue's keyset cursor is reset in setRound / advanceToNextRound (resetThisQueueKeysetCursor).
	// Mid-round cursor updates only advance (never rewind) so concurrent pulls cannot re-lease earlier IDs.
	// Initialize RoundInfo for the new round (will be created on first pull)
	q.EnsureRoundInfo(newRound) // Ensure it exists

	// Update coordinator when rounds advance
	coordinator := q.Coordinator()
	if coordinator != nil {
		switch q.Name() {
		case "src":
			coordinator.UpdateRound("src", newRound)
		case "dst":
			coordinator.UpdateRound("dst", newRound)
		}
	}

	// Depth-scoped copy-work seal (durable denominators for progress monitor).
	q.FinalizeCopyWorkAfterTraversalRoundAdvance(newRound)

	if logservice.LS != nil {
		err := logservice.LS.Log("info", fmt.Sprintf("Advanced to round %d", newRound), "queue", q.Name(), q.Name())
		if err != nil {
			fmt.Println("error logging", err)
		}
	}

	// Pull tasks for the new round (must record a DB-committed pull for completion/advance gates).
	q.PullWithRetryIfNeeded(true)
}

func srckidsFromInsertOps(ops []db.InsertOperation) []opsdb.KidRecord {
	kids := make([]opsdb.KidRecord, 0, len(ops))
	for _, op := range ops {
		if op.State == nil {
			continue
		}
		n := op.State
		trav := n.TraversalStatus
		if trav == "" {
			trav = n.Status
		}
		kids = append(kids, opsdb.KidRecord{
			ID:              n.ID,
			ServiceID:       n.ServiceID,
			ParentServiceID: n.ParentServiceID,
			Path:            n.Path,
			ParentPath:      n.ParentPath,
			Name:            n.Name,
			Type:            n.Type,
			Size:            n.Size,
			MTime:           n.MTime,
			Depth:           n.Depth,
			TraversalStatus: trav,
			CopyStatus:      n.CopyStatus,
			DeleteStatus:    n.DeleteStatus,
		})
	}
	return kids
}
