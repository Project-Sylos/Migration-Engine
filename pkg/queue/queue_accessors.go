// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"context"
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
)

// ============================================================================
// Thread-safe getter/setter methods for queue state
// All locking is centralized here - logic functions should use these methods
// ============================================================================

// Getters (read-only, use RLock)

// State returns the current queue lifecycle state.
func (q *Queue) State() QueueState {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.state
}

// GetRound returns the current BFS round.
func (q *Queue) GetRound() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.round
}

// GetMode returns the current queue mode.
func (q *Queue) GetMode() QueueMode {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.mode
}

func (q *Queue) IsPulling() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.pulling
}

// TryBeginPulling atomically acquires the pull lock. Returns false if another pull is in flight.
// Pair with SetPulling(false) (typically via defer) after a successful acquire.
func (q *Queue) TryBeginPulling() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.pulling {
		return false
	}
	q.pulling = true
	return true
}


// GetCopyPass returns the current copy pass.
func (q *Queue) GetCopyPass() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.passNumber
}

// IncrementTasksCompletedTotal increments the queue's completed-task counter.
// Call once per task when it is marked successful or failed (past retries).
func (q *Queue) IncrementTasksCompletedTotal() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.tasksCompletedTotal++
}

// GetTasksCompletedTotal returns completed traversal/copy tasks (≈ FS op completions).
func (q *Queue) GetTasksCompletedTotal() int64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.tasksCompletedTotal
}

// SetCopyPass sets the current copy pass.
func (q *Queue) SetCopyPass(pass int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.passNumber = pass
}

// SetCopyResumeDstExistenceWindow enables resume existence handling until the anchor
// pass+round is left (see AdvanceCopyRound). Folder create still uses CreateFolderBatch:
// already-present folders are filtered via per-parent ListChildren before the batch RPC.
// File transfer keeps the single-task path while the window is active (byte-checkpoint resume).
// Only for normal copy mode (not copy-retry); call from RunCopyPhase when resuming a partially
// completed copy (Successful>0 and Pending>0 in status events).
func (q *Queue) SetCopyResumeDstExistenceWindow(anchorPass, anchorRound int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.name != "copy" {
		return
	}
	q.copyResumeDstExistenceActive = true
	q.copyResumeDstExistenceAnchorPass = anchorPass
	q.copyResumeDstExistenceAnchorRound = anchorRound
}

func (q *Queue) ShouldApplyCopyDstResumeExistenceCheck() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.mode != QueueModeCopy || !q.copyResumeDstExistenceActive {
		return false
	}
	return q.passNumber == q.copyResumeDstExistenceAnchorPass && q.round == q.copyResumeDstExistenceAnchorRound
}

// NoteCopyResumeDstExistenceLeavingAnchorRound clears resume dst precheck after the anchor round finishes
// (first AdvanceCopyRound call while still positioned on that pass+round).
func (q *Queue) NoteCopyResumeDstExistenceLeavingAnchorRound() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.copyResumeDstExistenceActive {
		return
	}
	if q.passNumber == q.copyResumeDstExistenceAnchorPass && q.round == q.copyResumeDstExistenceAnchorRound {
		q.copyResumeDstExistenceActive = false
	}
}

// SetWorkers sets the workers associated with this queue.
func (q *Queue) SetWorkers(workers []Worker) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.workers = workers
}

func (q *Queue) Coordinator() *QueueCoordinator {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.coordinator
}

func (q *Queue) Database() *db.DB {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.database
}

// SealIOWaitActive reports whether seal I/O is blocking (suppresses stall detection).
func (q *Queue) SealIOWaitActive() bool {
	d := q.Database()
	return d != nil && d.SealIOWaitActive()
}

func (q *Queue) ShutdownCtx() context.Context {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.shutdownCtx
}

// InProgressCount returns the number of tasks currently being executed.
func (q *Queue) InProgressCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.inProgress)
}

// GetPendingCount returns the number of tasks in the pending buffer.
func (q *Queue) GetPendingCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.pendingBuff)
}

func (q *Queue) GetLastPullWasPartial() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.lastPullWasPartial
}

// GetWorkerCount returns the number of workers registered with this queue.
func (q *Queue) GetWorkerCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.workers)
}

func (q *Queue) SetFirstPullForRound(value bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.firstPullForRound = value
}

func (q *Queue) MaxRetries() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.maxRetries
}

func (q *Queue) EffectiveLeaseBatchSize() int {
	q.mu.RLock()
	n := q.leaseBatchSize
	q.mu.RUnlock()
	if n <= 0 {
		return effectiveLeaseBatchSize()
	}
	if n > maxLeaseBatchSize {
		return maxLeaseBatchSize
	}
	return n
}

func (q *Queue) EffectiveRefillBatchSize() int {
	q.mu.RLock()
	n := q.refillBatchSize
	q.mu.RUnlock()
	if n <= 0 {
		return refillFromDBBatchSize
	}
	return n
}

// EffectivePullLowWM returns the low watermark for pulling more work from DuckDB into pendingBuff.
// Base WM is 25% of lease batch size (minimum 1). Pull decisions use pendingBuff length only —
// in-progress leases are not treated as available queue depth.
// Starve nudge: when any worker is idle and pendingCount < live activeWorkers, raise the
// effective WM to pending so underfed pools refill (pending <= WM) without waiting for the
// normal watermark. Especially matters when pending > base WM is false but still below worker count.
func (q *Queue) EffectivePullLowWM() int {
	bs := q.EffectiveLeaseBatchSize()
	wm := bs / 4
	if wm < 1 {
		wm = 1
	}
	pending := q.GetPendingCount()
	active := q.liveActiveWorkers()
	idle := q.IdleWorkerCount()
	if idle > 0 && pending < active {
		if pending > wm {
			return pending
		}
	}
	return wm
}

// IdleWorkerCount returns how many pool handles are currently marked idle.
func (q *Queue) IdleWorkerCount() int {
	q.pool.mu.Lock()
	defer q.pool.mu.Unlock()
	n := 0
	for _, h := range q.pool.handles {
		if h != nil && h.idle.Load() {
			n++
		}
	}
	return n
}

// Keyset cursors are strictly round-scoped per queue. Each queue (src, dst, copy) has its own cursor and runtime;
// we only reset the cursor for the queue that advanced or changed mode. Resets: round advance (setRound), mode switch (setMode).
// Mid-round setters only advance (never rewind) so a slower concurrent pull cannot move the cursor backwards.

func (q *Queue) keysetCursorPtrLocked() *string {
	switch q.name {
	case "src":
		return &q.srcKeysetCursor
	case "dst":
		return &q.dstKeysetCursor
	case "copy", "delete":
		return &q.copyKeysetCursor
	default:
		return nil
	}
}

func (q *Queue) GetKeysetCursor() string {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if cur := q.keysetCursorPtrLocked(); cur != nil {
		return *cur
	}
	return ""
}

// SetKeysetCursor advances this queue's keyset cursor. Never rewinds: a concurrent pull that
// started with an older afterID must not move the cursor backwards (ORDER BY id).
func (q *Queue) SetKeysetCursor(cursor string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	cur := q.keysetCursorPtrLocked()
	if cur == nil || cursor == "" || cursor <= *cur {
		return
	}
	*cur = cursor
}

func (q *Queue) clearKeysetCursorLocked() {
	if cur := q.keysetCursorPtrLocked(); cur != nil {
		*cur = ""
	}
}

// resetThisQueueKeysetCursor clears only this queue's keyset cursor. Call when this queue's round advances (setRound) or mode changes (setMode). Other queues' cursors are untouched—each queue has its own batching and runtime.
func (q *Queue) resetThisQueueKeysetCursor() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.clearKeysetCursorLocked()
}

func (q *Queue) GetFilesDiscoveredTotal() int64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.filesDiscoveredTotal
}

// SeedDiscoveryCounters restores traversal discovery totals from persisted queue metrics.
func (q *Queue) SeedDiscoveryCounters(files, folders int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.filesDiscoveredTotal = files
	q.foldersDiscoveredTotal = folders
}

// SeedCopyCounters restores copy-phase totals from persisted queue metrics.
// bytesFailed is permanently failed eligible file bytes (0 when unknown).
func (q *Queue) SeedCopyCounters(folders, files, bytes, bytesFailed int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.foldersCreatedTotal = folders
	q.filesCreatedTotal = files
	q.bytesTransferredTotal = bytes
	if bytesFailed > 0 {
		q.bytesFailedTotal = bytesFailed
	}
}

// RecordFailedBytes adds eligible file bytes for a permanent copy/delete failure (touched progress).
func (q *Queue) RecordFailedBytes(bytes int64) {
	if bytes <= 0 {
		return
	}
	q.mu.Lock()
	q.bytesFailedTotal += bytes
	q.mu.Unlock()
}

// GetBytesFailedTotal returns permanently failed eligible file bytes (excludes transferred bytes).
func (q *Queue) GetBytesFailedTotal() int64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.bytesFailedTotal
}

func (q *Queue) GetFoldersDiscoveredTotal() int64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.foldersDiscoveredTotal
}

// GetTotalDiscovered returns the total number of items discovered (files + folders).
func (q *Queue) GetTotalDiscovered() int64 {
	return q.GetFilesDiscoveredTotal() + q.GetFoldersDiscoveredTotal()
}

// GetBytesTransferredTotal returns the total bytes transferred during copy phase
// for completed tasks only (excludes in-flight leased progress).
func (q *Queue) GetBytesTransferredTotal() int64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.bytesTransferredTotal
}

// ReportTaskBytesTransferred records absolute mid-flight bytes on a leased task.
// Does not bump bytesTransferredTotal; observer uses GetLiveBytesTransferredTotal.
func (q *Queue) ReportTaskBytesTransferred(task *TaskBase, absoluteBytes int64) {
	if q == nil || task == nil || absoluteBytes < 0 {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	task.BytesTransferred = absoluteBytes
}

// GetLiveBytesTransferredTotal returns completed bytes plus in-flight task progress
// so the observer can show live throughput during long batch/single-file transfers.
func (q *Queue) GetLiveBytesTransferredTotal() int64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	var inFlight int64
	for _, task := range q.inProgress {
		if task != nil && task.BytesTransferred > 0 {
			inFlight += task.BytesTransferred
		}
	}
	return q.bytesTransferredTotal + inFlight
}

// GetFoldersCreatedTotal returns the total folders created during copy phase.
func (q *Queue) GetFoldersCreatedTotal() int64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.foldersCreatedTotal
}

// GetFilesCreatedTotal returns the total files created during copy phase.
func (q *Queue) GetFilesCreatedTotal() int64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.filesCreatedTotal
}

// GetTotalFailed returns the total number of failed tasks across all rounds.
func (q *Queue) GetTotalFailed() int {
	q.mu.RLock()
	defer q.mu.RUnlock()

	total := 0
	for _, roundInfo := range q.roundInfoMap {
		if roundInfo != nil {
			total += roundInfo.TasksFailed
		}
	}
	return total
}

// MemoryStatusTotals returns live pending/failed totals without touching DuckDB.
// RoundStats is the queue's authoritative live accounting during an active run.
func (q *Queue) MemoryStatusTotals() (pending, failed int) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	for _, stats := range q.roundStats {
		if stats == nil {
			continue
		}
		remaining := stats.Expected - stats.Completed
		if remaining > 0 {
			pending += remaining
		}
		failed += stats.Failed
	}
	return pending, failed
}

// SetWorkTotals installs copy/delete phase denominators loaded once at phase setup.
func (q *Queue) SetWorkTotals(t WorkTotals) {
	q.mu.Lock()
	q.workTotals = t
	q.mu.Unlock()
}

// GetWorkTotals returns the in-memory copy/delete phase denominator snapshot.
func (q *Queue) GetWorkTotals() WorkTotals {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.workTotals
}

// GetRoundStats returns the statistics for a specific round. Returns nil if the round has no stats yet.
func (q *Queue) GetRoundStats(round int) *RoundStats {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.roundStats[round]
}

func (q *Queue) getStatsChan() chan QueueStats {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.statsChan
}

func (q *Queue) getStatsTick() *time.Ticker {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.statsTick
}

// GetAverageExecutionTime returns the current average task execution time.
func (q *Queue) GetAverageExecutionTime() time.Duration {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.avgExecutionTime
}

// GetExecutionTimeDeltas returns a copy of the execution time deltas buffer.
func (q *Queue) GetExecutionTimeDeltas() []time.Duration {
	q.mu.RLock()
	defer q.mu.RUnlock()
	result := make([]time.Duration, len(q.executionTimeDeltas))
	copy(result, q.executionTimeDeltas)
	return result
}

func (q *Queue) getLastAvgTime() time.Time {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.lastAvgTime
}

func (q *Queue) getAvgInterval() time.Duration {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.avgInterval
}

// Setters (write, use Lock)

// SetState sets the queue lifecycle state.
func (q *Queue) SetState(state QueueState) {
	q.mu.Lock()
	q.state = state
	watchdog := q.watchdog
	q.mu.Unlock()

	// Stop watchdog when queue completes or stops
	if (state == QueueStateCompleted || state == QueueStateStopped) && watchdog != nil {
		watchdog.Stop()
	}
}

func (q *Queue) resetPullScopeLocked() {
	q.firstPullForRound = true
	q.clearKeysetCursorLocked()
}

// SetRound sets the queue's current round. Used for resume operations.
func (q *Queue) SetRound(round int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.round = round
	q.resetPullScopeLocked()
}

// SetMode sets the queue mode (traversal, retry, or copy).
func (q *Queue) SetMode(mode QueueMode) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.mode = mode
	q.resetPullScopeLocked()
}

func (q *Queue) SetPulling(pulling bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pulling = pulling
}

// SetMaxKnownDepth sets the maximum depth for traversal/copy. Set to -1 to auto-detect.
func (q *Queue) SetMaxKnownDepth(depth int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.maxKnownDepth = depth
}

// GetMaxKnownDepth returns the configured max depth (-1 if unset / auto).
func (q *Queue) GetMaxKnownDepth() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.maxKnownDepth
}

// EnsureRoundInfo returns the RoundInfo for the specified round, creating it if it doesn't exist.
func (q *Queue) EnsureRoundInfo(round int) *RoundInfo {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.roundInfoMap[round] == nil {
		q.roundInfoMap[round] = &RoundInfo{
			Round:     round,
			StartTime: time.Now(),
		}
	}
	return q.roundInfoMap[round]
}

// RoundInfoReadOnly returns a read-only copy of RoundInfo for the specified round.
// Returns nil if the round doesn't exist yet.
func (q *Queue) RoundInfoReadOnly(round int) *RoundInfo {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.roundInfoMap[round] == nil {
		return nil
	}

	// Return a copy to prevent external mutation
	info := *q.roundInfoMap[round]
	return &info
}

// RecordPull records a pull operation for the current round.
func (q *Queue) RecordPull(round int, itemsYielded int, wasPartial bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.roundInfoMap[round] == nil {
		q.roundInfoMap[round] = &RoundInfo{
			Round:     round,
			StartTime: time.Now(),
		}
	}

	info := q.roundInfoMap[round]
	info.PullCount++
	info.LastBatchYield = itemsYielded
	info.ItemsYielded += itemsYielded
	info.LastPullTime = time.Now()
	info.LastPartialPull = wasPartial

	// Calculate avg tasks/sec if we have a start time
	if !info.StartTime.IsZero() {
		elapsed := time.Since(info.StartTime).Seconds()
		if elapsed > 0 {
			info.AvgTasksPerSec = float64(info.TasksCompleted) / elapsed
		}
	}
}

// RecordTaskCompletion records a completed task for the current round.
func (q *Queue) RecordTaskCompletion(round int, success bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.roundInfoMap[round] == nil {
		q.roundInfoMap[round] = &RoundInfo{
			Round:     round,
			StartTime: time.Now(),
		}
	}

	info := q.roundInfoMap[round]
	if success {
		info.TasksCompleted++
	} else {
		info.TasksFailed++
	}

	// Update avg tasks/sec
	if !info.StartTime.IsZero() {
		elapsed := time.Since(info.StartTime).Seconds()
		if elapsed > 0 {
			info.AvgTasksPerSec = float64(info.TasksCompleted) / elapsed
		}
	}
}

// SetTraversalCacheLoaded sets whether we have completed the first pull for the current round.
// Until true, CheckTraversalCompletion returns false so the queue does not complete before the first pull.
func (q *Queue) SetTraversalCacheLoaded(loaded bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.traversalCacheLoaded = loaded
}

func (q *Queue) TraversalCacheLoaded() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.traversalCacheLoaded
}

func (q *Queue) SetLastPullWasPartial(value bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.lastPullWasPartial = value
}

// SetShutdownContext sets the shutdown context for the queue.
func (q *Queue) SetShutdownContext(ctx context.Context) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.shutdownCtx = ctx
}

func (q *Queue) SetDatabase(database *db.DB) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.database = database
}

func (q *Queue) setStatsChan(ch chan QueueStats) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.statsChan = ch
}

func (q *Queue) setStatsTick(tick *time.Ticker) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.statsTick = tick
}

func (q *Queue) setAvgExecutionTime(duration time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.avgExecutionTime = duration
}

func (q *Queue) setLastAvgTime(t time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.lastAvgTime = t
}

func (q *Queue) AddInProgress(nodeID string, task *TaskBase) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.inProgress[nodeID] = task
}

func (q *Queue) RemoveInProgress(nodeID string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.inProgress, nodeID)
}

func (q *Queue) HasInProgress(nodeID string) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	_, ok := q.inProgress[nodeID]
	return ok
}

func (q *Queue) IncrementRoundStatsCompleted(round int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	stats := q.getOrCreateRoundStatsUnlocked(round)
	stats.Completed++
}

// ResetRoundStatsCompleted zeros Completed for all rounds. Call when switching copy pass
// so pass 2 stats (files) don't include pass 1 completions (folders).
func (q *Queue) ResetRoundStatsCompleted() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, stats := range q.roundStats {
		if stats != nil {
			stats.Completed = 0
		}
	}
}

func (q *Queue) IncrementRoundStatsFailed(round int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	stats := q.getOrCreateRoundStatsUnlocked(round)
	stats.Failed++
}

// SetRoundStatsCounts sets Expected/Completed/Failed for a round (tests and resume bookkeeping).
func (q *Queue) SetRoundStatsCounts(round, expected, completed, failed int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	stats := q.getOrCreateRoundStatsUnlocked(round)
	stats.Expected = expected
	stats.Completed = completed
	stats.Failed = failed
}

// getOrCreateRoundStatsUnlocked is a helper used internally by other locked methods
func (q *Queue) getOrCreateRoundStatsUnlocked(round int) *RoundStats {
	if q.roundStats[round] == nil {
		q.roundStats[round] = &RoundStats{}
	}
	return q.roundStats[round]
}

// SetExpectedFromStatsBucket sets roundStats[round].Expected from the live DB count (or stats for init/resume).
// For traversal/retry: always compute from live pending count when advancing, to avoid stale stats from prior runs.
// For copy: compute from live. For init (EnsureRoundExpectedFromStats): try stats first for resume, else compute.
func (q *Queue) SetExpectedFromStatsBucket(round int) {
	database := q.Database()
	if database == nil {
		return
	}
	queueType := GetQueueType(q.name)
	mode := q.GetMode()
	var expected int64
	var err error

	// For traversal/retry, always compute from live count when advancing (no stale stats from prior runs).
	// EnsureRoundExpectedFromStats (init/resume) still goes through this; we compute for traversal there too.
	switch mode {
	case QueueModeTraversal:
		if round == 0 {
			expected = 1
		} else {
			expected, err = stats.GetTraversalCountAtDepthFromLive(database, queueType, round, db.StatusPending)
			if err != nil {
				fmt.Println("error getting pending traversal count at depth from live", err)
				return
			}
		}
	case QueueModeRetry:
		expected, err = stats.GetTraversalCountAtDepthFromLive(database, queueType, round, db.StatusPending)
		if err != nil {
			fmt.Println("error getting pending traversal count at depth from live", err)
			return
		}
	case QueueModeGPL:
		expected, err = stats.GetGPLCountAtDepthFromLive(database, queueType, round, db.GPLStatusPending)
		if err != nil {
			fmt.Println("error getting pending gpl count at depth from live", err)
			return
		}
	case QueueModeCopy:
		copyPass := q.GetCopyPass()
		nodeType := db.NodeTypeFolder
		if copyPass == 2 {
			nodeType = db.NodeTypeFile
		}
		expected, err = stats.GetCopyCountAtDepth(database, round, nodeType, db.CopyStatusPending, false)
		if err != nil {
			fmt.Println("error getting copy count at depth", err)
			return
		}
	case QueueModeCopyRetry:
		copyPass := q.GetCopyPass()
		nodeType := db.NodeTypeFolder
		if copyPass == 2 {
			nodeType = db.NodeTypeFile
		}
		expected, err = stats.GetCopyCountAtDepth(database, round, nodeType, db.CopyStatusFailed, false)
		if err != nil {
			fmt.Println("error getting copy count at depth", err)
			return
		}
	case QueueModeDelete:
		deletePass := q.GetCopyPass()
		nodeType := db.NodeTypeFile
		if deletePass == 2 {
			nodeType = db.NodeTypeFolder
		}
		expected, err = stats.GetDeleteCountAtDepth(database, round, nodeType, db.DeleteStatusPending, false)
		if err != nil {
			fmt.Println("error getting delete count at depth", err)
			return
		}
	case QueueModeDeleteRetry:
		deletePass := q.GetCopyPass()
		nodeType := db.NodeTypeFile
		if deletePass == 2 {
			nodeType = db.NodeTypeFolder
		}
		expected, err = stats.GetDeleteCountAtDepth(database, round, nodeType, db.DeleteStatusFailed, false)
		if err != nil {
			fmt.Println("error getting delete count at depth", err)
			return
		}
	default:
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	stats := q.getOrCreateRoundStatsUnlocked(round)
	stats.Expected = int(expected)
}

func (q *Queue) appendExecutionTimeDelta(delta time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	// Add to buffer (max 100 items)
	if len(q.executionTimeDeltas) >= 100 {
		// Remove oldest item (FIFO)
		q.executionTimeDeltas = q.executionTimeDeltas[1:]
	}
	q.executionTimeDeltas = append(q.executionTimeDeltas, delta)
}

func (q *Queue) clearExecutionTimeDeltas() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.executionTimeDeltas = q.executionTimeDeltas[:0]
}

// Batch operations

// StateSnapshot returns a snapshot of queue state for use in logic functions
type QueueStateSnapshot struct {
	State              QueueState
	Round              int
	PendingCount       int
	InProgressCount    int
	Pulling            bool
	LastPullWasPartial bool
	FirstPullForRound  bool
	PullLowWM          int
	Database           *db.DB
	Mode               QueueMode
}

func (q *Queue) StateSnapshot() QueueStateSnapshot {
	q.mu.RLock()
	defer q.mu.RUnlock()
	leaseCap := effectiveLeaseBatchSize()
	if q.leaseBatchSize > 0 {
		leaseCap = q.leaseBatchSize
		if leaseCap > maxLeaseBatchSize {
			leaseCap = maxLeaseBatchSize
		}
	}
	wm := leaseCap / 4
	if wm < 1 {
		wm = 1
	}
	return QueueStateSnapshot{
		State:              q.state,
		Round:              q.round,
		PendingCount:       len(q.pendingBuff),
		InProgressCount:    len(q.inProgress),
		Pulling:            q.pulling,
		LastPullWasPartial: q.lastPullWasPartial,
		FirstPullForRound:  q.firstPullForRound,
		PullLowWM:          wm,
		Database:           q.database,
		Mode:               q.mode,
	}
}

func (q *Queue) recordDequeueSkip(reason string, currentRound int) {
	q.mu.Lock()
	switch reason {
	case "old_round":
		q.dequeueSkipOldRound++
	case "empty_id":
		q.dequeueSkipEmptyID++
	case "already_in_progress":
		q.dequeueSkipInProgress++
	}
	shouldLog := false
	queueRound := q.round
	state := q.state
	workers := len(q.workers)
	pendingBuf := len(q.pendingBuff)
	inProgress := len(q.inProgress)
	oldRoundSkips := q.dequeueSkipOldRound
	emptyIDSkips := q.dequeueSkipEmptyID
	inProgressSkips := q.dequeueSkipInProgress
	if q.name == "dst" && q.coordinator != nil && q.coordinator.IsCompleted("src") {
		now := time.Now()
		if q.dequeueDebugLastLogAt.IsZero() || now.Sub(q.dequeueDebugLastLogAt) >= 2*time.Second {
			q.dequeueDebugLastLogAt = now
			shouldLog = true
			q.dequeueSkipOldRound = 0
			q.dequeueSkipEmptyID = 0
			q.dequeueSkipInProgress = 0
		}
	}
	q.mu.Unlock()
	if shouldLog {
		fmt.Printf("[dequeue-skip] queue=%s currentRound=%d queueRound=%d state=%s workers=%d pendingBuf=%d inProgress=%d oldRound=%d emptyID=%d alreadyInProgress=%d\n",
			q.name, currentRound, queueRound, state, workers, pendingBuf, inProgress, oldRoundSkips, emptyIDSkips, inProgressSkips)
	}
}

// Add enqueues a task into the pending buffer. Returns false if task is nil, has empty ID, or is already in progress.
func (q *Queue) Add(task *TaskBase) bool {
	if task == nil {
		return false
	}
	nodeID := task.ID
	if nodeID == "" {
		// With deterministic IDs, task.ID should always be pre-computed
		// This is a programming error if we reach here
		if logservice.LS != nil {
			err := logservice.LS.Log("error",
				"enqueuePending called with empty task.ID - this indicates a bug in ID generation",
				"queue", q.name, q.name)
			if err != nil {
				fmt.Println("error logging", err)
			}
		}
		return false
	}

	// Atomically check and add in a single lock
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, exists := q.inProgress[nodeID]; exists {
		return false
	}

	q.pendingBuff = append(q.pendingBuff, task)
	return true
}

// dequeuePending atomically dequeues a task from the pending buffer
func (q *Queue) dequeuePending() *TaskBase {
	for {
		q.mu.Lock()
		if len(q.pendingBuff) == 0 {
			q.mu.Unlock()
			return nil
		}
		task := q.pendingBuff[0]
		q.pendingBuff = q.pendingBuff[1:]
		nodeID := task.ID
		if nodeID == "" {
			// With deterministic IDs, task.ID should always be pre-computed
			// Skip this task and log an error
			if logservice.LS != nil {
				err := logservice.LS.Log("error",
					"dequeuePending found task with empty ID - this indicates a bug in ID generation",
					"queue", q.name, q.name)
				if err != nil {
					fmt.Println("error logging", err)
				}
			}
			q.mu.Unlock()
			q.recordDequeueSkip("empty_id", 0)
			continue
		}

		// Check if task is for current or future round
		currentRound := q.round
		// Check if already in progress
		_, inProgress := q.inProgress[nodeID]
		q.mu.Unlock()

		// Lease tasks even when task.Round < queue round (buffered work from before round advance).
		if inProgress {
			q.recordDequeueSkip("already_in_progress", currentRound)
			continue
		}

		return task
	}
}
