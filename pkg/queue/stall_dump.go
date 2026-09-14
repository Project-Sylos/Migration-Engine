// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"fmt"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

type stallTaskSnap struct {
	id           string
	owner        string
	epoch        uint64
	taskType     string
	nodeKind     string
	name         string
	idPath       string
	srcLogical   string
	round        int
	copyPass     int
	attempts     int
	leaseAge     time.Duration
	locked       bool
	workerResult string
	status       string
	lastError    string
}

// DumpStallState prints in-progress and pending task diagnostics for the queue watchdog.
func (q *Queue) DumpStallState(stalledFor time.Duration, inProgress, pending int) {
	fmt.Printf("\n")
	fmt.Printf("========================================\n")
	fmt.Printf("QUEUE WATCHDOG: STALL DETECTED\n")
	fmt.Printf("========================================\n")
	fmt.Printf("Queue: %s\n", q.Name())
	fmt.Printf("Stalled for: %v\n", stalledFor.Round(time.Second))
	fmt.Printf("State: %s\n", q.State())
	fmt.Printf("Mode: %s\n", q.GetMode())
	fmt.Printf("Round: %d\n", q.GetRound())
	fmt.Printf("Pending: %d\n", pending)
	fmt.Printf("InProgress: %d\n", inProgress)
	fmt.Printf("LastPullWasPartial: %v\n", q.GetLastPullWasPartial())
	fmt.Printf("Workers: %d\n", q.GetWorkerCount())

	round := q.GetRound()
	if stats := q.GetRoundStats(round); stats != nil {
		fmt.Printf("RoundStats[%d]: Expected=%d Completed=%d Failed=%d\n",
			round, stats.Expected, stats.Completed, stats.Failed)
	}

	inSnaps, pendSnaps := q.snapshotStallTasks()
	displayPaths := q.displayPathsForSnaps(inSnaps, pendSnaps)

	fmt.Printf("\n--- IN-PROGRESS TASKS ---\n")
	if len(inSnaps) == 0 {
		fmt.Printf("  (none)\n")
	} else {
		for _, s := range inSnaps {
			printStallTask("  ", s, displayPaths[s.id])
		}
	}

	fmt.Printf("\n--- PENDING TASKS (first 5) ---\n")
	if len(pendSnaps) == 0 {
		fmt.Printf("  (none)\n")
	} else {
		for i, s := range pendSnaps {
			prefix := fmt.Sprintf("  [%d] ", i)
			printStallTask(prefix, s, displayPaths[s.id])
		}
		pendingTotal := q.GetPendingCount()
		if pendingTotal > len(pendSnaps) {
			fmt.Printf("  ... and %d more\n", pendingTotal-len(pendSnaps))
		}
	}

	fmt.Printf("========================================\n\n")
}

func (q *Queue) snapshotStallTasks() (inProgress, pending []stallTaskSnap) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	inProgress = make([]stallTaskSnap, 0, len(q.inProgress))
	for id, f := range q.inProgress {
		task := f.task
		if task == nil {
			continue
		}
		inProgress = append(inProgress, stallSnapFromTask(id, f.owner, f.epoch, task))
	}

	count := len(q.pendingBuff)
	if count > 5 {
		count = 5
	}
	pending = make([]stallTaskSnap, 0, count)
	for i := 0; i < count; i++ {
		task := q.pendingBuff[i]
		if task == nil {
			continue
		}
		pending = append(pending, stallSnapFromTask(task.ID, "", 0, task))
	}
	return inProgress, pending
}

func stallSnapFromTask(id, owner string, epoch uint64, task *TaskBase) stallTaskSnap {
	nodeKind := "file"
	if task.IsFolder() {
		nodeKind = "folder"
	}
	workerResult := task.WorkerResult
	if workerResult == "" {
		workerResult = "(executing)"
	}
	return stallTaskSnap{
		id:           id,
		owner:        owner,
		epoch:        epoch,
		taskType:     task.Type,
		nodeKind:     nodeKind,
		name:         task.DisplayName(),
		idPath:       task.LocationPath(),
		srcLogical:   task.SrcLogicalPath,
		round:        task.Round,
		copyPass:     task.CopyPass,
		attempts:     task.Attempts,
		leaseAge:     time.Since(task.LeaseTime).Round(time.Second),
		locked:       task.Locked,
		workerResult: workerResult,
		status:       task.Status,
		lastError:    task.LastError,
	}
}

func (q *Queue) displayPathsForSnaps(snaps ...[]stallTaskSnap) map[string]string {
	out := make(map[string]string)
	database := q.Database()
	queueType := GetQueueType(q.Name())
	if database == nil || queueType == "" {
		return out
	}
	var ids []string
	seen := make(map[string]struct{})
	for _, group := range snaps {
		for _, s := range group {
			if s.id == "" {
				continue
			}
			if _, ok := seen[s.id]; ok {
				continue
			}
			seen[s.id] = struct{}{}
			ids = append(ids, s.id)
		}
	}
	if len(ids) == 0 {
		return out
	}
	paths, err := db.ComposeDisplayPaths(database, queueType, ids)
	if err != nil || paths == nil {
		return out
	}
	return paths
}

func printStallTask(prefix string, s stallTaskSnap, displayPath string) {
	fmt.Printf("%sID: %s\n", prefix, s.id)
	if s.name != "" {
		fmt.Printf("%s  Name: %s\n", prefix, s.name)
	}
	if displayPath != "" {
		fmt.Printf("%s  DisplayPath: %s\n", prefix, displayPath)
	}
	if s.idPath != "" {
		fmt.Printf("%s  IdPath: %s\n", prefix, s.idPath)
	}
	if s.srcLogical != "" && s.srcLogical != s.idPath && s.srcLogical != displayPath {
		fmt.Printf("%s  SrcLogicalPath: %s\n", prefix, s.srcLogical)
	}
	fmt.Printf("%s  Type: %s\n", prefix, s.nodeKind)
	if s.taskType != "" {
		fmt.Printf("%s  TaskType: %s\n", prefix, s.taskType)
	}
	fmt.Printf("%s  Round: %d\n", prefix, s.round)
	fmt.Printf("%s  CopyPass: %d\n", prefix, s.copyPass)
	fmt.Printf("%s  Attempts: %d\n", prefix, s.attempts)
	fmt.Printf("%s  LeaseAge: %v\n", prefix, s.leaseAge)
	fmt.Printf("%s  Locked: %v\n", prefix, s.locked)
	if s.owner != "" {
		fmt.Printf("%s  Owner: %s epoch=%d\n", prefix, s.owner, s.epoch)
	}
	fmt.Printf("%s  WorkerResult: %s\n", prefix, s.workerResult)
	fmt.Printf("%s  Status: %s\n", prefix, s.status)
	if s.lastError != "" {
		fmt.Printf("%s  LastError: %s\n", prefix, s.lastError)
	}
}

// DumpCompletionStall prints diagnostics when the queue is idle but not advancing.
func (q *Queue) DumpCompletionStall(stalledFor time.Duration) {
	round := q.GetRound()
	info := q.RoundInfoReadOnly(round)
	fmt.Printf("\n")
	fmt.Printf("========================================\n")
	fmt.Printf("QUEUE WATCHDOG: COMPLETION STALL\n")
	fmt.Printf("========================================\n")
	fmt.Printf("Queue: %s\n", q.Name())
	fmt.Printf("Stalled for: %v\n", stalledFor.Round(time.Second))
	fmt.Printf("State: %s\n", q.State())
	fmt.Printf("Mode: %s\n", q.GetMode())
	fmt.Printf("Round: %d\n", round)
	fmt.Printf("Pending: 0 | InProgress: 0\n")
	fmt.Printf("Pulling: %v\n", q.IsPulling())
	fmt.Printf("WaitingOnDB: %v\n", q.IsWaitingOnDB())
	fmt.Printf("LastPullWasPartial: %v\n", q.GetLastPullWasPartial())
	if info != nil {
		fmt.Printf("RoundInfo[%d]: PullCount=%d LastPartialPull=%v LastBatchYield=%d\n",
			round, info.PullCount, info.LastPartialPull, info.LastBatchYield)
	} else {
		fmt.Printf("RoundInfo[%d]: (none)\n", round)
	}
	fmt.Printf("HasCountedPull: %v\n", q.RoundHasCountedPull(round))
	fmt.Printf("========================================\n\n")
}
