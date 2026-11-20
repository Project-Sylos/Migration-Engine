// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/gpl"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// TraversalWorker executes traversal tasks by listing children and recording them to DB.
// Each worker runs independently in its own goroutine, continuously polling the queue for work.
type TraversalWorker struct {
	workerBase
	fsAdapter types.FSAdapter
	queueName string
	isDst     bool
}

func NewTraversalWorker(
	id string,
	q *queue.Queue,
	adapter types.FSAdapter,
	queueName string,
	shutdownCtx context.Context,
	workerCtx context.Context,
	idle *atomic.Bool,
	retire *atomic.Bool,
) *TraversalWorker {
	w := &TraversalWorker{
		workerBase: newWorkerBase(id, q, shutdownCtx, workerCtx, idle, retire),
		fsAdapter:  adapter,
		queueName:  queueName,
		isDst:      queueName == "dst",
	}
	w.loop = loopSpec{
		label:     "queue.Worker",
		queueName: queueName,
		execute:   w.execute,
		logFailure: func(task *queue.TaskBase, err error) {
			w.logError(task, err, task.Attempts < w.queue.MaxRetries() && !queue.IsNonRetryableTraversalError(err.Error()))
		},
	}
	return w
}

// execute performs the actual traversal work.
// It populates task.DiscoveredChildren instead of writing directly to DB.
func (w *TraversalWorker) execute(task *queue.TaskBase) error {
	if w.queue.GetMode() == queue.QueueModeGPL {
		return w.executeGPL(task)
	}
	if !task.IsFolder() {
		return fmt.Errorf("traversal worker received non-folder task")
	}

	// Prefer workerCtx so scale-down / stop force-checkout aborts mid-ListChildren; fall back to shutdown.
	parent := fsOpContext(w.workerCtx, w.shutdownCtx)
	// One Beat at watchdog creation, then a hard wall-clock cancel. No mid-list fake
	// beats and no seal suppress (seal must not hide kernel blackholes). Rate-limit
	// windows still freeze the timer so provider backoff is not treated as a hang.
	wd, ctx := observe.NewProgressWatchdog(parent, traversalStallTimeout, progressRateLimitSuppress(w.queue))
	defer wd.Stop()

	w.queue.SetActiveLeaseSize(w.id, 0)
	defer w.queue.ClearActiveLeaseSize(w.id)

	w.queue.WaitInterOp(w.workerCtx)

	folder := task.Folder
	if logservice.LS != nil {
		_ = logservice.LS.LogStructured("trace",
			fmt.Sprintf("traversal list: path=%s", task.DisplayPath),
			"worker", w.id,
			logservice.LogFields{Action: "traversal", NodeID: task.ID, Queue: w.queueName})
	}
	depth := folder.DepthLevel
	out, ok := awaitCancellable(w.queue, ctx, func() fsOut[types.ListResult] {
		d := depth
		r, err := w.fsAdapter.ListChildren(ctx, folder.ServiceID, &d, folder.LocationPath)
		return fsOut[types.ListResult]{Val: r, Err: err}
	})
	if !ok {
		if parent.Err() != nil || (w.shutdownCtx != nil && w.shutdownCtx.Err() != nil) {
			return errTransferAbandoned
		}
		return fmt.Errorf("%w: list children of %s stalled after %s", errTraversalStalled, task.DisplayPath, traversalStallTimeout)
	}
	if out.Err != nil {
		err := out.Err
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			if parent.Err() != nil || (w.shutdownCtx != nil && w.shutdownCtx.Err() != nil) {
				return errTransferAbandoned
			}
			return fmt.Errorf("%w: list children of %s stalled after %s", errTraversalStalled, task.DisplayPath, traversalStallTimeout)
		}
		if logservice.LS != nil {
			_ = logservice.LS.Log("error",
				fmt.Sprintf("Failed to list children: path=%s folderId=%s error=%v",
					task.DisplayPath, folder.ServiceID, err),
				"worker", w.id, w.queueName)
		}
		return fmt.Errorf("failed to list children of %s: %w", task.DisplayPath, err)
	}
	result := out.Val
	wd.Beat()
	if w.queue.HasWatchdog() {
		w.queue.BeatWatchdog()
	}

	w.queue.RecordListFill(len(result.Folders) + len(result.Files))

	const defaultPage = 100
	pageSize := w.queue.GetListPageSize()
	if pageSize <= 0 {
		pageSize = defaultPage
	}
	pager := types.NewListPager(result, pageSize)

	if w.isDst {
		aggregated := types.ListResult{}
		for {
			page, ok := pager.Next()
			if !ok {
				break
			}
			wd.Beat()
			aggregated.Folders = append(aggregated.Folders, page.Folders...)
			aggregated.Files = append(aggregated.Files, page.Files...)
		}
		return w.executeDstComparison(task, aggregated, wd)
	}

	task.DiscoveredChildren = make([]queue.ChildResult, 0, len(result.Folders)+len(result.Files))

	for {
		page, ok := pager.Next()
		if !ok {
			break
		}
		wd.Beat()

		for _, childFolder := range page.Folders {
			childFolder.DepthLevel = task.Round + 1
			task.DiscoveredChildren = append(task.DiscoveredChildren, queue.ChildResult{
				Folder: childFolder,
				Status: db.StatusPending,
				IsFile: false,
			})
		}

		for _, childFile := range page.Files {
			childFile.DepthLevel = task.Round + 1
			task.DiscoveredChildren = append(task.DiscoveredChildren, queue.ChildResult{
				File:   childFile,
				Status: db.StatusSuccessful,
				IsFile: true,
			})
		}
	}

	w.queue.NoteDiscoveredChildren(task, len(task.DiscoveredChildren))
	return nil
}

// executeDstComparison performs comparison between expected (src) and actual (dst) children.
// It populates task.DiscoveredChildren with comparison results.
// Matching uses Type + bare child name derived from LocationPath (adapters may put a full path in DisplayName).
func (w *TraversalWorker) executeDstComparison(task *queue.TaskBase, actualResult types.ListResult, wd *observe.ProgressWatchdog) error {
	wd.Beat()
	// Extract expected children from task (populated by queue)
	expectedFolders := task.ExpectedFolders
	expectedFiles := task.ExpectedFiles
	srcIDMap := task.ExpectedSrcIDMap
	if srcIDMap == nil {
		srcIDMap = make(map[string]string)
	}

	task.DiscoveredChildren = make([]queue.ChildResult, 0)

	// Build maps for quick lookup by canonical Type:Name match key.
	actualFolderMap := make(map[string]types.Folder)
	for _, f := range actualResult.Folders {
		// Override adapter-provided depth with BFS depth based on current round.
		f.DepthLevel = task.Round + 1
		matchKey := queue.DstChildMatchKey(f.Type, f.DisplayName, f.LocationPath)
		actualFolderMap[matchKey] = f
	}

	actualFileMap := make(map[string]types.File)
	for _, f := range actualResult.Files {
		// Override adapter-provided depth with BFS depth based on current round.
		f.DepthLevel = task.Round + 1
		matchKey := queue.DstChildMatchKey(f.Type, f.DisplayName, f.LocationPath)
		actualFileMap[matchKey] = f
	}

	expectedFolderMap := make(map[string]types.Folder)
	for _, f := range expectedFolders {
		matchKey := queue.DstChildMatchKey(f.Type, f.DisplayName, f.LocationPath)
		expectedFolderMap[matchKey] = f
	}

	expectedFileMap := make(map[string]types.File)
	for _, f := range expectedFiles {
		matchKey := queue.DstChildMatchKey(f.Type, f.DisplayName, f.LocationPath)
		expectedFileMap[matchKey] = f
	}

	// Compare folders by canonical Type:Name key.
	for _, expectedFolder := range expectedFolders {
		matchKey := queue.DstChildMatchKey(expectedFolder.Type, expectedFolder.DisplayName, expectedFolder.LocationPath)
		if actualFolder, exists := actualFolderMap[matchKey]; exists {

			// Get SRC node ID from map
			srcID := srcIDMap[matchKey]

			// Folder exists on both: never a copy task for this migration.
			task.DiscoveredChildren = append(task.DiscoveredChildren, queue.ChildResult{
				Folder:        actualFolder,
				Status:        db.StatusPending,
				IsFile:        false,
				SrcID:         srcID,
				SrcCopyStatus: db.CopyStatusAlreadyExisted,
				DstCopyStatus: db.CopyStatusAlreadyExisted,
			})
		}
	}

	// Rematch: cleaned/proposed SRC basename → unmatched DST sibling (already_existed + id_map).
	rematchDSTChildren(task, expectedFolders, actualFolderMap, expectedFolderMap, srcIDMap)

	// Check for extra folders on dst (not on src)
	for _, actualFolder := range actualResult.Folders {
		matchKey := queue.DstChildMatchKey(actualFolder.Type, actualFolder.DisplayName, actualFolder.LocationPath)
		if _, exists := expectedFolderMap[matchKey]; !exists {
			if rematchedDSTKey(task, matchKey) {
				continue
			}
			// Folder exists on dst but not src: still walk so nested DST-only kids are visible.
			task.DiscoveredChildren = append(task.DiscoveredChildren, queue.ChildResult{
				Folder:        actualFolder,
				Status:        db.StatusPending,
				IsFile:        false,
				SrcID:         "", // No SRC node; no copy enrollment
				DstCopyStatus: db.CopyStatusDestinationOnly,
			})
		}
	}

	// Compare files by canonical Type:Name key.
	policy := w.queue.OverwritePolicy()
	hashVerify := w.queue.HashVerify()
	peerSrc := w.queue.TraversalPeerAdapter()
	parent := fsOpContext(w.workerCtx, w.shutdownCtx)
	for _, expectedFile := range expectedFiles {
		matchKey := queue.DstChildMatchKey(expectedFile.Type, expectedFile.DisplayName, expectedFile.LocationPath)
		if actualFile, exists := actualFileMap[matchKey]; exists {
			srcID := srcIDMap[matchKey]
			outcome := resolveFileOverwrite(policy, expectedFile, actualFile)

			if hashVerify && peerSrc != nil && expectedFile.ServiceID != "" && actualFile.ServiceID != "" {
				display := expectedFile.DisplayName
				if display == "" {
					display = actualFile.DisplayName
				}
				equal, _, _, herr := hashVerifyPair(
					parent, peerSrc, w.fsAdapter, expectedFile, actualFile, display, srcID, wd, w.queue,
				)
				if herr != nil {
					if logservice.LS != nil {
						_ = logservice.LS.LogStructured("warning",
							fmt.Sprintf("hash verify failed path=%s: %v; falling back to overwrite policy", display, herr),
							"worker", w.id,
							logservice.LogFields{Action: "hash_verify", NodeID: srcID, Queue: w.queueName})
					}
				} else if queue.NormalizeOverwritePolicy(policy) == queue.OverwriteNewestMTime {
					if equal {
						outcome.SrcCopyStatus = db.CopyStatusAlreadyExisted
						outcome.DstCopyStatus = db.CopyStatusAlreadyExisted
						outcome.ExclusionSource = ""
					} else {
						outcome.SrcCopyStatus = db.CopyStatusPending
						outcome.DstCopyStatus = db.CopyStatusNeedsOverwrite
						outcome.ExclusionSource = ""
					}
				}
			}

			task.DiscoveredChildren = append(task.DiscoveredChildren, queue.ChildResult{
				File:            actualFile,
				Status:          db.StatusSuccessful, // File exists on dst, no traversal needed
				IsFile:          true,
				SrcID:           srcID,
				SrcCopyStatus:   outcome.SrcCopyStatus,
				DstCopyStatus:   outcome.DstCopyStatus,
				ExclusionSource: outcome.ExclusionSource,
			})
		}
	}

	// Rematch cleaned SRC file basenames onto unmatched DST siblings.
	rematchDSTChildrenFiles(task, expectedFiles, actualFileMap, expectedFileMap, srcIDMap)

	// Check for extra files on dst (not on src)
	for _, actualFile := range actualResult.Files {
		matchKey := queue.DstChildMatchKey(actualFile.Type, actualFile.DisplayName, actualFile.LocationPath)
		if _, exists := expectedFileMap[matchKey]; !exists {
			if rematchedDSTKey(task, matchKey) {
				continue
			}
			// File exists on dst but not src: terminal successful; no SRC / no copy.
			task.DiscoveredChildren = append(task.DiscoveredChildren, queue.ChildResult{
				File:          actualFile,
				Status:        db.StatusSuccessful,
				IsFile:        true,
				SrcID:         "", // No SRC node; no copy enrollment
				DstCopyStatus: db.CopyStatusDestinationOnly,
			})
		}
	}

	w.queue.NoteDiscoveredChildren(task, len(task.DiscoveredChildren))
	return nil
}

func rematchedDSTKey(task *queue.TaskBase, matchKey string) bool {
	if task == nil {
		return false
	}
	for _, c := range task.DiscoveredChildren {
		if c.SrcID == "" {
			continue
		}
		if c.IsFile {
			if queue.DstChildMatchKey(c.File.Type, c.File.DisplayName, c.File.LocationPath) == matchKey {
				return true
			}
			continue
		}
		if queue.DstChildMatchKey(c.Folder.Type, c.Folder.DisplayName, c.Folder.LocationPath) == matchKey {
			return true
		}
	}
	return false
}

// rematchDSTChildren previously auto-paired cleaned-name hits against DST siblings.
// That silent rematch is disabled: collisions become manual_review via
// AppendDSTSiblingCollisionIssues (merge / unique / rename). Exact basename
// pairing still happens in the first DST compare pass.
func rematchDSTChildren(task *queue.TaskBase, expectedFolders []types.Folder, actualFolderMap map[string]types.Folder, expectedFolderMap map[string]types.Folder, srcIDMap map[string]string) {
}

// rematchDSTChildrenFiles: cleaned-name auto-pair disabled (see rematchDSTChildren).
func rematchDSTChildrenFiles(task *queue.TaskBase, expectedFiles []types.File, actualFileMap map[string]types.File, expectedFileMap map[string]types.File, srcIDMap map[string]string) {
}

// compareTimestamps compares src and dst timestamps for copy skip decisions.
func compareTimestamps(srcMTime, dstMTime string) string {
	srcTime, srcOK := parseItemMTime(srcMTime)
	dstTime, dstOK := parseItemMTime(dstMTime)
	if !srcOK || !dstOK {
		return "Unparseable"
	}
	if !dstTime.Before(srcTime) {
		return "Successful"
	}
	return "Pending"
}

func parseItemMTime(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999999Z07:00",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	// Google sometimes returns RFC3339 without zone; treat as UTC.
	if t, err := time.Parse("2006-01-02T15:04:05.999999999", s); err == nil {
		return t.UTC(), true
	}
	if t, err := time.Parse("2006-01-02T15:04:05", s); err == nil {
		return t.UTC(), true
	}
	return time.Time{}, false
}

// logError logs a failed task execution.
func (w *TraversalWorker) logError(task *queue.TaskBase, paramErr error, willRetry bool) {
	if logservice.LS == nil {
		return // Logger not initialized
	}
	path := task.DisplayPath
	retryMsg := "will retry"
	if !willRetry {
		retryMsg = "max retries exceeded"
	}

	err := logservice.LS.Log(
		"error",
		fmt.Sprintf("Failed to traverse %s: %v (%s)", path, paramErr, retryMsg),
		"worker",
		w.id,
		w.queueName,
	)
	if err != nil {
		fmt.Println("error logging", err)
	}
}

// executeGPL revalidates path-scoped GPL rules for cascade after ancestor remaps.
func (w *TraversalWorker) executeGPL(task *queue.TaskBase) error {
	if w.isDst {
		// DST pending is acknowledgment only; path composition lives on SRC.
		return nil
	}
	if !gpl.PathChecksRequired(w.queue.ScalingSrcProvider(), w.queue.ScalingDstProvider(), w.queue.PathCheckProfile()) {
		return nil
	}
	parent := fsOpContext(w.workerCtx, w.shutdownCtx)
	wd, ctx := observe.NewProgressWatchdog(parent, gplStallTimeout, progressStallSuppress(w.queue))
	defer wd.Stop()

	database := w.queue.Database()
	checkTarget := gpl.ResolvePathCheckTarget(w.queue.ScalingSrcProvider(), w.queue.ScalingDstProvider(), w.queue.PathCheckProfile())
	target := gpl.GPLTargetFromProvider(checkTarget)
	err := awaitErr(nil, ctx, parent, gplStallTimeout, "gpl revalidate", task.DisplayPath, func() error {
		_, err := gpl.ProcessGPLTaskSRC(database, target, task, w.queue.WindowsCompat())
		return err
	})
	if err == nil {
		wd.Beat()
	}
	return err
}
