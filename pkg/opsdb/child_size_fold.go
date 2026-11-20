// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"encoding/json"
	"fmt"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

const (
	FoldKindTrav   = "trav"
	FoldKindCopy   = "copy"
	FoldKindRefold = "refold"

	// Small chunks so Stop can abort between Badger batch reads on wide depths.
	foldComputeBatch = 64
)

// foldTxnBatchSize caps Badger mutations per Update during fold seal/cleanup.
var foldTxnBatchSize = 10_000

// SizeFoldMetrics is published on qstat during the identity child_size fold.
type SizeFoldMetrics struct {
	FoldersDone    int64    `json:"folders_done"`
	FoldersTotal   int64    `json:"folders_total"`
	ItemsCompleted int64    `json:"items_completed"`
	ItemsTotal     int64    `json:"items_total"`
	ItemsPerSecond float64  `json:"items_per_second"`
	CurrentDepth   int      `json:"current_depth"`
	MaxDepth       int      `json:"max_depth"`
	SealedDepth    int      `json:"sealed_depth"`
	Round          int      `json:"round"`
	FoldersPerSec  float64  `json:"folders_per_sec"`
	EtaSeconds     *float64 `json:"eta_seconds,omitempty"`
	EtaBasis       string   `json:"eta_basis"`
}

// FoldHooks are optional progress callbacks. Nil is fine.
type FoldHooks struct {
	Activity func(string)
	Publish  func(SizeFoldMetrics)
	Aborted  func() bool
	// QStatPhase overrides the qstat phase key (API reads "trav" / "copy" only).
	// Empty means use the fold kind. Set to FoldKindTrav/Copy when kind is FoldKindRefold.
	QStatPhase string
}

type foldProgress struct {
	store      *Store
	side, kind string
	hooks      FoldHooks
	started    time.Time
	lastPub    *time.Time
	done       *int64
	total      *int64
	maxDepth   int
}

func (p foldProgress) aborted() bool {
	return p.hooks.Aborted != nil && p.hooks.Aborted()
}

func (p foldProgress) publish(depth int, force bool, sealed int) {
	if p.store == nil || p.done == nil || p.total == nil {
		return
	}
	p.store.publishFold(p.side, p.kind, p.hooks, SizeFoldMetrics{
		FoldersDone: *p.done, FoldersTotal: *p.total,
		ItemsCompleted: *p.done, ItemsTotal: *p.total,
		CurrentDepth: depth, MaxDepth: p.maxDepth, SealedDepth: sealed, Round: depth,
	}, p.started, p.lastPub, force)
}

// PatchKidInPack updates one child entry inside kids:{side}:{parent} by id.
// No-op when the pack or kid is missing (discovery may not have packed yet).
func (s *Store) PatchKidInPack(side, parentID, kidID string, patch func(*KidRecord)) error {
	if s == nil || parentID == "" || kidID == "" || patch == nil {
		return nil
	}
	kids, err := s.kidsFor(side, parentID)
	if err != nil {
		return err
	}
	found := false
	for i := range kids {
		if kids[i].ID != kidID {
			continue
		}
		k := kids[i]
		patch(&k)
		kids[i] = k
		found = true
		break
	}
	if !found {
		return nil
	}
	b, err := json.Marshal(kids)
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(kidsKey(side, parentID), b)
	})
}

// AppendKidTicket records a newly created child on the parent kids pack and
// tickets the parent for the copy-side size fold.
func (s *Store) AppendKidTicket(side, parentID string, parentDepth int, kid KidRecord) error {
	if s == nil || parentID == "" || kid.ID == "" {
		return nil
	}
	if parentDepth < 0 {
		parentDepth = 0
	}
	kids, err := s.kidsFor(side, parentID)
	if err != nil {
		return err
	}
	kids = MergeKid(kids, kid)
	b, err := json.Marshal(kids)
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(kidsKey(side, parentID), b); err != nil {
			return err
		}
		return txn.Set(foldTicketKey(side, parentDepth, parentID), []byte{1})
	})
}

func (s *Store) kidsFor(side, parentID string) ([]KidRecord, error) {
	var kids []KidRecord
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(kidsKey(side, parentID))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var derr error
			kids, derr = decodeKids(val)
			return derr
		})
	})
	return kids, err
}

// FoldChildSizes assigns identity child_size from deepest ticket depth up to root.
// kind is FoldKindTrav (leftover pend:trav), FoldKindCopy (fold:ticket), or
// FoldKindRefold (scan all folders; clears the fold cursor first).
func (s *Store) FoldChildSizes(side, kind string, hooks FoldHooks) error {
	if s == nil || s.db == nil {
		return nil
	}
	if kind != FoldKindTrav && kind != FoldKindCopy && kind != FoldKindRefold {
		return fmt.Errorf("opsdb fold: unknown kind %q", kind)
	}
	started := time.Now()
	var done int64
	var total int64
	lastPub := time.Time{}
	// Clear stale UI metrics immediately (previous run can leave 3.1M/3.5M on screen).
	s.publishFold(side, kind, hooks, SizeFoldMetrics{
		FoldersDone: 0, FoldersTotal: 0, ItemsCompleted: 0, ItemsTotal: 0,
		EtaBasis: "items",
	}, started, &lastPub, true)
	setActivity(hooks, "Preparing folder size totals…")

	if kind == FoldKindRefold {
		if err := s.clearFoldCursor(side); err != nil {
			return err
		}
	}
	var refoldByDepth map[int][]string
	maxDepth := -1
	var err error
	if kind == FoldKindRefold {
		setActivity(hooks, "Scanning folders for size recalculation…")
		refoldByDepth, maxDepth, err = s.scanFolderIDsByDepth(side, hooks, started, &lastPub)
		if err != nil {
			return err
		}
	} else {
		maxDepth, err = s.GetDepthMax(side)
		if err != nil {
			return err
		}
		if kind == FoldKindTrav {
			pendMax, err := s.maxPendDepth(side, PhaseTrav)
			if err != nil {
				return err
			}
			if pendMax > maxDepth {
				maxDepth = pendMax
			}
		}
		if ticketMax, err := s.maxTicketDepth(side, kind, maxDepth); err != nil {
			return err
		} else if ticketMax > maxDepth {
			maxDepth = ticketMax
		}
	}
	if hooks.Aborted != nil && hooks.Aborted() {
		return fmt.Errorf("child size fold aborted")
	}
	sealed, hasSealed, err := s.readSealedDepth(side)
	if err != nil {
		return err
	}
	if hasSealed && sealed <= 0 {
		return s.clearFoldCursor(side)
	}
	start := maxDepth
	replay := false
	if hasSealed {
		start = sealed - 1
		replay = true
		if start > maxDepth {
			start = maxDepth
		}
	}
	if start < 0 {
		return s.clearFoldCursor(side)
	}
	if kind == FoldKindRefold {
		for d := 0; d <= start; d++ {
			total += int64(len(refoldByDepth[d]))
		}
	} else {
		// Cheap estimate (depth folder stats); avoid paging millions of pend keys up front.
		total, err = s.estimateFoldTotal(side, kind, start)
		if err != nil {
			return err
		}
	}
	done = 0
	s.publishFold(side, kind, hooks, SizeFoldMetrics{
		FoldersDone: 0, FoldersTotal: total, ItemsCompleted: 0, ItemsTotal: total,
		MaxDepth: maxDepth, EtaBasis: "items",
	}, started, &lastPub, true)
	setActivity(hooks, "Calculating folder sizes…")
	prog := foldProgress{
		store: s, side: side, kind: kind, hooks: hooks, started: started, lastPub: &lastPub,
		done: &done, total: &total, maxDepth: maxDepth,
	}
	for depth := start; depth >= 0; depth-- {
		if prog.aborted() {
			return fmt.Errorf("child size fold aborted")
		}
		label := "Calculating folder sizes…"
		if depth < start {
			label = "Updating parent folder sizes…"
		}
		setActivity(hooks, label)
		var ids []string
		if kind == FoldKindRefold {
			ids = refoldByDepth[depth]
		}
		parents, _, err := s.foldAndSealDepth(side, kind, depth, replay, ids, prog)
		if err != nil {
			return err
		}
		replay = false
		if depth > 0 && kind != FoldKindRefold {
			extra, err := s.countNewParents(side, kind, depth-1, parents)
			if err != nil {
				return err
			}
			total += extra
		}
		if done > total {
			total = done
		}
		prog.publish(depth, true, depth)
	}
	return s.clearFoldCursor(side)
}

type childSizeWrite struct {
	id  string
	raw []byte
}

// foldAndSealDepth computes and commits child_size in batches, then seals the depth.
// Status writes land before sealed depth advances so an abort mid-depth resumes by replaying.
func (s *Store) foldAndSealDepth(side, kind string, depth int, replay bool, presetIDs []string, prog foldProgress) ([]string, int64, error) {
	parents := map[string]struct{}{}
	var processed int64
	var pendingWrites []childSizeWrite

	flushWrites := func(force bool) error {
		if len(pendingWrites) == 0 {
			return nil
		}
		if !force && len(pendingWrites) < foldTxnBatchSize {
			return nil
		}
		for start := 0; start < len(pendingWrites); start += foldTxnBatchSize {
			if prog.aborted() {
				return fmt.Errorf("child size fold aborted")
			}
			end := start + foldTxnBatchSize
			if end > len(pendingWrites) {
				end = len(pendingWrites)
			}
			chunk := pendingWrites[start:end]
			setActivity(prog.hooks, "Saving folder size totals…")
			if err := s.commitStatusWrites(side, chunk); err != nil {
				return err
			}
			prog.publish(depth, true, -1)
		}
		pendingWrites = pendingWrites[:0]
		return nil
	}

	handleChunk := func(chunk []string) error {
		if prog.aborted() {
			return fmt.Errorf("child size fold aborted")
		}
		writes, parentAdds, n, err := s.computeFoldChunk(side, chunk, replay)
		if err != nil {
			return err
		}
		for _, id := range parentAdds {
			parents[id] = struct{}{}
		}
		pendingWrites = append(pendingWrites, writes...)
		processed += n
		*prog.done += n
		prog.publish(depth, false, -1)
		return flushWrites(false)
	}

	if presetIDs != nil {
		for start := 0; start < len(presetIDs); start += foldComputeBatch {
			end := start + foldComputeBatch
			if end > len(presetIDs) {
				end = len(presetIDs)
			}
			if err := handleChunk(presetIDs[start:end]); err != nil {
				return nil, processed, err
			}
		}
	} else if err := s.forEachFoldIDPage(side, kind, depth, foldComputeBatch, handleChunk); err != nil {
		return nil, processed, err
	}
	if err := flushWrites(true); err != nil {
		return nil, processed, err
	}

	out := make([]string, 0, len(parents))
	for id := range parents {
		out = append(out, id)
	}
	setActivity(prog.hooks, "Saving folder size totals…")
	if err := s.commitFoldParentsAndSeal(side, depth, out, prog); err != nil {
		return out, processed, err
	}
	setActivity(prog.hooks, "Cleaning up fold tickets…")
	if err := s.dropFoldTickets(side, kind, depth, prog); err != nil {
		return out, processed, err
	}
	return out, processed, nil
}

func (s *Store) computeFoldChunk(side string, chunk []string, replay bool) ([]childSizeWrite, []string, int64, error) {
	if len(chunk) == 0 {
		return nil, nil, 0, nil
	}
	nodes, err := s.BatchGetNode(side, chunk)
	if err != nil {
		return nil, nil, 0, err
	}
	kidsByParent, err := s.BatchGetKids(side, chunk)
	if err != nil {
		return nil, nil, 0, err
	}
	folderIDs := folderKidIDs(kidsByParent)
	var folderSt map[string]StatusRecord
	if len(folderIDs) > 0 {
		folderSt, err = s.BatchGetStatus(side, folderIDs)
		if err != nil {
			return nil, nil, 0, err
		}
	}
	live, err := s.BatchGetStatus(side, chunk)
	if err != nil {
		return nil, nil, 0, err
	}
	var writes []childSizeWrite
	var parents []string
	var processed int64
	for _, id := range chunk {
		n, ok := nodes[id]
		if !ok || n.Type == NodeTypeFile {
			continue
		}
		computedSize := childSizeFromKids(kidsByParent[id], folderSt)
		computedCount := childCountFromKids(kidsByParent[id], folderSt)
		st := live[id]
		if !(replay && st.ChildSize == computedSize && st.ChildCount == computedCount) {
			st.ChildSize = computedSize
			st.ChildCount = computedCount
			raw, err := json.Marshal(statusTravRecord(st))
			if err != nil {
				return nil, nil, processed, err
			}
			writes = append(writes, childSizeWrite{id: id, raw: raw})
		}
		if n.ParentID != "" {
			parents = append(parents, n.ParentID)
		}
		processed++
	}
	return writes, parents, processed, nil
}

func (s *Store) commitStatusWrites(side string, writes []childSizeWrite) error {
	if len(writes) == 0 {
		return nil
	}
	return s.db.Update(func(txn *badger.Txn) error {
		for _, w := range writes {
			if w.id == "" || len(w.raw) == 0 {
				continue
			}
			if err := txn.Set(statusTravKey(side, w.id), w.raw); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) commitFoldParentsAndSeal(side string, depth int, parents []string, prog foldProgress) error {
	if prog.aborted() {
		return fmt.Errorf("child size fold aborted")
	}
	if err := s.deletePrefixAborted(foldNextPrefix(side), prog.hooks.Aborted); err != nil {
		return err
	}
	one := []byte{1}
	for start := 0; start < len(parents); start += foldTxnBatchSize {
		if prog.aborted() {
			return fmt.Errorf("child size fold aborted")
		}
		end := start + foldTxnBatchSize
		if end > len(parents) {
			end = len(parents)
		}
		chunk := parents[start:end]
		if err := s.db.Update(func(txn *badger.Txn) error {
			for _, id := range chunk {
				if id == "" {
					continue
				}
				if err := txn.Set(foldNextKey(side, id), one); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		prog.publish(depth, true, -1)
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(foldSealedDepthKey(side), fmt.Appendf(nil, "%d", depth))
	})
}

func (s *Store) dropFoldTickets(side, kind string, depth int, prog foldProgress) error {
	if prog.aborted() {
		return fmt.Errorf("child size fold aborted")
	}
	switch kind {
	case FoldKindTrav:
		return s.DropPendingPrefixAborted(side, PhaseTrav, depth, NodeTypeFolder, prog.hooks.Aborted)
	case FoldKindCopy:
		return s.deletePrefixAborted(foldTicketPrefix(side, depth), prog.hooks.Aborted)
	default:
		return nil
	}
}

func folderKidIDs(kidsByParent map[string][]KidRecord) []string {
	seen := map[string]struct{}{}
	var ids []string
	for _, kids := range kidsByParent {
		for _, k := range kids {
			if k.Type == NodeTypeFile || k.ID == "" {
				continue
			}
			if _, ok := seen[k.ID]; ok {
				continue
			}
			seen[k.ID] = struct{}{}
			ids = append(ids, k.ID)
		}
	}
	return ids
}

func childSizeFromKids(kids []KidRecord, folderSt map[string]StatusRecord) int64 {
	var sum int64
	for _, k := range kids {
		if k.Type == NodeTypeFile {
			if k.Size > 0 {
				sum += k.Size
			}
			continue
		}
		if folderSt != nil {
			sum += folderSt[k.ID].ChildSize
		}
	}
	return sum
}

// childCountFromKids folds identity descendant counts: each file is 1; each folder
// is 1 (itself) plus its folded ChildCount.
func childCountFromKids(kids []KidRecord, folderSt map[string]StatusRecord) int64 {
	var sum int64
	for _, k := range kids {
		if k.Type == NodeTypeFile {
			sum++
			continue
		}
		sum++ // the child folder itself
		if folderSt != nil {
			sum += folderSt[k.ID].ChildCount
		}
	}
	return sum
}

func (s *Store) forEachFoldIDPage(side, kind string, depth, pageSize int, fn func([]string) error) error {
	if pageSize <= 0 {
		pageSize = foldComputeBatch
	}
	seen := map[string]struct{}{}
	emit := func(ids []string) error {
		if len(ids) == 0 {
			return nil
		}
		uniq := ids[:0]
		for _, id := range ids {
			if id == "" {
				continue
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			uniq = append(uniq, id)
		}
		if len(uniq) == 0 {
			return nil
		}
		return fn(uniq)
	}

	next, err := s.listIDsWithPrefix(foldNextPrefix(side))
	if err != nil {
		return err
	}
	if len(next) > 0 {
		nodes, err := s.BatchGetNode(side, next)
		if err != nil {
			return err
		}
		var atDepth []string
		for _, id := range next {
			n, ok := nodes[id]
			if ok && n.Depth == depth && n.Type != NodeTypeFile {
				atDepth = append(atDepth, id)
			}
		}
		for start := 0; start < len(atDepth); start += pageSize {
			end := start + pageSize
			if end > len(atDepth) {
				end = len(atDepth)
			}
			if err := emit(atDepth[start:end]); err != nil {
				return err
			}
		}
	}

	if kind == FoldKindTrav {
		after := ""
		for {
			page, err := s.ListSchedAtDepth(side, PhaseTrav, depth, NodeTypeFolder, after, pageSize)
			if err != nil {
				return err
			}
			if len(page) == 0 {
				return nil
			}
			if err := emit(page); err != nil {
				return err
			}
			if len(page) < pageSize {
				return nil
			}
			after = page[len(page)-1]
		}
	}
	tickets, err := s.listIDsWithPrefix(foldTicketPrefix(side, depth))
	if err != nil {
		return err
	}
	for start := 0; start < len(tickets); start += pageSize {
		end := start + pageSize
		if end > len(tickets) {
			end = len(tickets)
		}
		if err := emit(tickets[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ticketIDsAtDepth(side, kind string, depth int) ([]string, error) {
	if kind == FoldKindTrav {
		var ids []string
		after := ""
		for {
			page, err := s.ListSchedAtDepth(side, PhaseTrav, depth, NodeTypeFolder, after, 1000)
			if err != nil {
				return nil, err
			}
			if len(page) == 0 {
				break
			}
			ids = append(ids, page...)
			if len(page) < 1000 {
				break
			}
			after = page[len(page)-1]
		}
		return ids, nil
	}
	return s.listIDsWithPrefix(foldTicketPrefix(side, depth))
}

func (s *Store) foldIDsAtDepth(side, kind string, depth int) ([]string, error) {
	var ids []string
	err := s.forEachFoldIDPage(side, kind, depth, 1000, func(page []string) error {
		ids = append(ids, page...)
		return nil
	})
	return ids, err
}

func (s *Store) scanFolderIDsByDepth(side string, hooks FoldHooks, started time.Time, lastPub *time.Time) (map[int][]string, int, error) {
	byDepth := map[int][]string{}
	maxDepth := -1
	est, _ := s.GetDepthStatTotal(side, "traversal/folder")
	var found int64
	err := s.db.View(func(txn *badger.Txn) error {
		prefix := nodePrefix(side)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		var seen int64
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			seen++
			if seen%10_000 == 0 {
				if hooks.Aborted != nil && hooks.Aborted() {
					return fmt.Errorf("child size fold aborted")
				}
				s.publishFold(side, FoldKindRefold, hooks, SizeFoldMetrics{
					FoldersDone: found, FoldersTotal: est,
					ItemsCompleted: found, ItemsTotal: est,
					CurrentDepth: -1, MaxDepth: maxDepth, Round: -1, EtaBasis: "items",
				}, started, lastPub, false)
			}
			item := it.Item()
			if err := item.Value(func(val []byte) error {
				n, err := decodeNode(val)
				if err != nil {
					return err
				}
				if n.Type == NodeTypeFile || n.ID == "" {
					return nil
				}
				d := n.Depth
				if d < 0 {
					d = 0
				}
				byDepth[d] = append(byDepth[d], n.ID)
				found++
				if d > maxDepth {
					maxDepth = d
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, -1, err
	}
	s.publishFold(side, FoldKindRefold, hooks, SizeFoldMetrics{
		FoldersDone: found, FoldersTotal: found,
		ItemsCompleted: found, ItemsTotal: found,
		CurrentDepth: -1, MaxDepth: maxDepth, Round: -1, EtaBasis: "items",
	}, started, lastPub, true)
	return byDepth, maxDepth, nil
}

func (s *Store) readSealedDepth(side string) (int, bool, error) {
	var n int64
	found := false
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(foldSealedDepthKey(side))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		return item.Value(func(val []byte) error {
			_, scanErr := fmt.Sscan(string(val), &n)
			return scanErr
		})
	})
	return int(n), found, err
}

func (s *Store) clearFoldCursor(side string) error {
	if err := s.deletePrefixAborted(foldNextPrefix(side), nil); err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(foldSealedDepthKey(side))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	})
}

func (s *Store) countNewParents(side, kind string, depth int, parents []string) (int64, error) {
	if len(parents) == 0 {
		return 0, nil
	}
	tickets, err := s.ticketIDsAtDepth(side, kind, depth)
	if err != nil {
		return 0, err
	}
	have := map[string]struct{}{}
	for _, id := range tickets {
		have[id] = struct{}{}
	}
	var n int64
	for _, id := range parents {
		if _, ok := have[id]; !ok {
			n++
		}
	}
	return n, nil
}

func (s *Store) countFoldTickets(side, kind string, fromDepth int) (int64, error) {
	var n int64
	for d := 0; d <= fromDepth; d++ {
		ids, err := s.foldIDsAtDepth(side, kind, d)
		if err != nil {
			return 0, err
		}
		n += int64(len(ids))
	}
	return n, nil
}

// estimateFoldTotal uses O(depth) depth-stat counters instead of paging every pend key.
func (s *Store) estimateFoldTotal(side, kind string, fromDepth int) (int64, error) {
	if fromDepth < 0 {
		return 0, nil
	}
	key := "traversal/folder"
	if kind == FoldKindCopy {
		key = "copy/folder"
	}
	var n int64
	for d := 0; d <= fromDepth; d++ {
		c, err := s.GetDepthStat(side, d, key)
		if err != nil {
			return 0, err
		}
		n += c
	}
	if n > 0 {
		return n, nil
	}
	// Fallback when depth stats are empty (tests / partial trees).
	return s.countFoldTickets(side, kind, fromDepth)
}

func (s *Store) maxPendDepth(side, phase string) (int, error) {
	max := -1
	err := s.db.View(func(txn *badger.Txn) error {
		prefix := []byte("pend:" + side + ":" + phase + ":")
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			_, _, depth, _, _, ok := parsePendingKey(it.Item().Key())
			if ok && depth > max {
				max = depth
			}
		}
		return nil
	})
	return max, err
}

func (s *Store) maxTicketDepth(side, kind string, hint int) (int, error) {
	if kind != FoldKindCopy {
		return hint, nil
	}
	max := -1
	err := s.db.View(func(txn *badger.Txn) error {
		prefix := []byte("fold:ticket:" + side + ":")
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			d, ok := parseFoldTicketDepth(it.Item().Key())
			if ok && d > max {
				max = d
			}
		}
		return nil
	})
	if max < 0 {
		return hint, err
	}
	return max, err
}

func parseFoldTicketDepth(key []byte) (int, bool) {
	s := string(key)
	rest := s
	const p = "fold:ticket:"
	if len(rest) <= len(p) || rest[:len(p)] != p {
		return 0, false
	}
	rest = rest[len(p):]
	i := 0
	for i < len(rest) && rest[i] != ':' {
		i++
	}
	if i >= len(rest) || rest[i] != ':' {
		return 0, false
	}
	rest = rest[i+1:]
	n := 0
	if rest == "" || rest[0] < '0' || rest[0] > '9' {
		return 0, false
	}
	for _, c := range rest {
		if c == ':' {
			break
		}
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}

func (s *Store) listIDsWithPrefix(prefix []byte) ([]string, error) {
	var ids []string
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			k := string(it.Item().Key())
			id := k
			if i := lastColon(k); i >= 0 {
				id = k[i+1:]
			}
			if id != "" {
				ids = append(ids, id)
			}
		}
		return nil
	})
	return ids, err
}

func lastColon(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == ':' {
			return i
		}
	}
	return -1
}

func (s *Store) deletePrefixAborted(prefix []byte, aborted func() bool) error {
	for {
		if aborted != nil && aborted() {
			return fmt.Errorf("child size fold aborted")
		}
		keys, err := s.listPrefixKeys(prefix, foldTxnBatchSize)
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			return nil
		}
		if err := s.db.Update(func(txn *badger.Txn) error {
			for _, k := range keys {
				if delErr := txn.Delete(k); delErr != nil && delErr != badger.ErrKeyNotFound {
					return delErr
				}
			}
			return nil
		}); err != nil {
			return err
		}
		if len(keys) < foldTxnBatchSize {
			return nil
		}
	}
}

func (s *Store) listPrefixKeys(prefix []byte, limit int) ([][]byte, error) {
	var keys [][]byte
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			keys = append(keys, it.Item().KeyCopy(nil))
			if limit > 0 && len(keys) >= limit {
				return nil
			}
		}
		return nil
	})
	return keys, err
}

func (s *Store) publishFold(side, kind string, hooks FoldHooks, m SizeFoldMetrics, started time.Time, last *time.Time, force bool) {
	if m.SealedDepth < 0 {
		m.SealedDepth = 0
	}
	elapsed := time.Since(started).Seconds()
	if elapsed > 0 {
		m.FoldersPerSec = float64(m.FoldersDone) / elapsed
		m.ItemsPerSecond = m.FoldersPerSec
	}
	remain := m.FoldersTotal - m.FoldersDone
	if remain < 0 {
		remain = 0
	}
	if m.FoldersPerSec > 0 {
		eta := float64(remain) / m.FoldersPerSec
		m.EtaSeconds = &eta
	}
	m.EtaBasis = "items"
	if !force && last != nil && time.Since(*last) < time.Second {
		return
	}
	if last != nil {
		*last = time.Now()
	}
	if hooks.Publish != nil {
		hooks.Publish(m)
	}
	b, err := json.Marshal(m)
	if err != nil {
		return
	}
	phase := hooks.QStatPhase
	if phase == "" {
		phase = kind
	}
	if phase == FoldKindRefold {
		phase = FoldKindTrav
	}
	_ = s.AppendQueueStats(QueueStatsRecord{
		QueueKey: "size-fold", Phase: phase, MetricsJSON: string(b), At: time.Now(),
	})
	_ = side
}

func setActivity(hooks FoldHooks, label string) {
	if hooks.Activity != nil {
		hooks.Activity(label)
	}
}
