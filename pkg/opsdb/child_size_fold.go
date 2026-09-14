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
	FoldKindTrav = "trav"
	FoldKindCopy = "copy"
)

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
	b, err := encode(kids)
	if err != nil {
		return err
	}
	return s.update(func(txn *badger.Txn) error {
		if err := txn.Set(kidsKey(side, parentID), b); err != nil {
			return err
		}
		return txn.Set(foldTicketKey(side, parentDepth, parentID), []byte{1})
	})
}

func (s *Store) kidsFor(side, parentID string) ([]KidRecord, error) {
	var kids []KidRecord
	err := s.view(func(txn *badger.Txn) error {
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
// kind is FoldKindTrav (leftover pend:trav) or FoldKindCopy (fold:ticket).
func (s *Store) FoldChildSizes(side, kind string, hooks FoldHooks) error {
	if s == nil || s.db == nil {
		return nil
	}
	if kind != FoldKindTrav && kind != FoldKindCopy {
		return fmt.Errorf("opsdb fold: unknown kind %q", kind)
	}
	maxDepth, err := s.GetDepthMax(side)
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
	total, err := s.countFoldTickets(side, kind, start)
	if err != nil {
		return err
	}
	setActivity(hooks, "Calculating folder sizes…")
	started := time.Now()
	var done int64
	lastPub := time.Time{}
	for depth := start; depth >= 0; depth-- {
		if hooks.Aborted != nil && hooks.Aborted() {
			return fmt.Errorf("child size fold aborted")
		}
		label := "Calculating folder sizes…"
		if depth < start {
			label = "Updating parent folder sizes…"
		}
		setActivity(hooks, label)
		parents, writes, n, err := s.foldDepth(side, kind, depth, replay, func(n int64) {
			s.publishFold(side, kind, hooks, SizeFoldMetrics{
				FoldersDone: done + n, FoldersTotal: total,
				ItemsCompleted: done + n, ItemsTotal: total,
				CurrentDepth: depth, MaxDepth: maxDepth, Round: depth,
			}, started, &lastPub, false)
		})
		if err != nil {
			return err
		}
		replay = false
		done += n
		if depth > 0 {
			extra, err := s.countNewParents(side, kind, depth-1, parents)
			if err != nil {
				return err
			}
			total += extra
		}
		if err := s.sealFoldDepth(side, kind, depth, parents, writes); err != nil {
			return err
		}
		s.publishFold(side, kind, hooks, SizeFoldMetrics{
			FoldersDone: done, FoldersTotal: total,
			ItemsCompleted: done, ItemsTotal: total,
			CurrentDepth: depth, MaxDepth: maxDepth, SealedDepth: depth, Round: depth,
		}, started, &lastPub, true)
	}
	return s.clearFoldCursor(side)
}

type childSizeWrite struct {
	id  string
	raw []byte
}

func (s *Store) foldDepth(side, kind string, depth int, replay bool, onBatch func(int64)) ([]string, []childSizeWrite, int64, error) {
	ids, err := s.foldIDsAtDepth(side, kind, depth)
	if err != nil {
		return nil, nil, 0, err
	}
	if len(ids) == 0 {
		return nil, nil, 0, nil
	}
	const batch = 256
	parents := map[string]struct{}{}
	var writes []childSizeWrite
	var processed int64
	for start := 0; start < len(ids); start += batch {
		end := start + batch
		if end > len(ids) {
			end = len(ids)
		}
		chunk := ids[start:end]
		nodes, err := s.BatchGetNode(side, chunk)
		if err != nil {
			return nil, nil, processed, err
		}
		kidsByParent, err := s.BatchGetKids(side, chunk)
		if err != nil {
			return nil, nil, processed, err
		}
		folderIDs := folderKidIDs(kidsByParent)
		var folderSt map[string]StatusRecord
		if len(folderIDs) > 0 {
			folderSt, err = s.BatchGetStatus(side, folderIDs)
			if err != nil {
				return nil, nil, processed, err
			}
		}
		live, err := s.BatchGetStatus(side, chunk)
		if err != nil {
			return nil, nil, processed, err
		}
		for _, id := range chunk {
			n, ok := nodes[id]
			if !ok || n.Type == NodeTypeFile {
				continue
			}
			computed := childSizeFromKids(kidsByParent[id], folderSt)
			st := live[id]
			if !(replay && st.ChildSize == computed) {
				st.ChildSize = computed
				raw, err := encode(statusTravRecord(st))
				if err != nil {
					return nil, nil, processed, err
				}
				writes = append(writes, childSizeWrite{id: id, raw: raw})
			}
			if n.ParentID != "" {
				parents[n.ParentID] = struct{}{}
			}
			processed++
		}
		if onBatch != nil {
			onBatch(processed)
		}
	}
	out := make([]string, 0, len(parents))
	for id := range parents {
		out = append(out, id)
	}
	return out, writes, processed, nil
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
	seen := map[string]struct{}{}
	var ids []string
	add := func(id string) {
		if id == "" {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	tickets, err := s.ticketIDsAtDepth(side, kind, depth)
	if err != nil {
		return nil, err
	}
	for _, id := range tickets {
		add(id)
	}
	next, err := s.listIDsWithPrefix(foldNextPrefix(side))
	if err != nil {
		return nil, err
	}
	if len(next) == 0 {
		return ids, nil
	}
	nodes, err := s.BatchGetNode(side, next)
	if err != nil {
		return nil, err
	}
	for _, id := range next {
		n, ok := nodes[id]
		if ok && n.Depth == depth && n.Type != NodeTypeFile {
			add(id)
		}
	}
	return ids, nil
}

func (s *Store) sealFoldDepth(side, kind string, depth int, parents []string, writes []childSizeWrite) error {
	if err := s.commitFoldDepth(side, depth, parents, writes); err != nil {
		return err
	}
	if kind == FoldKindTrav {
		return s.DropPendingPrefix(side, PhaseTrav, depth, NodeTypeFolder)
	}
	return s.deletePrefix(foldTicketPrefix(side, depth))
}

func (s *Store) commitFoldDepth(side string, depth int, parents []string, writes []childSizeWrite) error {
	return s.update(func(txn *badger.Txn) error {
		if err := deletePrefixTxn(txn, foldNextPrefix(side)); err != nil {
			return err
		}
		for _, id := range parents {
			if id == "" {
				continue
			}
			if err := txn.Set(foldNextKey(side, id), []byte{1}); err != nil {
				return err
			}
		}
		for _, w := range writes {
			if w.id == "" || len(w.raw) == 0 {
				continue
			}
			if err := txn.Set(statusTravKey(side, w.id), w.raw); err != nil {
				return err
			}
		}
		return txn.Set(foldSealedDepthKey(side), fmt.Appendf(nil, "%d", depth))
	})
}

func (s *Store) writeSealedDepth(side string, depth int) error {
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(foldSealedDepthKey(side), fmt.Appendf(nil, "%d", depth))
	})
}

func (s *Store) readSealedDepth(side string) (int, bool, error) {
	var n int64
	found := false
	err := s.view(func(txn *badger.Txn) error {
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
	if err := s.deletePrefix(foldNextPrefix(side)); err != nil {
		return err
	}
	return s.update(func(txn *badger.Txn) error {
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

func (s *Store) maxPendDepth(side, phase string) (int, error) {
	max := -1
	err := s.view(func(txn *badger.Txn) error {
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
	err := s.view(func(txn *badger.Txn) error {
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
	// fold:ticket:{side}:{depth}:{id}
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
	err := s.view(func(txn *badger.Txn) error {
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

func (s *Store) deletePrefix(prefix []byte) error {
	return s.update(func(txn *badger.Txn) error {
		return deletePrefixTxn(txn, prefix)
	})
}

func deletePrefixTxn(txn *badger.Txn, prefix []byte) error {
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	var keys [][]byte
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		keys = append(keys, it.Item().KeyCopy(nil))
	}
	for _, k := range keys {
		if err := txn.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) publishFold(side, kind string, hooks FoldHooks, m SizeFoldMetrics, started time.Time, last *time.Time, force bool) {
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
	_ = s.AppendQueueStats(QueueStatsRecord{
		QueueKey: "size-fold", Phase: kind, MetricsJSON: string(b), At: time.Now(),
	})
	_ = side
}

func setActivity(hooks FoldHooks, label string) {
	if hooks.Activity != nil {
		hooks.Activity(label)
	}
}
