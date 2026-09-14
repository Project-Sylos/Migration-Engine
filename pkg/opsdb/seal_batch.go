// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"bytes"
	"fmt"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

// SealNodeWrite is one node+status+pending unit for WriteSealBatch.
type SealNodeWrite struct {
	Side       string
	Node       NodeRecord
	Status     StatusRecord
	Depth      int
	Deltas     []PendingDelta
	PrevNode   *NodeRecord
	PrevStatus StatusRecord
	InsertOnly bool
}

// SealStatusWrite is one status+pending unit for WriteSealBatch.
type SealStatusWrite struct {
	Side       string
	ID         string
	Status     StatusRecord
	Depth      int
	Deltas     []PendingDelta
	PrevStatus StatusRecord
}

// SealKidsReplace replaces kids:{side}:{parent} with an authoritative child snapshot list.
// Side defaults to SRC when empty. Ticket marks the parent for the copy size fold.
type SealKidsReplace struct {
	Side        string
	ParentID    string
	Kids        []KidRecord
	Ticket      bool
	TicketDepth int
}

// KidTicket is one child appended to a parent's kids pack during copy.
type KidTicket struct {
	Side        string
	ParentID    string
	ParentDepth int
	Kid         KidRecord
}

// SealMapWrite is one id_map pair plus catalog ingest index.
type SealMapWrite struct {
	Map   IDMapRecord
	Depth int
}

// WriteSealBatch writes a seal drain using trusted caller context (no Badger probes).
func (s *Store) WriteSealBatch(nodes []SealNodeWrite, status []SealStatusWrite, maps []SealMapWrite, logs []LogRecord, errs []TaskErrorRecord) ([]SchedCountDelta, error) {
	return s.WriteSealBatchTrusted(nodes, status, nil, maps, logs, errs)
}

// WriteSealBatchTrusted writes a seal drain without existence or prev-state probes.
func (s *Store) WriteSealBatchTrusted(nodes []SealNodeWrite, status []SealStatusWrite, kidsReplace []SealKidsReplace, maps []SealMapWrite, logs []LogRecord, errs []TaskErrorRecord) ([]SchedCountDelta, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	if len(nodes) == 0 && len(status) == 0 && len(kidsReplace) == 0 && len(maps) == 0 && len(logs) == 0 && len(errs) == 0 {
		return nil, nil
	}
	if err := s.preserveSkippedDescendantCounts(status); err != nil {
		return nil, err
	}
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()
	var sched []SchedCountDelta
	var bucketKeys []string
	var bucketDeltas []int64
	for i := range nodes {
		w := &nodes[i]
		delta, err := writeSealNodeTrusted(wb, w)
		if err != nil {
			return nil, fmt.Errorf("write seal node: %w", err)
		}
		sched = append(sched, delta...)
		bucketKeys, bucketDeltas = appendIndexBucketDeltas(bucketKeys, bucketDeltas, w.Side, w.Node, func() *NodeRecord {
			if w.InsertOnly || w.PrevNode == nil {
				return nil
			}
			return w.PrevNode
		}())
	}
	for i := range status {
		w := &status[i]
		delta, err := writeSealStatusTrusted(wb, w)
		if err != nil {
			return nil, fmt.Errorf("write seal status: %w", err)
		}
		sched = append(sched, delta...)
	}
	for i := range kidsReplace {
		if err := writeKidsReplace(wb, &kidsReplace[i]); err != nil {
			return nil, fmt.Errorf("write kids replace: %w", err)
		}
	}
	for i := range maps {
		if err := writeSealMap(wb, &maps[i]); err != nil {
			return nil, fmt.Errorf("write seal map: %w", err)
		}
	}
	for i := range logs {
		if err := writeSealLog(wb, &logs[i]); err != nil {
			return nil, fmt.Errorf("write seal log: %w", err)
		}
	}
	for i := range errs {
		if err := writeSealTaskErr(wb, &errs[i]); err != nil {
			return nil, fmt.Errorf("write seal task error: %w", err)
		}
	}
	if err := wb.Flush(); err != nil {
		return nil, fmt.Errorf("write seal batch flush: %w", err)
	}
	if err := s.frontloadKidsChildSize(kidsReplace); err != nil {
		return nil, fmt.Errorf("frontload child size: %w", err)
	}
	if len(bucketKeys) > 0 {
		if err := s.ApplyReviewAndDepth(bucketKeys, bucketDeltas, nil); err != nil {
			return nil, fmt.Errorf("index bucket stats: %w", err)
		}
	}
	return sched, nil
}

// preserveSkippedDescendantCounts keeps live skip counters when a status write does not
// intentionally change them (Status count equals PrevStatus count). Intentional updates
// set Status.SkippedDescendantCount different from PrevStatus.SkippedDescendantCount.
func (s *Store) preserveSkippedDescendantCounts(status []SealStatusWrite) error {
	if len(status) == 0 {
		return nil
	}
	bySide := map[string][]string{}
	need := false
	for i := range status {
		w := &status[i]
		if w.Status.SkippedDescendantCount != w.PrevStatus.SkippedDescendantCount &&
			w.Status.ChildSize != w.PrevStatus.ChildSize {
			continue
		}
		bySide[w.Side] = append(bySide[w.Side], w.ID)
		need = true
	}
	if !need {
		return nil
	}
	live := map[string]StatusRecord{}
	for side, ids := range bySide {
		stMap, err := s.BatchGetStatus(side, ids)
		if err != nil {
			return err
		}
		for id, st := range stMap {
			live[side+":"+id] = st
		}
	}
	for i := range status {
		w := &status[i]
		cur, ok := live[w.Side+":"+w.ID]
		if !ok {
			continue
		}
		if w.Status.SkippedDescendantCount == w.PrevStatus.SkippedDescendantCount {
			w.Status.SkippedDescendantCount = cur.SkippedDescendantCount
		}
		if w.Status.ChildSize == w.PrevStatus.ChildSize {
			w.Status.ChildSize = cur.ChildSize
		}
	}
	return nil
}

func writeSealNodeTrusted(wb *badger.WriteBatch, w *SealNodeWrite) ([]SchedCountDelta, error) {
	if w.Node.ID == "" {
		return nil, fmt.Errorf("opsdb WriteSealBatch: empty node id")
	}
	nb, err := encode(w.Node)
	if err != nil {
		return nil, err
	}
	if err := wb.Set(nodeKey(w.Side, w.Node.ID), nb); err != nil {
		return nil, err
	}
	if w.Node.ParentID != "" {
		if err := wb.Set(childKey(w.Side, w.Node.ParentID, w.Node.ID), []byte{1}); err != nil {
			return nil, err
		}
	}
	if err := writePhaseStatusBatch(wb, w.Side, w.Node.ID, w.Status); err != nil {
		return nil, err
	}
	var prev *NodeRecord
	if !w.InsertOnly && w.PrevNode != nil {
		prev = w.PrevNode
	}
	if err := writeNodeIndexes(wb, w.Side, w.Node, prev); err != nil {
		return nil, err
	}
	return applyPendingBatchTrusted(wb, w.Side, w.Node.ID, w.Depth, w.Deltas)
}

func appendIndexBucketDeltas(keys []string, deltas []int64, side string, n NodeRecord, prev *NodeRecord) ([]string, []int64) {
	if prev != nil && nodeIndexFieldsEqual(*prev, n) {
		return keys, deltas
	}
	if prev != nil {
		for _, k := range indexBucketDeltas(side, *prev, -1) {
			keys = append(keys, k)
			deltas = append(deltas, -1)
		}
	}
	for _, k := range indexBucketDeltas(side, n, 1) {
		keys = append(keys, k)
		deltas = append(deltas, 1)
	}
	return keys, deltas
}

func writeSealStatusTrusted(wb *badger.WriteBatch, w *SealStatusWrite) ([]SchedCountDelta, error) {
	if w.ID == "" {
		return nil, fmt.Errorf("opsdb WriteSealBatch: empty status id")
	}
	if err := writePhaseStatusBatch(wb, w.Side, w.ID, w.Status); err != nil {
		return nil, err
	}
	return applyPendingBatchTrusted(wb, w.Side, w.ID, w.Depth, w.Deltas)
}

func applyPendingBatchTrusted(wb *badger.WriteBatch, side, id string, depth int, deltas []PendingDelta) ([]SchedCountDelta, error) {
	var sched []SchedCountDelta
	for _, d := range deltas {
		nt := pendingNodeType(d.NodeType)
		pk := pendingKey(side, d.Phase, depth, nt, id)
		// Key presence follows Add. PendWasSet is only the schedcnt transition
		// (whether the covering index was believed to already hold this id).
		if d.Add {
			if err := wb.Set(pk, []byte{1}); err != nil {
				return nil, err
			}
		} else if !d.RetainKey {
			if err := wb.Delete(pk); err != nil {
				return nil, err
			}
		}
		_, _, delta := pendingWrite(d.Add, d.PendWasSet)
		if delta != 0 {
			sched = append(sched, pendingSchedDelta(side, d.Phase, nt, depth, d.Add))
		}
	}
	return sched, nil
}

func kidsReplaceSide(side string) string {
	if side == SideDST {
		return SideDST
	}
	return SideSRC
}

func writeKidsReplace(wb *badger.WriteBatch, r *SealKidsReplace) error {
	if r == nil || r.ParentID == "" {
		return nil
	}
	b, err := encode(r.Kids)
	if err != nil {
		return err
	}
	if err := wb.Set(kidsKey(kidsReplaceSide(r.Side), r.ParentID), b); err != nil {
		return err
	}
	if !r.Ticket {
		return nil
	}
	depth := r.TicketDepth
	if depth < 0 {
		depth = 0
	}
	return wb.Set(foldTicketKey(kidsReplaceSide(r.Side), depth, r.ParentID), []byte{1})
}

// MergeKid inserts or replaces kid in a kids pack by id.
func MergeKid(kids []KidRecord, kid KidRecord) []KidRecord {
	if kid.ID == "" {
		return kids
	}
	out := append([]KidRecord(nil), kids...)
	for i := range out {
		if out[i].ID == kid.ID {
			out[i] = kid
			return out
		}
	}
	return append(out, kid)
}

// MergeKidTickets applies copy appends onto kids replaces. A parent with no replace
// in this batch is loaded via existing and becomes one replace plus a fold ticket.
func MergeKidTickets(replaces []SealKidsReplace, tickets []KidTicket, existing func(side, parentID string) ([]KidRecord, error)) ([]SealKidsReplace, error) {
	if len(tickets) == 0 {
		return replaces, nil
	}
	type pk struct {
		side, id string
	}
	idx := map[pk]int{}
	out := append([]SealKidsReplace(nil), replaces...)
	for i := range out {
		out[i].Kids = append([]KidRecord(nil), out[i].Kids...)
		idx[pk{kidsReplaceSide(out[i].Side), out[i].ParentID}] = i
	}
	for _, t := range tickets {
		if t.ParentID == "" || t.Kid.ID == "" {
			continue
		}
		key := pk{kidsReplaceSide(t.Side), t.ParentID}
		i, ok := idx[key]
		if !ok {
			var kids []KidRecord
			if existing != nil {
				var err error
				kids, err = existing(key.side, t.ParentID)
				if err != nil {
					return nil, err
				}
			}
			out = append(out, SealKidsReplace{Side: key.side, ParentID: t.ParentID, Kids: kids})
			i = len(out) - 1
			idx[key] = i
		}
		out[i].Kids = MergeKid(out[i].Kids, t.Kid)
		out[i].Ticket = true
		out[i].TicketDepth = t.ParentDepth
	}
	return out, nil
}

// DirectFileBytes is the sum of file sizes in a kids pack. Folder kids contribute 0.
func DirectFileBytes(kids []KidRecord) int64 {
	var n int64
	for _, k := range kids {
		if k.Type == NodeTypeFile && k.Size > 0 {
			n += k.Size
		}
	}
	return n
}

func (s *Store) frontloadKidsChildSize(replaces []SealKidsReplace) error {
	for i := range replaces {
		r := replaces[i]
		if r.ParentID == "" {
			continue
		}
		if _, err := s.AssignChildSize(kidsReplaceSide(r.Side), r.ParentID, DirectFileBytes(r.Kids)); err != nil {
			return err
		}
	}
	return nil
}

// AssignChildSize overwrites identity child_size. Equal stored values are not rewritten.
func (s *Store) AssignChildSize(side, id string, size int64) (bool, error) {
	if s == nil || id == "" {
		return false, nil
	}
	st, ok, err := s.GetStatus(side, id)
	if err != nil {
		return false, err
	}
	if ok && st.ChildSize == size {
		return false, nil
	}
	st.ChildSize = size
	b, err := encode(statusTravRecord(st))
	if err != nil {
		return false, err
	}
	err = s.update(func(txn *badger.Txn) error {
		return txn.Set(statusTravKey(side, id), b)
	})
	return err == nil, err
}

func writeSealMap(wb *badger.WriteBatch, w *SealMapWrite) error {
	if w.Map.SrcID == "" || w.Map.DstID == "" {
		return fmt.Errorf("opsdb WriteSealBatch: empty map ids")
	}
	b, err := encode(w.Map)
	if err != nil {
		return err
	}
	if err := wb.Set(mapSrcKey(w.Map.SrcID), b); err != nil {
		return err
	}
	if err := wb.Set(mapDstKey(w.Map.DstID), b); err != nil {
		return err
	}
	return nil
}

func writeSealLog(wb *badger.WriteBatch, rec *LogRecord) error {
	if rec.At.IsZero() {
		rec.At = time.Now()
	}
	if rec.ID == "" {
		rec.ID = fmt.Sprintf("%d", rec.At.UnixNano())
	}
	b, err := encode(*rec)
	if err != nil {
		return err
	}
	if err := wb.Set(logKey(rec.At.UnixNano(), rec.ID), b); err != nil {
		return err
	}
	return wb.Set(logIDKey(rec.ID), b)
}

func writeSealTaskErr(wb *badger.WriteBatch, rec *TaskErrorRecord) error {
	if rec.At.IsZero() {
		rec.At = time.Now()
	}
	id := rec.NodeID
	if id == "" {
		id = fmt.Sprintf("%d", rec.At.UnixNano())
	}
	b, err := encode(*rec)
	if err != nil {
		return err
	}
	return wb.Set(taskErrKey(rec.At.UnixNano(), id), b)
}

// WalkStatusPhase iterates st:{phase}:{side}: keys in id order after afterID.
func (s *Store) WalkStatusPhase(side, phase, afterID string, reverse bool, fn func(id string, st StatusRecord) (bool, error)) error {
	if s == nil || s.db == nil || fn == nil || phase == "" {
		return nil
	}
	prefix := statusPhasePrefix(side, phase)
	return s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = reverse
		it := txn.NewIterator(opts)
		defer it.Close()
		if reverse {
			if afterID != "" {
				k := statusPhaseKeyFor(side, phase, afterID)
				it.Seek(k)
				if it.Valid() && bytes.Equal(it.Item().Key(), k) {
					it.Next()
				}
			} else {
				it.Seek(append(append([]byte(nil), prefix...), 0xff))
			}
		} else if afterID != "" {
			k := statusPhaseKeyFor(side, phase, afterID)
			it.Seek(k)
			if it.Valid() && bytes.Equal(it.Item().Key(), k) {
				it.Next()
			}
		} else {
			it.Seek(prefix)
		}
		for ; it.ValidForPrefix(prefix); it.Next() {
			_, _, id, ok := parseStatusPhaseKey(it.Item().KeyCopy(nil))
			if !ok {
				continue
			}
			var st StatusRecord
			if err := it.Item().Value(func(val []byte) error {
				var decErr error
				st, decErr = decodeStatus(val)
				return decErr
			}); err != nil {
				return err
			}
			cont, err := fn(id, st)
			if err != nil {
				return err
			}
			if !cont {
				return nil
			}
		}
		return nil
	})
}

func statusPhaseKeyFor(side, phase, id string) []byte {
	switch phase {
	case PhaseCopy:
		return statusCopyKey(side, id)
	case PhaseDel:
		return statusDelKey(side, id)
	default:
		return statusTravKey(side, id)
	}
}
