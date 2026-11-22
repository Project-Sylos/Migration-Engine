// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"encoding/json"
	"strings"

	badger "github.com/dgraph-io/badger/v4"
)

func syncCopyStatusIndexTxn(txn *badger.Txn, side, id, prevCopy, nextCopy string) error {
	prevCopy = strings.TrimSpace(prevCopy)
	nextCopy = strings.TrimSpace(nextCopy)
	if prevCopy == nextCopy {
		return nil
	}
	if prevCopy != "" {
		if err := txn.Delete(copyStatusIndexKey(side, prevCopy, id)); err != nil && err != badger.ErrKeyNotFound {
			return err
		}
	}
	if nextCopy != "" {
		if err := txn.Set(copyStatusIndexKey(side, nextCopy, id), []byte{1}); err != nil {
			return err
		}
	}
	return nil
}

func syncCopyStatusIndexBatch(wb *badger.WriteBatch, side, id, prevCopy, nextCopy string) error {
	prevCopy = strings.TrimSpace(prevCopy)
	nextCopy = strings.TrimSpace(nextCopy)
	if prevCopy == nextCopy {
		return nil
	}
	if prevCopy != "" {
		if err := wb.Delete(copyStatusIndexKey(side, prevCopy, id)); err != nil {
			return err
		}
	}
	if nextCopy != "" {
		if err := wb.Set(copyStatusIndexKey(side, nextCopy, id), []byte{1}); err != nil {
			return err
		}
	}
	return nil
}

func statusTravRecord(st StatusRecord) StatusRecord {
	return StatusRecord{
		TraversalStatus:   st.TraversalStatus,
		GPLStatus:         st.GPLStatus,
		IncludeOnly:       st.IncludeOnly,
		ExclusionSource:   st.ExclusionSource,
		DeterminingRuleID: st.DeterminingRuleID,
		ErrorLogID:        st.ErrorLogID,
		ChildSize:         st.ChildSize,
		ChildCount:        st.ChildCount,
	}
}

func statusCopyRecord(st StatusRecord) StatusRecord {
	return StatusRecord{
		CopyStatus:      st.CopyStatus,
		XferOffset:      st.XferOffset,
		XferSrcSize:     st.XferSrcSize,
		XferSrcMTime:    st.XferSrcMTime,
		XferDstRef:      st.XferDstRef,
		XferResumeToken: st.XferResumeToken,
	}
}

func statusDelRecord(st StatusRecord) StatusRecord {
	return StatusRecord{
		DeleteStatus:           st.DeleteStatus,
		SkippedDescendantCount: st.SkippedDescendantCount,
	}
}

func travPhaseNonempty(st StatusRecord) bool {
	return st.TraversalStatus != "" || st.GPLStatus != "" || st.IncludeOnly != "" ||
		st.ExclusionSource != "" || st.DeterminingRuleID != "" || st.ErrorLogID != "" ||
		st.ChildSize != 0 || st.ChildCount != 0
}

func copyPhaseNonempty(st StatusRecord) bool {
	return st.CopyStatus != "" || st.XferOffset != 0 || st.XferSrcSize != 0 ||
		st.XferSrcMTime != "" || st.XferDstRef != "" || st.XferResumeToken != ""
}

func delPhaseNonempty(st StatusRecord) bool {
	return st.DeleteStatus != "" || st.SkippedDescendantCount != 0
}

func mergeStatusPhases(trav, copy, del, legacy StatusRecord, travPresent bool) StatusRecord {
	out := legacy
	if trav.TraversalStatus != "" {
		out.TraversalStatus = trav.TraversalStatus
	}
	if trav.GPLStatus != "" {
		out.GPLStatus = trav.GPLStatus
	}
	if trav.IncludeOnly != "" {
		out.IncludeOnly = trav.IncludeOnly
	}
	if trav.ExclusionSource != "" {
		out.ExclusionSource = trav.ExclusionSource
	}
	if trav.DeterminingRuleID != "" {
		out.DeterminingRuleID = trav.DeterminingRuleID
	}
	if trav.ErrorLogID != "" {
		out.ErrorLogID = trav.ErrorLogID
	}
	// Trav-phase key is authoritative for identity child_size/count (including zero).
	if travPresent {
		out.ChildSize = trav.ChildSize
		out.ChildCount = trav.ChildCount
	}
	if copy.CopyStatus != "" {
		out.CopyStatus = copy.CopyStatus
	}
	if copy.XferOffset != 0 {
		out.XferOffset = copy.XferOffset
	}
	if copy.XferSrcSize != 0 {
		out.XferSrcSize = copy.XferSrcSize
	}
	if copy.XferSrcMTime != "" {
		out.XferSrcMTime = copy.XferSrcMTime
	}
	if copy.XferDstRef != "" {
		out.XferDstRef = copy.XferDstRef
	}
	if copy.XferResumeToken != "" {
		out.XferResumeToken = copy.XferResumeToken
	}
	if del.DeleteStatus != "" {
		out.DeleteStatus = del.DeleteStatus
	}
	// Del-phase key is authoritative for the skip counter (including zero).
	out.SkippedDescendantCount = del.SkippedDescendantCount
	return out
}

func readStatusValue(txn *badger.Txn, key []byte) (StatusRecord, bool, error) {
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return StatusRecord{}, false, nil
	}
	if err != nil {
		return StatusRecord{}, false, err
	}
	var st StatusRecord
	if err := item.Value(func(val []byte) error {
		st, err = decodeStatus(val)
		return err
	}); err != nil {
		return StatusRecord{}, false, err
	}
	return st, true, nil
}

func mergedStatusTxn(txn *badger.Txn, side, id string) (StatusRecord, bool, error) {
	trav, travOK, err := readStatusValue(txn, statusTravKey(side, id))
	if err != nil {
		return StatusRecord{}, false, err
	}
	copySt, copyOK, err := readStatusValue(txn, statusCopyKey(side, id))
	if err != nil {
		return StatusRecord{}, false, err
	}
	del, delOK, err := readStatusValue(txn, statusDelKey(side, id))
	if err != nil {
		return StatusRecord{}, false, err
	}
	if travOK || copyOK || delOK {
		return mergeStatusPhases(trav, copySt, del, StatusRecord{}, travOK), true, nil
	}
	legacy, legacyOK, err := readStatusValue(txn, statusKey(side, id))
	if err != nil {
		return StatusRecord{}, false, err
	}
	if !legacyOK {
		return StatusRecord{}, false, nil
	}
	return legacy, true, nil
}

func writePhaseStatusTxn(txn *badger.Txn, side, id string, st StatusRecord) error {
	if id == "" {
		return nil
	}
	if travPhaseNonempty(st) {
		b, err := json.Marshal(statusTravRecord(st))
		if err != nil {
			return err
		}
		if err := txn.Set(statusTravKey(side, id), b); err != nil {
			return err
		}
	}
	if copyPhaseNonempty(st) {
		prevCopy, _, err := readStatusValue(txn, statusCopyKey(side, id))
		if err != nil {
			return err
		}
		if prevCopy.CopyStatus == "" {
			// Legacy combined key may still hold copy status.
			if legacy, ok, lerr := readStatusValue(txn, statusKey(side, id)); lerr != nil {
				return lerr
			} else if ok {
				prevCopy.CopyStatus = legacy.CopyStatus
			}
		}
		b, err := json.Marshal(statusCopyRecord(st))
		if err != nil {
			return err
		}
		if err := txn.Set(statusCopyKey(side, id), b); err != nil {
			return err
		}
		if err := syncCopyStatusIndexTxn(txn, side, id, prevCopy.CopyStatus, st.CopyStatus); err != nil {
			return err
		}
	}
	if delPhaseNonempty(st) {
		b, err := json.Marshal(statusDelRecord(st))
		if err != nil {
			return err
		}
		if err := txn.Set(statusDelKey(side, id), b); err != nil {
			return err
		}
	}
	return nil
}

// writePhaseStatusBatch writes phase overlays. prevCopy is used to maintain idx:copy
// when CopyStatus is being set (trusted seal path already knows the previous value).
func writePhaseStatusBatch(wb *badger.WriteBatch, side, id string, st StatusRecord, prevCopy string) error {
	if id == "" {
		return nil
	}
	if travPhaseNonempty(st) {
		b, err := json.Marshal(statusTravRecord(st))
		if err != nil {
			return err
		}
		if err := wb.Set(statusTravKey(side, id), b); err != nil {
			return err
		}
	}
	if copyPhaseNonempty(st) {
		b, err := json.Marshal(statusCopyRecord(st))
		if err != nil {
			return err
		}
		if err := wb.Set(statusCopyKey(side, id), b); err != nil {
			return err
		}
		if err := syncCopyStatusIndexBatch(wb, side, id, prevCopy, st.CopyStatus); err != nil {
			return err
		}
	}
	if delPhaseNonempty(st) {
		b, err := json.Marshal(statusDelRecord(st))
		if err != nil {
			return err
		}
		if err := wb.Set(statusDelKey(side, id), b); err != nil {
			return err
		}
	}
	return nil
}

func deletePhaseStatusTxn(txn *badger.Txn, side, id string) error {
	prev, ok, err := mergedStatusTxn(txn, side, id)
	if err != nil {
		return err
	}
	if ok && prev.CopyStatus != "" {
		if err := syncCopyStatusIndexTxn(txn, side, id, prev.CopyStatus, ""); err != nil {
			return err
		}
	}
	if err := txn.Delete(statusTravKey(side, id)); err != nil && err != badger.ErrKeyNotFound {
		return err
	}
	if err := txn.Delete(statusCopyKey(side, id)); err != nil && err != badger.ErrKeyNotFound {
		return err
	}
	if err := txn.Delete(statusDelKey(side, id)); err != nil && err != badger.ErrKeyNotFound {
		return err
	}
	if err := txn.Delete(statusKey(side, id)); err != nil && err != badger.ErrKeyNotFound {
		return err
	}
	return nil
}
