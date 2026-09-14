// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	badger "github.com/dgraph-io/badger/v4"
)

func statusTravRecord(st StatusRecord) StatusRecord {
	return StatusRecord{
		TraversalStatus:   st.TraversalStatus,
		GPLStatus:         st.GPLStatus,
		IncludeOnly:       st.IncludeOnly,
		ExclusionSource:   st.ExclusionSource,
		DeterminingRuleID: st.DeterminingRuleID,
		ErrorLogID:        st.ErrorLogID,
		ChildSize:         st.ChildSize,
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
		st.ChildSize != 0
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
	// Trav-phase key is authoritative for identity child_size (including zero).
	if travPresent {
		out.ChildSize = trav.ChildSize
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
		b, err := encode(statusTravRecord(st))
		if err != nil {
			return err
		}
		if err := txn.Set(statusTravKey(side, id), b); err != nil {
			return err
		}
	}
	if copyPhaseNonempty(st) {
		b, err := encode(statusCopyRecord(st))
		if err != nil {
			return err
		}
		if err := txn.Set(statusCopyKey(side, id), b); err != nil {
			return err
		}
	}
	if delPhaseNonempty(st) {
		b, err := encode(statusDelRecord(st))
		if err != nil {
			return err
		}
		if err := txn.Set(statusDelKey(side, id), b); err != nil {
			return err
		}
	}
	return nil
}

func writePhaseStatusBatch(wb *badger.WriteBatch, side, id string, st StatusRecord) error {
	if id == "" {
		return nil
	}
	if travPhaseNonempty(st) {
		b, err := encode(statusTravRecord(st))
		if err != nil {
			return err
		}
		if err := wb.Set(statusTravKey(side, id), b); err != nil {
			return err
		}
	}
	if copyPhaseNonempty(st) {
		b, err := encode(statusCopyRecord(st))
		if err != nil {
			return err
		}
		if err := wb.Set(statusCopyKey(side, id), b); err != nil {
			return err
		}
	}
	if delPhaseNonempty(st) {
		b, err := encode(statusDelRecord(st))
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
