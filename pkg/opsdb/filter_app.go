// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

// FilterApplicationRecord is provenance for a filter apply.
type FilterApplicationRecord struct {
	ID            string `json:"id"`
	CriteriaJSON  string `json:"criteria_json,omitempty"`
	AppliedAt     int64  `json:"applied_at,omitempty"`
	MatchedCount  int64  `json:"matched_count,omitempty"`
	ExcludedCount int64  `json:"excluded_count,omitempty"`
}

func filterAppKey(id string) []byte {
	return []byte("filtapp:" + id)
}

// PutFilterApplication writes filter apply provenance.
func (s *Store) PutFilterApplication(rec FilterApplicationRecord) error {
	if s == nil || rec.ID == "" {
		return nil
	}
	b, err := encode(rec)
	if err != nil {
		return err
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(filterAppKey(rec.ID), b)
	})
}

// GetFilterApplication loads filter apply provenance by id.
func (s *Store) GetFilterApplication(id string) (FilterApplicationRecord, error) {
	var rec FilterApplicationRecord
	if s == nil || id == "" {
		return rec, badger.ErrKeyNotFound
	}
	err := s.view(func(txn *badger.Txn) error {
		item, err := txn.Get(filterAppKey(id))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return decodeInto(val, &rec)
		})
	})
	return rec, err
}

// UpdateFilterApplicationCounts updates matched/excluded counts.
func (s *Store) UpdateFilterApplicationCounts(id string, matched, excluded int64) error {
	if s == nil || id == "" {
		return nil
	}
	return s.update(func(txn *badger.Txn) error {
		item, err := txn.Get(filterAppKey(id))
		if err == badger.ErrKeyNotFound {
			return fmt.Errorf("filter application %s not found", id)
		}
		if err != nil {
			return err
		}
		var rec FilterApplicationRecord
		if err := item.Value(func(val []byte) error {
			return decodeInto(val, &rec)
		}); err != nil {
			return err
		}
		rec.MatchedCount = matched
		rec.ExcludedCount = excluded
		b, err := encode(rec)
		if err != nil {
			return err
		}
		return txn.Set(filterAppKey(id), b)
	})
}
