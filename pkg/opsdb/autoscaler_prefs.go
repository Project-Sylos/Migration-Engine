// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"encoding/json"
	badger "github.com/dgraph-io/badger/v4"
)

const metaAutoscalerPrefsKey = "meta:autoscaler_prefs"

// AutoscalerKnobPref is one durable min/max/value overlay from the Advanced panel.
type AutoscalerKnobPref struct {
	Queue string `json:"queue"`
	Knob  string `json:"knob"`
	Min   *int   `json:"min,omitempty"`
	Max   *int   `json:"max,omitempty"`
	Value *int   `json:"value,omitempty"`
}

// AutoscalerKnobView is a cached operator knob row for paused / offline display.
type AutoscalerKnobView struct {
	Queue    string `json:"queue"`
	Knob     string `json:"knob"`
	Value    int    `json:"value"`
	Min      int    `json:"min"`
	Max      int    `json:"max"`
	Unit     string `json:"unit,omitempty"`
	ReadOnly bool   `json:"readOnly,omitempty"`
}

// AutoscalerPrefs is per-migration autoscaler Advanced-panel state in ops Badger.
type AutoscalerPrefs struct {
	Knobs     []AutoscalerKnobPref `json:"knobs,omitempty"`
	LastViews []AutoscalerKnobView `json:"lastViews,omitempty"`
}

// PutAutoscalerPrefs stores Advanced-panel overlays and last knob views.
func (s *Store) PutAutoscalerPrefs(prefs AutoscalerPrefs) error {
	if s == nil {
		return nil
	}
	b, err := json.Marshal(prefs)
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(metaAutoscalerPrefsKey), b)
	})
}

// GetAutoscalerPrefs loads Advanced-panel overlays, or empty if unset.
func (s *Store) GetAutoscalerPrefs() (AutoscalerPrefs, bool, error) {
	var prefs AutoscalerPrefs
	if s == nil {
		return prefs, false, nil
	}
	found := false
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(metaAutoscalerPrefsKey))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &prefs)
		})
	})
	return prefs, found, err
}
