// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

// SetActivity records a human-readable label for the current DB wait (soft stop UI / observer).
// Pass empty to clear.
func (db *DB) SetActivity(label string) {
	if db == nil {
		return
	}
	db.activity.Store(label)
}

// ActivityLabel returns the current DB activity label, or empty.
func (db *DB) ActivityLabel() string {
	if db == nil {
		return ""
	}
	v := db.activity.Load()
	if v == nil {
		return ""
	}
	s, _ := v.(string)
	return s
}
