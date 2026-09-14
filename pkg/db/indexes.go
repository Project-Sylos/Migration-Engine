// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

// Legacy Duck secondary-index helpers. Badger ops indexes (including idx:tri) are written on node insert.

func DropBulkPhaseNodeIndexes(db *DB) error { return nil }

func DropBulkPhaseStatusEventIndexes(db *DB) error { return nil }

func EnsureBulkPhaseParentIDIndexesIfMissing(db *DB) error { return nil }

func EnsureBulkPhaseNodeIndexesIfMissing(db *DB) error { return nil }

func EnsureBulkPhaseStatusEventIndexesIfMissing(db *DB) error { return nil }

func EnsureNodeTableIndexes(db *DB, table string) error { return nil }

func EnsureNodeTableIndexesIfMissing(db *DB, table string) error { return nil }

func EnsureReviewPhaseIndexesIfMissing(db *DB) error { return nil }
