// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

const (
	queuePosPhaseTraversal = "traversal"
	queuePosPhaseCopy      = "copy"
	queuePosPhaseDelete    = "delete"
)

// SaveTraversalQueuePositions persists src/dst round and keyset cursors in Badger ops.
func (db *DB) SaveTraversalQueuePositions(srcRound int, srcCursor string, dstRound int, dstCursor string) error {
	if db == nil || db.Ops() == nil {
		return nil
	}
	ops := db.Ops()
	if err := ops.PutQueuePosition("src", queuePosPhaseTraversal, opsdb.QueuePosition{Round: srcRound, Cursor: srcCursor}); err != nil {
		return err
	}
	return ops.PutQueuePosition("dst", queuePosPhaseTraversal, opsdb.QueuePosition{Round: dstRound, Cursor: dstCursor})
}

// LoadTraversalQueuePositions reads saved src/dst positions from Badger ops.
func (db *DB) LoadTraversalQueuePositions() (srcRound int, srcCursor string, dstRound int, dstCursor string, ok bool) {
	if db == nil || db.Ops() == nil {
		return 0, "", 0, "", false
	}
	ops := db.Ops()
	src, srcOK, err := ops.GetQueuePosition("src", queuePosPhaseTraversal)
	if err != nil || !srcOK {
		return 0, "", 0, "", false
	}
	dst, dstOK, err := ops.GetQueuePosition("dst", queuePosPhaseTraversal)
	if err != nil || !dstOK {
		return src.Round, src.Cursor, 0, "", true
	}
	return src.Round, src.Cursor, dst.Round, dst.Cursor, true
}

// SaveCopyQueuePosition persists copy queue round and cursor in Badger ops.
func (db *DB) SaveCopyQueuePosition(round int, cursor string) error {
	if db == nil || db.Ops() == nil {
		return nil
	}
	return db.Ops().PutQueuePosition("copy", queuePosPhaseCopy, opsdb.QueuePosition{Round: round, Cursor: cursor})
}

// LoadCopyQueuePosition reads copy queue position from Badger ops.
func (db *DB) LoadCopyQueuePosition() (round int, cursor string, ok bool) {
	if db == nil || db.Ops() == nil {
		return 0, "", false
	}
	pos, found, err := db.Ops().GetQueuePosition("copy", queuePosPhaseCopy)
	if err != nil || !found {
		return 0, "", false
	}
	return pos.Round, pos.Cursor, true
}

// SaveDeleteQueuePosition persists delete queue round and cursor in Badger ops.
func (db *DB) SaveDeleteQueuePosition(round int, cursor string) error {
	if db == nil || db.Ops() == nil {
		return nil
	}
	return db.Ops().PutQueuePosition("delete", queuePosPhaseDelete, opsdb.QueuePosition{Round: round, Cursor: cursor})
}

// LoadDeleteQueuePosition reads delete queue position from Badger ops.
func (db *DB) LoadDeleteQueuePosition() (round int, cursor string, ok bool) {
	if db == nil || db.Ops() == nil {
		return 0, "", false
	}
	pos, found, err := db.Ops().GetQueuePosition("delete", queuePosPhaseDelete)
	if err != nil || !found {
		return 0, "", false
	}
	return pos.Round, pos.Cursor, true
}
