// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"encoding/json"

	badger "github.com/dgraph-io/badger/v4"
)

func queuePosKey(queueKey, phase string) []byte {
	return []byte("pos:" + queueKey + ":" + phase)
}

// QueuePosition is durable pull state for resume (round + keyset cursor).
type QueuePosition struct {
	Round  int    `json:"round"`
	Cursor string `json:"cursor,omitempty"`
}

// PutQueuePosition stores round/cursor for queue_key and phase.
func (s *Store) PutQueuePosition(queueKey, phase string, pos QueuePosition) error {
	if queueKey == "" || phase == "" {
		return nil
	}
	b, err := encode(pos)
	if err != nil {
		return err
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(queuePosKey(queueKey, phase), b)
	})
}

// GetQueuePosition loads saved round/cursor for queue_key and phase.
func (s *Store) GetQueuePosition(queueKey, phase string) (QueuePosition, bool, error) {
	var out QueuePosition
	found := false
	err := s.view(func(txn *badger.Txn) error {
		item, err := txn.Get(queuePosKey(queueKey, phase))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &out)
		})
	})
	return out, found, err
}
