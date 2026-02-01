// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// SetSrcToDstMapping stores a SRC→DST node mapping in the lookup table at the given level.
// srcID is the ULID of the SRC node, dstID is the ULID of the corresponding DST node.
func SetSrcToDstMapping(db *DB, level int, srcID, dstID string) error {
	return db.Update(func(tx *bolt.Tx) error {
		bucket, err := GetOrCreateSrcToDstBucket(tx, level)
		if err != nil {
			return fmt.Errorf("failed to get src-to-dst bucket: %w", err)
		}
		return bucket.Put([]byte(srcID), []byte(dstID))
	})
}

// GetDstIDFromSrcID retrieves the DST node ULID for a given SRC node ULID at the given level.
// Returns empty string if no mapping exists.
func GetDstIDFromSrcID(db *DB, level int, srcID string) (string, error) {
	var dstID string
	err := db.View(func(tx *bolt.Tx) error {
		bucket := GetSrcToDstBucket(tx, level)
		if bucket == nil {
			return nil // Bucket doesn't exist, no mapping
		}
		value := bucket.Get([]byte(srcID))
		if value != nil {
			dstID = string(value)
		}
		return nil
	})
	return dstID, err
}

// BatchGetDstIDsFromSrcIDs retrieves DST node ULIDs for multiple SRC node ULIDs at the given level in one transaction.
// Returns map[srcID]dstID; missing mappings are omitted from the map.
func BatchGetDstIDsFromSrcIDs(db *DB, level int, srcIDs []string) (map[string]string, error) {
	result := make(map[string]string)
	if len(srcIDs) == 0 {
		return result, nil
	}
	err := db.View(func(tx *bolt.Tx) error {
		bucket := GetSrcToDstBucket(tx, level)
		if bucket == nil {
			return nil
		}
		for _, srcID := range srcIDs {
			value := bucket.Get([]byte(srcID))
			if value != nil {
				result[srcID] = string(value)
			}
		}
		return nil
	})
	return result, err
}

// SetDstToSrcMapping stores a DST→SRC node mapping in the lookup table at the given level.
// dstID is the ULID of the DST node, srcID is the ULID of the corresponding SRC node.
func SetDstToSrcMapping(db *DB, level int, dstID, srcID string) error {
	return db.Update(func(tx *bolt.Tx) error {
		bucket, err := GetOrCreateDstToSrcBucket(tx, level)
		if err != nil {
			return fmt.Errorf("failed to get dst-to-src bucket: %w", err)
		}
		return bucket.Put([]byte(dstID), []byte(srcID))
	})
}

// GetSrcIDFromDstID retrieves the SRC node ULID for a given DST node ULID at the given level.
// Returns empty string if no mapping exists.
func GetSrcIDFromDstID(db *DB, level int, dstID string) (string, error) {
	var srcID string
	err := db.View(func(tx *bolt.Tx) error {
		bucket := GetDstToSrcBucket(tx, level)
		if bucket == nil {
			return nil // Bucket doesn't exist, no mapping
		}
		value := bucket.Get([]byte(dstID))
		if value != nil {
			srcID = string(value)
		}
		return nil
	})
	return srcID, err
}

// DeleteSrcToDstMapping removes a SRC→DST node mapping from the lookup table at the given level.
func DeleteSrcToDstMapping(db *DB, level int, srcID string) error {
	return db.Update(func(tx *bolt.Tx) error {
		bucket := GetSrcToDstBucket(tx, level)
		if bucket == nil {
			return nil // Bucket doesn't exist, nothing to delete
		}
		return bucket.Delete([]byte(srcID))
	})
}

// DeleteDstToSrcMapping removes a DST→SRC node mapping from the lookup table at the given level.
func DeleteDstToSrcMapping(db *DB, level int, dstID string) error {
	return db.Update(func(tx *bolt.Tx) error {
		bucket := GetDstToSrcBucket(tx, level)
		if bucket == nil {
			return nil // Bucket doesn't exist, nothing to delete
		}
		return bucket.Delete([]byte(dstID))
	})
}
