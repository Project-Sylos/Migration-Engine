// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// SetSrcToDstMapping stores a SRC→DST node mapping in the lookup table.
// srcID is the ULID of the SRC node, dstID is the ULID of the corresponding DST node.
func SetSrcToDstMapping(db *DB, srcID, dstID string) error {
	return db.Update(func(tx *bolt.Tx) error {
		bucket, err := GetOrCreateSrcToDstBucket(tx)
		if err != nil {
			return fmt.Errorf("failed to get src-to-dst bucket: %w", err)
		}
		return bucket.Put([]byte(srcID), []byte(dstID))
	})
}

// GetDstIDFromSrcID retrieves the DST node ULID for a given SRC node ULID.
// Returns empty string if no mapping exists.
func GetDstIDFromSrcID(db *DB, srcID string) (string, error) {
	var dstID string
	err := db.View(func(tx *bolt.Tx) error {
		bucket := GetSrcToDstBucket(tx)
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

// SetDstToSrcMapping stores a DST→SRC node mapping in the lookup table.
// dstID is the ULID of the DST node, srcID is the ULID of the corresponding SRC node.
func SetDstToSrcMapping(db *DB, dstID, srcID string) error {
	return db.Update(func(tx *bolt.Tx) error {
		bucket, err := GetOrCreateDstToSrcBucket(tx)
		if err != nil {
			return fmt.Errorf("failed to get dst-to-src bucket: %w", err)
		}
		return bucket.Put([]byte(dstID), []byte(srcID))
	})
}

// GetSrcIDFromDstID retrieves the SRC node ULID for a given DST node ULID.
// Returns empty string if no mapping exists.
func GetSrcIDFromDstID(db *DB, dstID string) (string, error) {
	var srcID string
	err := db.View(func(tx *bolt.Tx) error {
		bucket := GetDstToSrcBucket(tx)
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

// DeleteSrcToDstMapping removes a SRC→DST node mapping from the lookup table.
func DeleteSrcToDstMapping(db *DB, srcID string) error {
	return db.Update(func(tx *bolt.Tx) error {
		bucket := GetSrcToDstBucket(tx)
		if bucket == nil {
			return nil // Bucket doesn't exist, nothing to delete
		}
		return bucket.Delete([]byte(srcID))
	})
}

// DeleteDstToSrcMapping removes a DST→SRC node mapping from the lookup table.
func DeleteDstToSrcMapping(db *DB, dstID string) error {
	return db.Update(func(tx *bolt.Tx) error {
		bucket := GetDstToSrcBucket(tx)
		if bucket == nil {
			return nil // Bucket doesn't exist, nothing to delete
		}
		return bucket.Delete([]byte(dstID))
	})
}
