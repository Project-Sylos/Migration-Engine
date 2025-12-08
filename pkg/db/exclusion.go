// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// EnsureExclusionHoldingBuckets ensures that the exclusion-holding buckets exist for both SRC and DST.
// This should be called when traversal completes to ensure the buckets are ready for exclusion intent queuing.
// The buckets are created during DB initialization, but this provides a safety check.
func EnsureExclusionHoldingBuckets(db *DB) error {
	return db.Update(func(tx *bolt.Tx) error {
		traversalBucket := tx.Bucket([]byte(TraversalDataBucket))
		if traversalBucket == nil {
			return fmt.Errorf("Traversal-Data bucket not found")
		}

		for _, queueType := range []string{BucketSrc, BucketDst} {
			queueBucket := traversalBucket.Bucket([]byte(queueType))
			if queueBucket == nil {
				return fmt.Errorf("queue bucket %s not found in Traversal-Data", queueType)
			}

			// Create exclusion-holding bucket if it doesn't exist
			if _, err := queueBucket.CreateBucketIfNotExists([]byte(SubBucketExclusionHolding)); err != nil {
				return fmt.Errorf("failed to create exclusion-holding bucket for %s: %w", queueType, err)
			}
		}

		return nil
	})
}
