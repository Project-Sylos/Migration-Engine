// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// HashPath creates a SHA-256 hash of a path for use as a lookup key.
// This allows path-based queries while using ULIDs as primary keys.
func HashPath(path string) string {
	hash := sha256.Sum256([]byte(path))
	return hex.EncodeToString(hash[:])
}

// GetPathToULIDBucket returns the path-to-ulid lookup bucket for the given queue type.
// Path: /Traversal-Data/{queueType}/path-to-ulid
func GetPathToULIDBucket(tx *bolt.Tx, queueType string) *bolt.Bucket {
	traversalBucket := tx.Bucket([]byte(TraversalDataBucket))
	if traversalBucket == nil {
		return nil
	}

	topBucket := traversalBucket.Bucket([]byte(queueType))
	if topBucket == nil {
		return nil
	}

	return topBucket.Bucket([]byte(SubBucketPathToULID))
}

// GetPathToULIDBucketPath returns the bucket path for path-to-ulid lookup.
func GetPathToULIDBucketPath(queueType string) []string {
	return []string{TraversalDataBucket, queueType, SubBucketPathToULID}
}

// EnsurePathToULIDBucket ensures the path-to-ulid bucket exists.
func EnsurePathToULIDBucket(tx *bolt.Tx, queueType string) (*bolt.Bucket, error) {
	traversalBucket := tx.Bucket([]byte(TraversalDataBucket))
	if traversalBucket == nil {
		return nil, fmt.Errorf("traversal-data bucket not found")
	}

	topBucket := traversalBucket.Bucket([]byte(queueType))
	if topBucket == nil {
		return nil, fmt.Errorf("%s bucket not found", queueType)
	}

	pathBucket, err := topBucket.CreateBucketIfNotExists([]byte(SubBucketPathToULID))
	if err != nil {
		return nil, fmt.Errorf("failed to create path-to-ulid bucket: %w", err)
	}

	return pathBucket, nil
}

// SetPathToULIDMapping creates a mapping from path hash to ULID within a transaction.
func SetPathToULIDMapping(tx *bolt.Tx, queueType string, path string, ulid string) error {
	pathBucket, err := EnsurePathToULIDBucket(tx, queueType)
	if err != nil {
		return err
	}

	pathHash := HashPath(path)
	return pathBucket.Put([]byte(pathHash), []byte(ulid))
}

// GetULIDFromPath retrieves the ULID for a given path hash.
func GetULIDFromPath(db *DB, queueType string, path string) (string, error) {
	var ulid string

	err := db.db.View(func(tx *bolt.Tx) error {
		pathBucket := GetPathToULIDBucket(tx, queueType)
		if pathBucket == nil {
			return fmt.Errorf("path-to-ulid bucket not found for %s", queueType)
		}

		pathHash := HashPath(path)
		ulidBytes := pathBucket.Get([]byte(pathHash))
		if ulidBytes == nil {
			return fmt.Errorf("path not found: %s", path)
		}

		ulid = string(ulidBytes)
		return nil
	})

	return ulid, err
}

// GetULIDFromPathHash retrieves the ULID for a given path hash (hash already computed).
func GetULIDFromPathHash(db *DB, queueType string, pathHash string) (string, error) {
	var ulid string

	err := db.db.View(func(tx *bolt.Tx) error {
		pathBucket := GetPathToULIDBucket(tx, queueType)
		if pathBucket == nil {
			return fmt.Errorf("path-to-ulid bucket not found for %s", queueType)
		}

		ulidBytes := pathBucket.Get([]byte(pathHash))
		if ulidBytes == nil {
			return fmt.Errorf("path hash not found: %s", pathHash)
		}

		ulid = string(ulidBytes)
		return nil
	})

	return ulid, err
}

// DeletePathToULIDMapping removes a path hash → ULID mapping within a transaction.
func DeletePathToULIDMapping(tx *bolt.Tx, queueType string, path string) error {
	pathBucket := GetPathToULIDBucket(tx, queueType)
	if pathBucket == nil {
		return fmt.Errorf("path-to-ulid bucket not found for %s", queueType)
	}

	pathHash := HashPath(path)
	return pathBucket.Delete([]byte(pathHash))
}

// BatchGetULIDsFromPaths retrieves ULIDs for multiple paths in a single transaction.
func BatchGetULIDsFromPaths(db *DB, queueType string, paths []string) (map[string]string, error) {
	results := make(map[string]string)

	err := db.db.View(func(tx *bolt.Tx) error {
		pathBucket := GetPathToULIDBucket(tx, queueType)
		if pathBucket == nil {
			return fmt.Errorf("path-to-ulid bucket not found for %s", queueType)
		}

		for _, path := range paths {
			pathHash := HashPath(path)
			ulidBytes := pathBucket.Get([]byte(pathHash))
			if ulidBytes != nil {
				results[path] = string(ulidBytes)
			}
		}

		return nil
	})

	return results, err
}

// BatchGetULIDsFromPathHashes retrieves ULIDs for multiple path hashes in a single transaction.
func BatchGetULIDsFromPathHashes(db *DB, queueType string, pathHashes []string) (map[string]string, error) {
	results := make(map[string]string)

	err := db.db.View(func(tx *bolt.Tx) error {
		pathBucket := GetPathToULIDBucket(tx, queueType)
		if pathBucket == nil {
			return fmt.Errorf("path-to-ulid bucket not found for %s", queueType)
		}

		for _, pathHash := range pathHashes {
			ulidBytes := pathBucket.Get([]byte(pathHash))
			if ulidBytes != nil {
				results[pathHash] = string(ulidBytes)
			}
		}

		return nil
	})

	return results, err
}
