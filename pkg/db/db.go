// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	bolt "go.etcd.io/bbolt"
)

// DB wraps BoltDB instance with lifecycle management.
type DB struct {
	db     *bolt.DB
	dbPath string
}

// Options for BoltDB initialization
type Options struct {
	// Path is the path where BoltDB will store its data.
	// If empty, a temporary directory will be created.
	Path string
}

// DefaultOptions returns default options for BoltDB.
func DefaultOptions() Options {
	return Options{}
}

// Open creates and opens a new BoltDB instance.
// The database will be created at the specified path.
// Call Close() when done to ensure proper cleanup.
func Open(opts Options) (*DB, error) {
	dbPath := opts.Path
	if dbPath == "" {
		// Create temporary directory
		tmpDir, err := os.MkdirTemp("", "sylos-bolt-*")
		if err != nil {
			return nil, fmt.Errorf("failed to create temp directory: %w", err)
		}
		dbPath = filepath.Join(tmpDir, "migration.db")
	} else {
		// Ensure directory exists
		dir := filepath.Dir(dbPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create bolt directory: %w", err)
		}
	}

	// Open Bolt database
	boltDB, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open bolt db: %w", err)
	}

	database := &DB{
		db:     boltDB,
		dbPath: dbPath,
	}

	// Initialize bucket structure
	if err := database.initializeBuckets(); err != nil {
		boltDB.Close()
		return nil, fmt.Errorf("failed to initialize buckets: %w", err)
	}

	return database, nil
}

// initializeBuckets creates the core bucket structure for migration data.
// This is called once when the database is first opened.
func (db *DB) initializeBuckets() error {
	return db.Update(func(tx *bolt.Tx) error {
		// Create top-level buckets
		for _, topLevel := range []string{"SRC", "DST", "LOGS"} {
			bucket, err := tx.CreateBucketIfNotExists([]byte(topLevel))
			if err != nil {
				return fmt.Errorf("failed to create %s bucket: %w", topLevel, err)
			}

			// For SRC and DST, create sub-buckets
			if topLevel == "SRC" || topLevel == "DST" {
				// Create nodes bucket
				if _, err := bucket.CreateBucketIfNotExists([]byte("nodes")); err != nil {
					return fmt.Errorf("failed to create %s/nodes bucket: %w", topLevel, err)
				}

				// Create children bucket
				if _, err := bucket.CreateBucketIfNotExists([]byte("children")); err != nil {
					return fmt.Errorf("failed to create %s/children bucket: %w", topLevel, err)
				}

				// Create levels bucket (individual level buckets created on demand)
				if _, err := bucket.CreateBucketIfNotExists([]byte("levels")); err != nil {
					return fmt.Errorf("failed to create %s/levels bucket: %w", topLevel, err)
				}
			}
		}

		// Initialize stats bucket
		if err := initializeStatsBucket(tx); err != nil {
			return fmt.Errorf("failed to initialize stats bucket: %w", err)
		}

		return nil
	})
}

// GetDB returns the underlying BoltDB instance for direct operations.
func (db *DB) GetDB() *bolt.DB {
	return db.db
}

// Close closes the BoltDB instance.
// This does NOT delete the database file.
func (db *DB) Close() error {
	if db.db == nil {
		return nil
	}
	return db.db.Close()
}

// Cleanup closes the database and deletes the entire database file.
// This should be called after ETL #2 completes to remove ephemeral data.
func (db *DB) Cleanup() error {
	if db.db != nil {
		if err := db.db.Close(); err != nil {
			return fmt.Errorf("failed to close bolt db: %w", err)
		}
		db.db = nil
	}

	if db.dbPath != "" {
		if err := os.Remove(db.dbPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove bolt database: %w", err)
		}
	}

	return nil
}

// Path returns the path to the BoltDB file.
func (db *DB) Path() string {
	return db.dbPath
}

// Update executes a read-write transaction.
func (db *DB) Update(fn func(*bolt.Tx) error) error {
	return db.db.Update(fn)
}

// View executes a read-only transaction.
func (db *DB) View(fn func(*bolt.Tx) error) error {
	return db.db.View(fn)
}

// Get retrieves a value by key from a bucket path.
// bucketPath should be like []string{"SRC", "nodes"}.
func (db *DB) Get(bucketPath []string, key []byte) ([]byte, error) {
	var value []byte
	err := db.View(func(tx *bolt.Tx) error {
		bucket := getBucket(tx, bucketPath)
		if bucket == nil {
			return fmt.Errorf("bucket not found: %v", bucketPath)
		}
		val := bucket.Get(key)
		if val != nil {
			value = make([]byte, len(val))
			copy(value, val)
		}
		return nil
	})
	return value, err
}

// Set stores a key-value pair in a bucket.
func (db *DB) Set(bucketPath []string, key, value []byte) error {
	return db.Update(func(tx *bolt.Tx) error {
		bucket := getBucket(tx, bucketPath)
		if bucket == nil {
			return fmt.Errorf("bucket not found: %v", bucketPath)
		}
		return bucket.Put(key, value)
	})
}

// Delete removes a key from a bucket.
func (db *DB) Delete(bucketPath []string, key []byte) error {
	return db.Update(func(tx *bolt.Tx) error {
		bucket := getBucket(tx, bucketPath)
		if bucket == nil {
			return fmt.Errorf("bucket not found: %v", bucketPath)
		}
		return bucket.Delete(key)
	})
}

// Exists checks if a key exists in a bucket.
func (db *DB) Exists(bucketPath []string, key []byte) (bool, error) {
	var exists bool
	err := db.View(func(tx *bolt.Tx) error {
		bucket := getBucket(tx, bucketPath)
		if bucket == nil {
			return fmt.Errorf("bucket not found: %v", bucketPath)
		}
		exists = bucket.Get(key) != nil
		return nil
	})
	return exists, err
}

// IsTemporary returns true if the database was created in a temporary directory.
func (db *DB) IsTemporary() bool {
	if db.dbPath == "" {
		return false
	}
	return strings.Contains(db.dbPath, os.TempDir()) ||
		strings.Contains(filepath.Base(filepath.Dir(db.dbPath)), "sylos-bolt-")
}

// getBucket navigates to a nested bucket given a path.
// Returns nil if any bucket in the path doesn't exist.
func getBucket(tx *bolt.Tx, bucketPath []string) *bolt.Bucket {
	if len(bucketPath) == 0 {
		return nil
	}

	bucket := tx.Bucket([]byte(bucketPath[0]))
	if bucket == nil {
		return nil
	}

	for i := 1; i < len(bucketPath); i++ {
		bucket = bucket.Bucket([]byte(bucketPath[i]))
		if bucket == nil {
			return nil
		}
	}

	return bucket
}

// getOrCreateBucket navigates to a nested bucket, creating buckets as needed.
func getOrCreateBucket(tx *bolt.Tx, bucketPath []string) (*bolt.Bucket, error) {
	if len(bucketPath) == 0 {
		return nil, fmt.Errorf("empty bucket path")
	}

	bucket, err := tx.CreateBucketIfNotExists([]byte(bucketPath[0]))
	if err != nil {
		return nil, err
	}

	for i := 1; i < len(bucketPath); i++ {
		bucket, err = bucket.CreateBucketIfNotExists([]byte(bucketPath[i]))
		if err != nil {
			return nil, err
		}
	}

	return bucket, nil
}
