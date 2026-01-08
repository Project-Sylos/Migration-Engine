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
	db          *bolt.DB
	dbPath      string
	requireOpen bool // If true, DB must be open (error if closed). If false, can auto-open.
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
		// Create Traversal-Data root bucket for all traversal-related data
		traversalBucket, err := tx.CreateBucketIfNotExists([]byte("Traversal-Data"))
		if err != nil {
			return fmt.Errorf("failed to create Traversal-Data bucket: %w", err)
		}

		// Create SRC and DST buckets under Traversal-Data
		for _, queueType := range []string{"SRC", "DST"} {
			queueBucket, err := traversalBucket.CreateBucketIfNotExists([]byte(queueType))
			if err != nil {
				return fmt.Errorf("failed to create %s bucket: %w", queueType, err)
			}

			// Create nodes bucket
			if _, err := queueBucket.CreateBucketIfNotExists([]byte("nodes")); err != nil {
				return fmt.Errorf("failed to create Traversal-Data/%s/nodes bucket: %w", queueType, err)
			}

			// Create children bucket
			if _, err := queueBucket.CreateBucketIfNotExists([]byte("children")); err != nil {
				return fmt.Errorf("failed to create Traversal-Data/%s/children bucket: %w", queueType, err)
			}

			// Create levels bucket (individual level buckets created on demand)
			if _, err := queueBucket.CreateBucketIfNotExists([]byte("levels")); err != nil {
				return fmt.Errorf("failed to create Traversal-Data/%s/levels bucket: %w", queueType, err)
			}

			// Create exclusion-holding bucket (regular bucket, not nested)
			// This stores path hash -> depth level mappings for exclusion intent queuing
			if _, err := queueBucket.CreateBucketIfNotExists([]byte("exclusion-holding")); err != nil {
				return fmt.Errorf("failed to create Traversal-Data/%s/exclusion-holding bucket: %w", queueType, err)
			}

			// Create unexclusion-holding bucket (regular bucket, not nested)
			// This stores path hash -> depth level mappings for unexclusion intent queuing
			if _, err := queueBucket.CreateBucketIfNotExists([]byte("unexclusion-holding")); err != nil {
				return fmt.Errorf("failed to create Traversal-Data/%s/unexclusion-holding bucket: %w", queueType, err)
			}

			// Create path-to-ulid lookup bucket
			// This stores path hash -> ULID mappings for API path-based queries
			if _, err := queueBucket.CreateBucketIfNotExists([]byte("path-to-ulid")); err != nil {
				return fmt.Errorf("failed to create Traversal-Data/%s/path-to-ulid bucket: %w", queueType, err)
			}
		}

		// Create src-to-dst lookup bucket under SRC
		srcBucket := traversalBucket.Bucket([]byte("SRC"))
		if srcBucket != nil {
			if _, err := srcBucket.CreateBucketIfNotExists([]byte("src-to-dst")); err != nil {
				return fmt.Errorf("failed to create Traversal-Data/SRC/src-to-dst bucket: %w", err)
			}
		}

		// Create dst-to-src lookup bucket under DST
		dstBucket := traversalBucket.Bucket([]byte("DST"))
		if dstBucket != nil {
			if _, err := dstBucket.CreateBucketIfNotExists([]byte("dst-to-src")); err != nil {
				return fmt.Errorf("failed to create Traversal-Data/DST/dst-to-src bucket: %w", err)
			}
		}

		// Create LOGS bucket as separate top-level (its own island)
		if _, err := tx.CreateBucketIfNotExists([]byte("LOGS")); err != nil {
			return fmt.Errorf("failed to create LOGS bucket: %w", err)
		}

		// Initialize stats bucket (under Traversal-Data)
		if err := initializeStatsBucket(tx); err != nil {
			return fmt.Errorf("failed to initialize stats bucket: %w", err)
		}

		return nil
	})
}

// GetDB returns the underlying BoltDB instance for direct operations.
// If DB is closed and RequireOpen=false, it will attempt to auto-open the DB.
// If RequireOpen=true and DB is closed, it returns an error.
func (db *DB) GetDB() (*bolt.DB, error) {
	// Check if DB is open
	if !db.IsOpen() {
		// Try to auto-open if RequireOpen is false
		if !db.requireOpen && db.dbPath != "" {
			// Re-open the DB
			boltDB, err := bolt.Open(db.dbPath, 0600, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to auto-open database: %w", err)
			}
			db.db = boltDB
		} else {
			return nil, fmt.Errorf("database is not open")
		}
	}
	return db.db, nil
}

// IsOpen returns true if the database is currently open.
func (db *DB) IsOpen() bool {
	return db.db != nil
}

// SetRequireOpen sets the RequireOpen flag for this DB instance.
// When true, operations will fail if DB is closed. When false, operations can auto-open the DB.
func (db *DB) SetRequireOpen(requireOpen bool) {
	db.requireOpen = requireOpen
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
// If DB is closed and RequireOpen=false, it will attempt to auto-open the DB.
// If RequireOpen=true and DB is closed, it returns an error.
func (db *DB) Update(fn func(*bolt.Tx) error) error {
	// Check if DB is open
	if !db.IsOpen() {
		// Try to auto-open if RequireOpen is false
		if !db.requireOpen && db.dbPath != "" {
			// Re-open the DB
			boltDB, err := bolt.Open(db.dbPath, 0600, nil)
			if err != nil {
				return fmt.Errorf("failed to auto-open database: %w", err)
			}
			db.db = boltDB
		} else {
			return fmt.Errorf("database is not open")
		}
	}
	return db.db.Update(fn)
}

// View executes a read-only transaction.
// If DB is closed and RequireOpen=false, it will attempt to auto-open the DB.
// If RequireOpen=true and DB is closed, it returns an error.
func (db *DB) View(fn func(*bolt.Tx) error) error {
	// Check if DB is open
	if !db.IsOpen() {
		// Try to auto-open if RequireOpen is false
		if !db.requireOpen && db.dbPath != "" {
			// Re-open the DB
			boltDB, err := bolt.Open(db.dbPath, 0600, nil)
			if err != nil {
				return fmt.Errorf("failed to auto-open database: %w", err)
			}
			db.db = boltDB
		} else {
			return fmt.Errorf("database is not open")
		}
	}
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

// ValidateCoreSchema validates that all buckets have the correct structure.
// This checks bucket hierarchy and required sub-buckets but does not validate
// the contents of buckets (e.g., individual entries) as that would be too expensive.
// Returns an error if any structural issues are found.
func (db *DB) ValidateCoreSchema() error {
	return db.View(func(tx *bolt.Tx) error {
		// Validate top-level buckets: Traversal-Data and LOGS
		traversalBucket := tx.Bucket([]byte("Traversal-Data"))
		if traversalBucket == nil {
			return fmt.Errorf("missing top-level bucket: Traversal-Data")
		}

		logsBucket := tx.Bucket([]byte(BucketLogs))
		if logsBucket == nil {
			return fmt.Errorf("missing top-level bucket: %s", BucketLogs)
		}

		// Validate STATS bucket under Traversal-Data
		statsBucket := traversalBucket.Bucket([]byte(StatsBucketName))
		if statsBucket == nil {
			return fmt.Errorf("missing stats bucket: Traversal-Data/%s", StatsBucketName)
		}

		// Validate SRC and DST structure under Traversal-Data
		for _, queueType := range []string{BucketSrc, BucketDst} {
			topBucket := traversalBucket.Bucket([]byte(queueType))
			if topBucket == nil {
				return fmt.Errorf("missing queue bucket: Traversal-Data/%s", queueType)
			}

			// Validate required sub-buckets: nodes, children, levels
			requiredSubBuckets := []string{SubBucketNodes, SubBucketChildren, SubBucketLevels}
			for _, subName := range requiredSubBuckets {
				if topBucket.Bucket([]byte(subName)) == nil {
					return fmt.Errorf("missing Traversal-Data/%s/%s bucket", queueType, subName)
				}
			}

			// Validate all level buckets have correct status sub-buckets
			levelsBucket := topBucket.Bucket([]byte(SubBucketLevels))
			if levelsBucket == nil {
				return fmt.Errorf("missing %s/%s bucket", queueType, SubBucketLevels)
			}

			// Iterate through all level buckets
			cursor := levelsBucket.Cursor()
			for levelKey, _ := cursor.First(); levelKey != nil; levelKey, _ = cursor.Next() {
				levelBucket := levelsBucket.Bucket(levelKey)
				if levelBucket == nil {
					// Skip non-bucket entries (shouldn't happen, but be defensive)
					continue
				}

				// Required status buckets for all queues
				requiredStatuses := []string{StatusPending, StatusSuccessful, StatusFailed}
				if queueType == BucketDst {
					// DST also needs not_on_src
					requiredStatuses = append(requiredStatuses, StatusNotOnSrc)
				}

				// Validate each status bucket exists
				for _, status := range requiredStatuses {
					if levelBucket.Bucket([]byte(status)) == nil {
						return fmt.Errorf("missing status bucket Traversal-Data/%s/%s/%s/%s", queueType, SubBucketLevels, string(levelKey), status)
					}
				}
			}
		}

		// Note: Log level buckets (trace, debug, info, warning, error, critical) are created
		// on demand when log entries are written, so we don't validate their existence here.
		// This allows for empty log databases and is consistent with the on-demand creation pattern.

		return nil
	})
}

// getRootNode returns the first node (key and value) in the "nodes" bucket for the given source ("src" or "dst").
// Returns (key, value, error). If there are no nodes, key and value will be nil.
func (db *DB) GetRootNode(queueType string) ([]byte, []byte, error) {
	var key, value []byte

	err := db.View(func(tx *bolt.Tx) error {
		var b *bolt.Bucket

		switch queueType {
		case "SRC", "src":
			b = getBucket(tx, []string{"Traversal-Data", "SRC", "nodes"})
		case "DST", "dst":
			b = getBucket(tx, []string{"Traversal-Data", "DST", "nodes"})
		default:
			return fmt.Errorf("invalid queue type: %s", queueType)
		}

		if b == nil {
			return fmt.Errorf("nodes bucket not found for queue type: %s", queueType)
		}

		c := b.Cursor()
		k, v := c.First()
		if k == nil {
			// No node in this table
			key, value = nil, nil
			return nil
		}
		key = make([]byte, len(k))
		copy(key, k)
		value = make([]byte, len(v))
		copy(value, v)
		return nil
	})

	return key, value, err
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
