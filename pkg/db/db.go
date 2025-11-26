// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

// DB wraps BadgerDB instance with lifecycle management.
type DB struct {
	db     *badger.DB
	dbPath string
}

// Options for BadgerDB initialization
type Options struct {
	// Path is the path where BadgerDB will store its data.
	// If empty, a temporary directory will be created.
	Path string
	// ValueLogFileSize is the size of the value log file in bytes.
	// Default: 1GB
	ValueLogFileSize int64
}

// DefaultOptions returns default options for BadgerDB.
func DefaultOptions() Options {
	return Options{
		ValueLogFileSize: 1 << 30, // 1GB
	}
}

// Open creates and opens a new BadgerDB instance.
// The database will be created at the specified directory.
// Call Close() when done to ensure proper cleanup.
func Open(opts Options) (*DB, error) {
	dbPath := opts.Path
	if dbPath == "" {
		// Create temporary directory
		tmpDir, err := os.MkdirTemp("", "sylos-badger-*")
		if err != nil {
			return nil, fmt.Errorf("failed to create temp directory: %w", err)
		}
		dbPath = tmpDir
	} else {
		// Ensure directory exists
		if err := os.MkdirAll(dbPath, 0755); err != nil {
			return nil, fmt.Errorf("failed to create badger directory: %w", err)
		}
	}

	badgerOpts := badger.DefaultOptions(dbPath)
	badgerOpts.Logger = nil // Disable Badger's default logging (we'll use our own)

	// Set value log file size if specified
	if opts.ValueLogFileSize > 0 {
		badgerOpts.ValueLogFileSize = opts.ValueLogFileSize
	}

	badgerDB, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger db: %w", err)
	}

	return &DB{
		db:     badgerDB,
		dbPath: dbPath,
	}, nil
}

// GetDB returns the underlying BadgerDB instance for direct operations.
func (db *DB) GetDB() *badger.DB {
	return db.db
}

// Close closes the BadgerDB instance.
// This does NOT delete the database directory.
func (db *DB) Close() error {
	if db.db == nil {
		return nil
	}
	return db.db.Close()
}

// Cleanup closes the database and deletes the entire database directory.
// This should be called after ETL #2 completes to remove ephemeral data.
func (db *DB) Cleanup() error {
	if db.db != nil {
		if err := db.db.Close(); err != nil {
			return fmt.Errorf("failed to close badger db: %w", err)
		}
		db.db = nil
	}

	if db.dbPath != "" {
		if err := os.RemoveAll(db.dbPath); err != nil {
			return fmt.Errorf("failed to remove badger directory: %w", err)
		}
	}

	return nil
}

// Path returns the path to the BadgerDB directory.
func (db *DB) Path() string {
	return db.dbPath
}

// Update executes a read-write transaction.
func (db *DB) Update(fn func(*badger.Txn) error) error {
	return db.db.Update(fn)
}

// View executes a read-only transaction.
func (db *DB) View(fn func(*badger.Txn) error) error {
	return db.db.View(fn)
}

// Get retrieves a value by key.
func (db *DB) Get(key []byte) ([]byte, error) {
	var value []byte
	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			value = make([]byte, len(val))
			copy(value, val)
			return nil
		})
	})
	return value, err
}

// Set stores a key-value pair.
func (db *DB) Set(key, value []byte) error {
	return db.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

// Delete removes a key from the database.
func (db *DB) Delete(key []byte) error {
	return db.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// DeletePrefix deletes all keys with the given prefix.
// This is used during pruning operations.
func (db *DB) DeletePrefix(prefix []byte) error {
	return db.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

// Exists checks if a key exists in the database.
func (db *DB) Exists(key []byte) (bool, error) {
	var exists bool
	err := db.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			exists = false
			return nil
		}
		if err != nil {
			return err
		}
		exists = true
		return nil
	})
	return exists, err
}

// IsTemporary returns true if the database was created in a temporary directory.
func (db *DB) IsTemporary() bool {
	if db.dbPath == "" {
		return false
	}
	// Check if path contains "temp" (typical for os.MkdirTemp)
	return filepath.Base(filepath.Dir(db.dbPath)) == "temp" ||
		filepath.Base(db.dbPath) == "temp" ||
		strings.HasPrefix(db.dbPath, os.TempDir())
}
