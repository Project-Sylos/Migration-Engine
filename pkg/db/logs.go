// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"
)

// LogEntry represents a single log entry stored in BoltDB.
type LogEntry struct {
	ID        string `json:"id"`        // UUID
	Timestamp string `json:"timestamp"` // RFC3339Nano format
	Level     string `json:"level"`     // "trace", "debug", "info", "warning", "error", "critical"
	Entity    string `json:"entity"`    // "worker", "queue", "coordinator", etc.
	EntityID  string `json:"entity_id"` // Specific entity identifier
	Message   string `json:"message"`   // Log message
	Queue     string `json:"queue"`     // "src", "dst", or ""
}

// SerializeLogEntry converts a LogEntry to bytes.
func SerializeLogEntry(entry LogEntry) ([]byte, error) {
	return json.Marshal(entry)
}

// DeserializeLogEntry converts bytes to a LogEntry.
func DeserializeLogEntry(data []byte) (*LogEntry, error) {
	var entry LogEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, fmt.Errorf("failed to deserialize log entry: %w", err)
	}
	return &entry, nil
}

// GenerateLogID generates a unique log ID (UUID v4).
func GenerateLogID() string {
	return uuid.New().String()
}

// GetLogLevelBucketPath returns the bucket path for a specific log level.
// Returns: ["LOGS", "info"] or ["LOGS", "error"], etc.
func GetLogLevelBucketPath(level string) []string {
	return []string{BucketLogs, level}
}

// GetOrCreateLogLevelBucket returns or creates the log level bucket.
func GetOrCreateLogLevelBucket(tx *bolt.Tx, level string) (*bolt.Bucket, error) {
	logsBucket := tx.Bucket([]byte(BucketLogs))
	if logsBucket == nil {
		return nil, fmt.Errorf("LOGS bucket not found")
	}

	levelBucket, err := logsBucket.CreateBucketIfNotExists([]byte(level))
	if err != nil {
		return nil, fmt.Errorf("failed to create log level bucket %s: %w", level, err)
	}

	return levelBucket, nil
}

// GetLogLevelBucket returns the log level bucket (read-only).
func GetLogLevelBucket(tx *bolt.Tx, level string) *bolt.Bucket {
	logsBucket := tx.Bucket([]byte(BucketLogs))
	if logsBucket == nil {
		return nil
	}
	return logsBucket.Bucket([]byte(level))
}

// InsertLogEntry inserts a single log entry into BoltDB under the appropriate level bucket.
func InsertLogEntry(db *DB, entry LogEntry) error {
	data, err := SerializeLogEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to serialize log entry: %w", err)
	}

	return db.Update(func(tx *bolt.Tx) error {
		levelBucket, err := GetOrCreateLogLevelBucket(tx, entry.Level)
		if err != nil {
			return err
		}

		return levelBucket.Put([]byte(entry.ID), data)
	})
}

// GetLogEntry retrieves a log entry by ID and level.
func GetLogEntry(db *DB, level string, id string) (*LogEntry, error) {
	var entry *LogEntry

	err := db.View(func(tx *bolt.Tx) error {
		levelBucket := GetLogLevelBucket(tx, level)
		if levelBucket == nil {
			return nil // Bucket doesn't exist
		}

		data := levelBucket.Get([]byte(id))
		if data == nil {
			return nil // Not found
		}

		var err error
		entry, err = DeserializeLogEntry(data)
		return err
	})

	return entry, err
}

// GetLogsByLevel retrieves all log entries for a specific level.
func GetLogsByLevel(db *DB, level string) ([]*LogEntry, error) {
	var logs []*LogEntry

	err := db.View(func(tx *bolt.Tx) error {
		levelBucket := GetLogLevelBucket(tx, level)
		if levelBucket == nil {
			return nil // No logs at this level yet
		}

		cursor := levelBucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			entry, err := DeserializeLogEntry(v)
			if err != nil {
				continue // Skip invalid entries
			}
			logs = append(logs, entry)
		}

		return nil
	})

	return logs, err
}

// GetAllLogs retrieves all log entries across all levels.
func GetAllLogs(db *DB) ([]*LogEntry, error) {
	var logs []*LogEntry

	// Standard log levels
	levels := []string{"trace", "debug", "info", "warning", "error", "critical"}

	err := db.View(func(tx *bolt.Tx) error {
		logsBucket := GetLogsBucket(tx)
		if logsBucket == nil {
			return nil // No logs yet
		}

		// Iterate through each level bucket
		for _, level := range levels {
			levelBucket := logsBucket.Bucket([]byte(level))
			if levelBucket == nil {
				continue // This level doesn't exist yet
			}

			cursor := levelBucket.Cursor()
			for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
				entry, err := DeserializeLogEntry(v)
				if err != nil {
					continue // Skip invalid entries
				}
				logs = append(logs, entry)
			}
		}

		return nil
	})

	return logs, err
}
