// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"
)

// Log sharding: count-based shards (e.g. 1M entries per shard).
// Layout per shard: LOGS/<shardID>/logs (full JSON), LOGS/<shardID>/<level> (id->empty), LOGS/<shardID>/stats (counts).
// LOGS/_meta stores current_shard and current_count for resume.

const (
	LogShardMetaBucket = "_meta"
	LogShardLogsBucket = "logs"
	LogShardStatsBucket = "stats"
	DefaultLogShardCap  = 1_000_000
)

// FormatLogShardID formats shard ID as zero-padded string (e.g. 0 -> "000000").
func FormatLogShardID(shardID int) string {
	return fmt.Sprintf("%06d", shardID)
}

// GetLogShardPath returns the bucket path for a log shard. Returns ["LOGS", "000000"].
func GetLogShardPath(shardID int) []string {
	return []string{BucketLogs, FormatLogShardID(shardID)}
}

// GetLogMetaPath returns the path for the global log meta bucket (current shard + count).
func GetLogMetaPath() []string {
	return []string{BucketLogs, LogShardMetaBucket}
}

// getLogShardBucket returns the shard root bucket (LOGS/<shardID>). Creates if not exist when create is true.
func getLogShardBucket(tx *bolt.Tx, shardID int, create bool) (*bolt.Bucket, error) {
	logsRoot := tx.Bucket([]byte(BucketLogs))
	if logsRoot == nil {
		return nil, fmt.Errorf("LOGS bucket not found")
	}
	key := []byte(FormatLogShardID(shardID))
	if create {
		b, err := logsRoot.CreateBucketIfNotExists(key)
		if err != nil {
			return nil, fmt.Errorf("create log shard bucket: %w", err)
		}
		return b, nil
	}
	return logsRoot.Bucket(key), nil
}

// GetOrCreateLogShardBucket returns or creates a sub-bucket under a log shard (e.g. "logs", "info", "stats").
func GetOrCreateLogShardBucket(tx *bolt.Tx, shardID int, subBucket string) (*bolt.Bucket, error) {
	shard, err := getLogShardBucket(tx, shardID, true)
	if err != nil {
		return nil, err
	}
	b, err := shard.CreateBucketIfNotExists([]byte(subBucket))
	if err != nil {
		return nil, fmt.Errorf("create log shard sub-bucket %s: %w", subBucket, err)
	}
	return b, nil
}

// GetLogShardBucket returns a sub-bucket under a log shard (read-only).
func GetLogShardBucket(tx *bolt.Tx, shardID int, subBucket string) *bolt.Bucket {
	shard := tx.Bucket([]byte(BucketLogs))
	if shard == nil {
		return nil
	}
	shard = shard.Bucket([]byte(FormatLogShardID(shardID)))
	if shard == nil {
		return nil
	}
	return shard.Bucket([]byte(subBucket))
}

// ReadLogShardStats reads the count for a shard from its stats bucket (for resume).
func ReadLogShardStats(tx *bolt.Tx, shardID int) (count int64, err error) {
	statsBucket := GetLogShardBucket(tx, shardID, LogShardStatsBucket)
	if statsBucket == nil {
		return 0, nil
	}
	v := statsBucket.Get([]byte("count"))
	if len(v) < 8 {
		return 0, nil
	}
	return int64(binary.BigEndian.Uint64(v)), nil
}

// WriteLogShardStats writes the count for a shard (for resume / after flush).
func WriteLogShardStats(tx *bolt.Tx, shardID int, count int64) error {
	statsBucket, err := GetOrCreateLogShardBucket(tx, shardID, LogShardStatsBucket)
	if err != nil {
		return err
	}
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(count))
	return statsBucket.Put([]byte("count"), b)
}

// GetCurrentLogShardAndCount reads current shard ID and count from _meta (for startup/resume).
func GetCurrentLogShardAndCount(tx *bolt.Tx) (shardID int, count int64, err error) {
	logsRoot := tx.Bucket([]byte(BucketLogs))
	if logsRoot == nil {
		return 0, 0, nil
	}
	meta := logsRoot.Bucket([]byte(LogShardMetaBucket))
	if meta == nil {
		return 0, 0, nil
	}
	v := meta.Get([]byte("current_shard"))
	if len(v) < 8 {
		return 0, 0, nil
	}
	shardID = int(binary.BigEndian.Uint64(v))
	count = 0
	if c := meta.Get([]byte("current_count")); len(c) >= 8 {
		count = int64(binary.BigEndian.Uint64(c))
	}
	return shardID, count, nil
}

// SetCurrentLogShardAndCount writes current shard ID and count to _meta.
func SetCurrentLogShardAndCount(tx *bolt.Tx, shardID int, count int64) error {
	logsRoot := tx.Bucket([]byte(BucketLogs))
	if logsRoot == nil {
		return fmt.Errorf("LOGS bucket not found")
	}
	meta, err := logsRoot.CreateBucketIfNotExists([]byte(LogShardMetaBucket))
	if err != nil {
		return err
	}
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(shardID))
	if err := meta.Put([]byte("current_shard"), b); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(b, uint64(count))
	return meta.Put([]byte("current_count"), b)
}

// GetLogShardIDs returns all numeric shard IDs under LOGS (for reading across shards).
func GetLogShardIDs(tx *bolt.Tx) ([]int, error) {
	logsRoot := tx.Bucket([]byte(BucketLogs))
	if logsRoot == nil {
		return nil, nil
	}
	var ids []int
	c := logsRoot.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		if string(k) == LogShardMetaBucket {
			continue
		}
		n, err := strconv.Atoi(string(k))
		if err != nil {
			continue
		}
		ids = append(ids, n)
	}
	return ids, nil
}

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

// GetLogEntry retrieves a log entry by ID; searches all shards (sharded layout).
func GetLogEntry(db *DB, level string, id string) (*LogEntry, error) {
	var entry *LogEntry

	err := db.View(func(tx *bolt.Tx) error {
		shardIDs, _ := GetLogShardIDs(tx)
		for _, shardID := range shardIDs {
			logsBucket := GetLogShardBucket(tx, shardID, LogShardLogsBucket)
			if logsBucket == nil {
				continue
			}
			data := logsBucket.Get([]byte(id))
			if data == nil {
				continue
			}
			var err error
			entry, err = DeserializeLogEntry(data)
			return err
		}
		return nil // Not found
	})

	return entry, err
}

// GetLogsByLevel retrieves all log entries for a specific level across all shards.
func GetLogsByLevel(db *DB, level string) ([]*LogEntry, error) {
	var logs []*LogEntry

	err := db.View(func(tx *bolt.Tx) error {
		shardIDs, _ := GetLogShardIDs(tx)
		for _, shardID := range shardIDs {
			levelBucket := GetLogShardBucket(tx, shardID, level)
			if levelBucket == nil {
				continue
			}
			logsBucket := GetLogShardBucket(tx, shardID, LogShardLogsBucket)
			if logsBucket == nil {
				continue
			}
			c := levelBucket.Cursor()
			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				data := logsBucket.Get(k)
				if data == nil {
					continue
				}
				entry, err := DeserializeLogEntry(data)
				if err != nil {
					continue
				}
				logs = append(logs, entry)
			}
		}
		return nil
	})

	return logs, err
}

// GetAllLogs retrieves all log entries across all shards and levels.
func GetAllLogs(db *DB) ([]*LogEntry, error) {
	var logs []*LogEntry
	levels := []string{"trace", "debug", "info", "warning", "error", "critical"}

	err := db.View(func(tx *bolt.Tx) error {
		shardIDs, _ := GetLogShardIDs(tx)
		for _, shardID := range shardIDs {
			logsBucket := GetLogShardBucket(tx, shardID, LogShardLogsBucket)
			if logsBucket == nil {
				continue
			}
			for _, level := range levels {
				levelBucket := GetLogShardBucket(tx, shardID, level)
				if levelBucket == nil {
					continue
				}
				c := levelBucket.Cursor()
				for k, _ := c.First(); k != nil; k, _ = c.Next() {
					data := logsBucket.Get(k)
					if data == nil {
						continue
					}
					entry, err := DeserializeLogEntry(data)
					if err != nil {
						continue
					}
					logs = append(logs, entry)
				}
			}
		}
		return nil
	})

	return logs, err
}
