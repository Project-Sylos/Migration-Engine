// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// LogEntry represents a single log record to be buffered.
type LogEntry struct {
	ID        string
	Timestamp string // ISO8601 format for consistency
	Level     string
	Entity    string
	EntityID  string
	Details   any
	Message   string
	Queue     string
}

// KeyLog generates a key for a log entry in BadgerDB.
// Format: log:{level}:{uuid}
func KeyLog(level, logID string) []byte {
	return []byte(fmt.Sprintf("log:%s:%s", level, logID))
}

// PrefixLogByLevel generates a prefix for iterating logs by level.
// Format: log:{level}:
func PrefixLogByLevel(level string) []byte {
	return []byte(fmt.Sprintf("log:%s:", level))
}

// PrefixLogAll generates a prefix for iterating all logs.
// Format: log:
func PrefixLogAll() []byte {
	return []byte("log:")
}

// SerializeLogEntry converts LogEntry to bytes for storage in BadgerDB.
func SerializeLogEntry(entry LogEntry) ([]byte, error) {
	return json.Marshal(entry)
}

// DeserializeLogEntry creates a LogEntry from bytes stored in BadgerDB.
func DeserializeLogEntry(data []byte) (*LogEntry, error) {
	var entry LogEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, fmt.Errorf("failed to deserialize LogEntry: %w", err)
	}
	return &entry, nil
}

// GenerateLogID generates a new UUID for a log entry.
func GenerateLogID() string {
	return uuid.New().String()
}
